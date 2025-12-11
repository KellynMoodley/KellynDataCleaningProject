"""
Supabase Data Manager
Handles all database operations including table creation, data insertion, and retrieval.
Optimized for large datasets with parallel batch uploads.
"""

import os
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SupabaseManager:
    """Manages all Supabase database operations"""
    
    def __init__(self, supabase_url: Optional[str] = None, supabase_key: Optional[str] = None):
        """
        Initialize Supabase client
        
        Args:
            supabase_url: Supabase project URL (defaults to env var SUPABASE_URL)
            supabase_key: Supabase API key (defaults to env var SUPABASE_KEY)
        """
        self.url = supabase_url or os.getenv('SUPABASE_URL')
        self.key = supabase_key or os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and KEY must be provided or set in environment variables")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized successfully")
    
    def _insert_batch(self, table_name: str, batch: List[Dict[str, Any]], batch_num: int, max_retries: int = 3) -> tuple[int, int, int]:
        """
        Insert a single batch with retry logic
        
        Args:
            table_name: Table to insert into
            batch: Data batch to insert
            batch_num: Batch number for logging
            max_retries: Maximum retry attempts
            
        Returns:
            Tuple of (batch_num, rows_inserted, rows_failed)
        """
        rows_inserted = 0
        rows_failed = 0
        
        for retry in range(max_retries):
            try:
                # Create a new client for this thread
                client = create_client(self.url, self.key)
                client.table(table_name).insert(batch).execute()
                rows_inserted = len(batch)
                return (batch_num, rows_inserted, rows_failed)
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 2
                    logger.warning(f"Batch {batch_num} failed (attempt {retry + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    # Final retry failed - try row by row
                    logger.warning(f"Batch {batch_num} failed after {max_retries} attempts, trying row-by-row")
                    client = create_client(self.url, self.key)
                    for row in batch:
                        try:
                            client.table(table_name).insert([row]).execute()
                            rows_inserted += 1
                        except Exception as row_error:
                            logger.error(f"Row in batch {batch_num} failed: {row_error}")
                            rows_failed += 1
                    
                    return (batch_num, rows_inserted, rows_failed)
        
        return (batch_num, rows_inserted, rows_failed)
    
    def create_table_if_not_exists(self, table_name: str, sheet_identifier: str) -> bool:
        """
        Create included and excluded data tables dynamically
        
        Args:
            table_name: Base name for the tables
            sheet_identifier: Identifier for the sheet (e.g., 'jan', 'apr')
        
        Returns:
            bool: True if successful
        """
        # Sanitize table name
        safe_table_name = table_name.lower().replace(' ', '_').replace('-', '_')
        
        included_table = f"{safe_table_name}_{sheet_identifier}_included"
        excluded_table = f"{safe_table_name}_{sheet_identifier}_excluded"
        
        try:
            # Create included data table
            included_sql = f"""
            CREATE TABLE IF NOT EXISTS {included_table} (
                id BIGSERIAL PRIMARY KEY,
                row_id UUID UNIQUE NOT NULL,
                name TEXT NOT NULL,
                birth_day INTEGER NOT NULL CHECK (birth_day >= 1 AND birth_day <= 31),
                birth_month INTEGER NOT NULL CHECK (birth_month >= 1 AND birth_month <= 12),
                birth_year INTEGER NOT NULL CHECK (birth_year >= 1940),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_{included_table}_row_id ON {included_table}(row_id);
            CREATE INDEX IF NOT EXISTS idx_{included_table}_name ON {included_table}(name);
            CREATE INDEX IF NOT EXISTS idx_{included_table}_birth_year ON {included_table}(birth_year);
            """
            
            # Create excluded data table
            excluded_sql = f"""
            CREATE TABLE IF NOT EXISTS {excluded_table} (
                id BIGSERIAL PRIMARY KEY,
                row_id UUID NOT NULL,
                original_name TEXT,
                original_birth_day TEXT,
                original_birth_month TEXT,
                original_birth_year TEXT,
                exclusion_reason TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_{excluded_table}_row_id ON {excluded_table}(row_id);
            CREATE INDEX IF NOT EXISTS idx_{excluded_table}_exclusion_reason ON {excluded_table}(exclusion_reason);
            """
            
            # Execute SQL using the execute_sql function
            self.client.rpc('execute_sql', {'query': included_sql}).execute()
            logger.info(f"Created/verified table: {included_table}")
            
            self.client.rpc('execute_sql', {'query': excluded_sql}).execute()
            logger.info(f"Created/verified table: {excluded_table}")
            
            # Refresh schema
            self.client.rpc('refresh_schema').execute()
            
            return True
        
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def insert_included_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]], 
                            batch_size: int = 10000, max_workers: int = 5) -> bool:
        """
        Insert cleaned/included data into Supabase with parallel batch processing
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            data: List of dictionaries containing the cleaned data
            batch_size: Number of rows per batch (default: 10000)
            max_workers: Number of parallel workers (default: 5)
        
        Returns:
            bool: True if successful
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_included"
        
        try:
            # Clear existing data for this batch
            logger.info(f"Clearing existing data from {safe_table_name}...")
            self.client.table(safe_table_name).delete().neq('row_id', '00000000-0000-0000-0000-000000000000').execute()
            
            if not data:
                logger.warning("No data to insert")
                return True
            
            total_rows = len(data)
            logger.info(f"Starting parallel batch insert of {total_rows:,} rows (batch_size={batch_size}, workers={max_workers})...")
            
            # Create batches
            batches = []
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batches.append((i // batch_size, batch))
            
            total_batches = len(batches)
            logger.info(f"Created {total_batches} batches")
            
            # Process batches in parallel
            total_inserted = 0
            total_failed = 0
            completed_batches = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batches
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
                    
                    # Log progress
                    if completed_batches % 10 == 0 or completed_batches == total_batches:
                        elapsed = time.time() - start_time
                        progress_pct = (completed_batches / total_batches) * 100
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {completed_batches}/{total_batches} batches ({progress_pct:.1f}%) | "
                            f"{total_inserted:,}/{total_rows:,} rows | "
                            f"Rate: {rate:.0f} rows/sec | "
                            f"Elapsed: {elapsed:.1f}s"
                        )
            
            # Final summary
            elapsed = time.time() - start_time
            success_rate = (total_inserted / total_rows) * 100 if total_rows > 0 else 0
            avg_rate = total_inserted / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"✓ Insert complete: {total_inserted:,}/{total_rows:,} rows ({success_rate:.2f}%) | "
                f"Failed: {total_failed} | "
                f"Time: {elapsed:.1f}s | "
                f"Avg rate: {avg_rate:.0f} rows/sec"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Error inserting included data: {str(e)}")
            raise
    
    def insert_excluded_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]],
                            batch_size: int = 10000, max_workers: int = 5) -> bool:
        """
        Insert excluded data into Supabase with parallel batch processing
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            data: List of dictionaries containing the excluded data
            batch_size: Number of rows per batch (default: 10000)
            max_workers: Number of parallel workers (default: 5)
        
        Returns:
            bool: True if successful
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_excluded"
        
        try:
            # Clear existing data for this batch
            logger.info(f"Clearing existing data from {safe_table_name}...")
            self.client.table(safe_table_name).delete().neq('row_id', '00000000-0000-0000-0000-000000000000').execute()
            
            if not data:
                logger.warning("No data to insert")
                return True
            
            total_rows = len(data)
            logger.info(f"Starting parallel batch insert of {total_rows:,} rows (batch_size={batch_size}, workers={max_workers})...")
            
            # Create batches
            batches = []
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batches.append((i // batch_size, batch))
            
            total_batches = len(batches)
            logger.info(f"Created {total_batches} batches")
            
            # Process batches in parallel
            total_inserted = 0
            total_failed = 0
            completed_batches = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batches
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
                    
                    # Log progress
                    if completed_batches % 10 == 0 or completed_batches == total_batches:
                        elapsed = time.time() - start_time
                        progress_pct = (completed_batches / total_batches) * 100
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {completed_batches}/{total_batches} batches ({progress_pct:.1f}%) | "
                            f"{total_inserted:,}/{total_rows:,} rows | "
                            f"Rate: {rate:.0f} rows/sec | "
                            f"Elapsed: {elapsed:.1f}s"
                        )
            
            # Final summary
            elapsed = time.time() - start_time
            success_rate = (total_inserted / total_rows) * 100 if total_rows > 0 else 0
            avg_rate = total_inserted / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"✓ Insert complete: {total_inserted:,}/{total_rows:,} rows ({success_rate:.2f}%) | "
                f"Failed: {total_failed} | "
                f"Time: {elapsed:.1f}s | "
                f"Avg rate: {avg_rate:.0f} rows/sec"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Error inserting excluded data: {str(e)}")
            raise
    
    def get_included_data(self, table_name: str, sheet_identifier: str, 
                          limit: Optional[int] = None, offset: Optional[int] = 0) -> List[Dict[str, Any]]:
        """
        Retrieve included data from Supabase
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            limit: Maximum number of rows to retrieve
            offset: Number of rows to skip
        
        Returns:
            List of dictionaries containing the data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_included"
        
        try:
            query = self.client.table(safe_table_name).select("*").order('id')
            
            if offset:
                query = query.range(offset, offset + limit - 1 if limit else 999999)
            elif limit:
                query = query.limit(limit)
            
            response = query.execute()
            return response.data
        
        except Exception as e:
            logger.error(f"Error retrieving included data: {str(e)}")
            return []
    
    def get_excluded_data(self, table_name: str, sheet_identifier: str,
                          limit: Optional[int] = None, offset: Optional[int] = 0) -> List[Dict[str, Any]]:
        """
        Retrieve excluded data from Supabase
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            limit: Maximum number of rows to retrieve
            offset: Number of rows to skip
        
        Returns:
            List of dictionaries containing the data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_excluded"
        
        try:
            query = self.client.table(safe_table_name).select("*").order('id')
            
            if offset:
                query = query.range(offset, offset + limit - 1 if limit else 999999)
            elif limit:
                query = query.limit(limit)
            
            response = query.execute()
            return response.data
        
        except Exception as e:
            logger.error(f"Error retrieving excluded data: {str(e)}")
            return []
    
    def count_records(self, table_name: str, sheet_identifier: str, table_type: str = 'included') -> int:
        """
        Count records in a table
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            table_type: 'included' or 'excluded'
        
        Returns:
            int: Number of records
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_{table_type}"
        
        try:
            response = self.client.table(safe_table_name).select("id", count='exact').execute()
            return response.count or 0
        
        except Exception as e:
            error_str = str(e)
            # If table doesn't exist (PGRST205), return 0 instead of logging error
            if 'PGRST205' in error_str or 'not find the table' in error_str:
                logger.debug(f"Table {safe_table_name} does not exist yet")
                return 0
            else:
            # Log other errors
                logger.error(f"Error counting records in {safe_table_name}: {str(e)}")
                return 0