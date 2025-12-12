"""
Supabase Data Manager
Handles all database operations including table creation, data insertion, and retrieval.
Optimized for large datasets with parallel batch uploads.
OPTIMIZED VERSION - Direct processing from Google Sheets with optional original storage
ENHANCED - Added parallel fetching and database-level analytics
"""

import os
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import uuid
import re
from dotenv import load_dotenv
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'sslmode': 'require'
}


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
    
    # =========================
    # ORIGINAL DATA METHODS (Optional - for audit trail)
    # =========================
    
    def create_original_table(self, table_name: str, sheet_identifier: str) -> bool:
        """
        Create table for original (unprocessed) data
        
        Args:
            table_name: Base name for the tables
            sheet_identifier: Identifier for the sheet (e.g., 'jan', 'apr')
        
        Returns:
            bool: True if successful
        """
        safe_table_name = table_name.lower().replace(' ', '_').replace('-', '_')
        original_table = f"{safe_table_name}_{sheet_identifier}_original"
        
        try:
            original_sql = f"""
            CREATE TABLE IF NOT EXISTS {original_table} (
                row_id UUID PRIMARY KEY,
                original_row_number INTEGER NOT NULL,
                firstname TEXT,
                birthday TEXT,
                birthmonth TEXT,
                birthyear TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_{original_table}_row_number 
            ON {original_table}(original_row_number);
            """
    
            
            self.client.rpc('execute_sql', {'query': original_sql}).execute()
            logger.info(f"Created/verified table: {original_table}")
            
            # Refresh schema
            self.client.rpc('refresh_schema').execute()
            
            # Wait for PostgREST to update its schema cache
            time.sleep(2)  # Give PostgREST time to reload schema
            logger.info(f"Schema cache refreshed for {original_table}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error creating original table: {str(e)}")
            raise
    
    def append_original_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]], 
                            batch_size: int = 5000, max_workers: int = 5) -> bool:
        """
        Append original data to Supabase (optional - for audit trail)
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            data: List of dictionaries containing the original data
            batch_size: Number of rows per batch
            max_workers: Number of parallel workers
        
        Returns:
            bool: True if successful
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_original"
        
        try:
            if not data:
                logger.warning("No data to insert")
                return True
            
            total_rows = len(data)
            logger.info(f"Appending {total_rows:,} rows to {safe_table_name}...")
            
            # Create batches
            batches = []
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batches.append((i // batch_size, batch))
            
            total_batches = len(batches)
            
            # Process batches in parallel
            total_inserted = 0
            total_failed = 0
            completed_batches = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
            
            elapsed = time.time() - start_time
            logger.info(f"✓ Appended {total_inserted:,} rows in {elapsed:.1f}s")
            
            return True
        
        except Exception as e:
            logger.error(f"Error appending original data: {str(e)}")
            raise
    
    def get_original_data(self, table_name: str, sheet_identifier: str, 
                         limit: Optional[int] = None, offset: Optional[int] = 0) -> List[Dict[str, Any]]:
        """
        Retrieve original data from Supabase with pagination
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            limit: Maximum number of rows to retrieve
            offset: Number of rows to skip
        
        Returns:
            List of dictionaries containing the data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_original"
        
        try:
            query = self.client.table(safe_table_name).select("*").order('original_row_number')
            
            if offset:
                query = query.range(offset, offset + limit - 1 if limit else 999999)
            elif limit:
                query = query.limit(limit)
            
            response = query.execute()
            
            # Data is already in separate columns, return as-is
            return response.data
        
        except Exception as e:
            logger.error(f"Error retrieving original data: {str(e)}")
            return []
    
    def get_all_original_data(self, project_name, identifier):
        """
        Retrieve ALL original data from Supabase
        
        Args:
            project_name: Base project name
            identifier: Sheet identifier
        
        Returns:
            List of all dictionaries containing the data
        """
        table_name = f"{project_name}_{identifier}_original"
    
        all_data = []
        offset = 0
        batch_size = 1000  # Supabase pagination limit
            
        while True:
            response = self.client.table(table_name)\
                .select("*")\
                .order('original_row_number', desc=False)\
                .range(offset, offset + batch_size - 1)\
                .execute()
                
            if not response.data:
                break
                
            # Data is already in separate columns, extend directly
            all_data.extend(response.data)
                
            if len(response.data) < batch_size:
                break
                
            offset += batch_size
    
        return all_data
    
    # =========================
    # BATCH INSERT HELPER
    # =========================
    
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
                client = create_client(self.url, self.key)
                client.table(table_name).insert(batch).execute()
                rows_inserted = len(batch)
                return (batch_num, rows_inserted, rows_failed)
                
            except Exception as e:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 2
                    logger.warning(f"Batch {batch_num} failed (attempt {retry + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
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
    
    # =========================
    # CLEANED DATA TABLES
    # =========================
    
    def create_table_if_not_exists(self, table_name: str, sheet_identifier: str) -> bool:
        """
        Create included and excluded data tables dynamically
        
        Args:
            table_name: Base name for the tables
            sheet_identifier: Identifier for the sheet (e.g., 'jan', 'apr')
        
        Returns:
            bool: True if successful
        """
        safe_table_name = table_name.lower().replace(' ', '_').replace('-', '_')
        
        included_table = f"{safe_table_name}_{sheet_identifier}_included"
        excluded_table = f"{safe_table_name}_{sheet_identifier}_excluded"
        
        try:
            # Create included data table
            included_sql = f"""
            CREATE TABLE IF NOT EXISTS {included_table} (
                row_id UUID PRIMARY KEY,
                original_row_number INTEGER NOT NULL,
                name TEXT NOT NULL,
                birth_day INTEGER NOT NULL CHECK (birth_day >= 1 AND birth_day <= 31),
                birth_month INTEGER NOT NULL CHECK (birth_month >= 1 AND birth_month <= 12),
                birth_year INTEGER NOT NULL CHECK (birth_year >= 1940),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_{included_table}_row_number 
            ON {included_table}(original_row_number);
            """
            
            # Create excluded data table
            excluded_sql = f"""
            CREATE TABLE IF NOT EXISTS {excluded_table} (
                id BIGSERIAL PRIMARY KEY,
                row_id UUID NOT NULL,
                original_row_number INTEGER NOT NULL,
                original_name TEXT,
                original_birth_day TEXT,
                original_birth_month TEXT,
                original_birth_year TEXT,
                exclusion_reason TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_{excluded_table}_row_number 
            ON {excluded_table}(original_row_number);
            """
            
            self.client.rpc('execute_sql', {'query': included_sql}).execute()
            logger.info(f"Created/verified table: {included_table}")
            
            self.client.rpc('execute_sql', {'query': excluded_sql}).execute()
            logger.info(f"Created/verified table: {excluded_table}")
            
            # Refresh schema
            self.client.rpc('refresh_schema').execute()
            
            # Wait for PostgREST to update its schema cache
            time.sleep(2)
            logger.info(f"Schema cache refreshed for cleaned data tables")
            
            return True
        
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def append_included_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]], 
                            batch_size: int = 5000, max_workers: int = 5) -> bool:
        """
        Append included data to Supabase (does not clear existing data)
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            data: List of dictionaries containing the cleaned data
            batch_size: Number of rows per batch
            max_workers: Number of parallel workers
        
        Returns:
            bool: True if successful
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_included"
        
        try:
            if not data:
                logger.warning("No data to insert")
                return True
            
            total_rows = len(data)
            logger.info(f"Appending {total_rows:,} rows to {safe_table_name}...")
            
            # Create batches
            batches = []
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batches.append((i // batch_size, batch))
            
            total_batches = len(batches)
            
            # Process batches in parallel
            total_inserted = 0
            total_failed = 0
            completed_batches = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
            
            elapsed = time.time() - start_time
            logger.info(f"✓ Appended {total_inserted:,} rows in {elapsed:.1f}s")
            
            return True
        
        except Exception as e:
            logger.error(f"Error appending included data: {str(e)}")
            raise
    
    def append_excluded_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]],
                            batch_size: int = 5000, max_workers: int = 5) -> bool:
        """
        Append excluded data to Supabase (does not clear existing data)
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            data: List of dictionaries containing the excluded data
            batch_size: Number of rows per batch
            max_workers: Number of parallel workers
        
        Returns:
            bool: True if successful
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_excluded"
        
        try:
            if not data:
                logger.warning("No data to insert")
                return True
            
            total_rows = len(data)
            logger.info(f"Appending {total_rows:,} rows to {safe_table_name}...")
            
            # Create batches
            batches = []
            for i in range(0, total_rows, batch_size):
                batch = data[i:i + batch_size]
                batches.append((i // batch_size, batch))
            
            total_batches = len(batches)
            
            # Process batches in parallel
            total_inserted = 0
            total_failed = 0
            completed_batches = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
            
            elapsed = time.time() - start_time
            logger.info(f"✓ Appended {total_inserted:,} rows in {elapsed:.1f}s")
            
            return True
        
        except Exception as e:
            logger.error(f"Error appending excluded data: {str(e)}")
            raise
    
    def insert_included_data(self, table_name: str, sheet_identifier: str, data: List[Dict[str, Any]], 
                            batch_size: int = 10000, max_workers: int = 5) -> bool:
        """
        Insert cleaned/included data into Supabase with parallel batch processing (clears existing)
        
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
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
                    
                    if completed_batches % 10 == 0 or completed_batches == total_batches:
                        elapsed = time.time() - start_time
                        progress_pct = (completed_batches / total_batches) * 100
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {completed_batches}/{total_batches} batches ({progress_pct:.1f}%) | "
                            f"{total_inserted:,}/{total_rows:,} rows | "
                            f"Rate: {rate:.0f} rows/sec"
                        )
            
            elapsed = time.time() - start_time
            success_rate = (total_inserted / total_rows) * 100 if total_rows > 0 else 0
            avg_rate = total_inserted / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"✓ Insert complete: {total_inserted:,}/{total_rows:,} rows ({success_rate:.2f}%) | "
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
        Insert excluded data into Supabase with parallel batch processing (clears existing)
        
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
                future_to_batch = {
                    executor.submit(self._insert_batch, safe_table_name, batch, batch_num): batch_num
                    for batch_num, batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_num, rows_inserted, rows_failed = future.result()
                    total_inserted += rows_inserted
                    total_failed += rows_failed
                    completed_batches += 1
                    
                    if completed_batches % 10 == 0 or completed_batches == total_batches:
                        elapsed = time.time() - start_time
                        progress_pct = (completed_batches / total_batches) * 100
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"Progress: {completed_batches}/{total_batches} batches ({progress_pct:.1f}%) | "
                            f"{total_inserted:,}/{total_rows:,} rows | "
                            f"Rate: {rate:.0f} rows/sec"
                        )
            
            elapsed = time.time() - start_time
            success_rate = (total_inserted / total_rows) * 100 if total_rows > 0 else 0
            avg_rate = total_inserted / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"✓ Insert complete: {total_inserted:,}/{total_rows:,} rows ({success_rate:.2f}%) | "
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
        Retrieve included data from Supabase with pagination
        
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
            query = self.client.table(safe_table_name).select("*").order('original_row_number')
            
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
        Retrieve excluded data from Supabase with pagination
        
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
            query = self.client.table(safe_table_name).select("*").order('original_row_number')
            
            if offset:
                query = query.range(offset, offset + limit - 1 if limit else 999999)
            elif limit:
                query = query.limit(limit)
            
            response = query.execute()
            return response.data
        
        except Exception as e:
            logger.error(f"Error retrieving excluded data: {str(e)}")
            return []
    
    def get_all_included_data(self, table_name: str, sheet_identifier: str, 
                             batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve ALL included data from Supabase using pagination
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            batch_size: Number of rows to fetch per batch
        
        Returns:
            List of all dictionaries containing the data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_included"
        
        try:
            all_data = []
            offset = 0
            
            while True:
                response = self.client.table(safe_table_name)\
                    .select("*")\
                    .order('original_row_number')\
                    .range(offset, offset + batch_size - 1)\
                    .execute()
                
                batch_data = response.data
                
                if not batch_data:
                    break
                
                all_data.extend(batch_data)
                
                if len(batch_data) < batch_size:
                    break
                
                offset += batch_size
            
            logger.info(f"Retrieved {len(all_data):,} included rows from {safe_table_name}")
            return all_data
        
        except Exception as e:
            logger.error(f"Error retrieving all included data: {str(e)}")
            return []
    
    def get_all_excluded_data(self, table_name: str, sheet_identifier: str,
                             batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Retrieve ALL excluded data from Supabase using pagination
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            batch_size: Number of rows to fetch per batch
        
        Returns:
            List of all dictionaries containing the data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_excluded"
        
        try:
            all_data = []
            offset = 0
            
            while True:
                response = self.client.table(safe_table_name)\
                    .select("*")\
                    .order('original_row_number')\
                    .range(offset, offset + batch_size - 1)\
                    .execute()
                
                batch_data = response.data
                
                if not batch_data:
                    break
                
                all_data.extend(batch_data)
                
                if len(batch_data) < batch_size:
                    break
                
                offset += batch_size
            
            logger.info(f"Retrieved {len(all_data):,} excluded rows from {safe_table_name}")
            return all_data
        
        except Exception as e:
            logger.error(f"Error retrieving all excluded data: {str(e)}")
            return []
        
    
    # =========================
    # NEW: PARALLEL FETCHING METHODS (MUCH FASTER!)
    # =========================
    
    def get_all_included_data_parallel(self, table_name: str, sheet_identifier: str, 
                                      batch_size: int = 1000, max_workers: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieve ALL included data using PARALLEL fetching (much faster!)
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            batch_size: Rows per batch (default 1000)
            max_workers: Parallel workers (default 10)
        
        Returns:
            List of all data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_included"
        
        try:
            # Step 1: Get total count
            count_response = self.client.table(safe_table_name).select("*", count='exact').limit(1).execute()
            total_count = count_response.count or 0
            
            if total_count == 0:
                return []
            
            logger.info(f"Fetching {total_count:,} included rows in parallel...")
            
            # Step 2: Calculate batches
            num_batches = (total_count + batch_size - 1) // batch_size
            
            # Step 3: Fetch in parallel
            def fetch_batch(batch_num):
                offset = batch_num * batch_size
                try:
                    client = create_client(self.url, self.key)
                    response = client.table(safe_table_name)\
                        .select("*")\
                        .order('original_row_number')\
                        .range(offset, offset + batch_size - 1)\
                        .execute()
                    return (batch_num, response.data)
                except Exception as e:
                    logger.error(f"Error fetching batch {batch_num}: {str(e)}")
                    return (batch_num, [])
            
            start_time = time.time()
            all_data = [None] * num_batches
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {executor.submit(fetch_batch, i): i for i in range(num_batches)}
                
                completed = 0
                for future in as_completed(future_to_batch):
                    batch_num, batch_data = future.result()
                    all_data[batch_num] = batch_data
                    completed += 1
                    
                    if completed % 100 == 0:
                        logger.info(f"Progress: {completed}/{num_batches} batches")
            
            # Flatten
            flattened = []
            for batch in all_data:
                if batch:
                    flattened.extend(batch)
            
            elapsed = time.time() - start_time
            logger.info(f"✓ Fetched {len(flattened):,} rows in {elapsed:.1f}s ({len(flattened)/elapsed:.0f} rows/sec)")
            
            return flattened
        
        except Exception as e:
            logger.error(f"Error retrieving data: {str(e)}")
            return []
    
    def get_all_excluded_data_parallel(self, table_name: str, sheet_identifier: str,
                                      batch_size: int = 1000, max_workers: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieve ALL excluded data using PARALLEL fetching (much faster!)
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            batch_size: Rows per batch (default 1000)
            max_workers: Parallel workers (default 10)
        
        Returns:
            List of all data
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_excluded"
        
        try:
            count_response = self.client.table(safe_table_name).select("*", count='exact').limit(1).execute()
            total_count = count_response.count or 0
            
            if total_count == 0:
                return []
            
            logger.info(f"Fetching {total_count:,} excluded rows in parallel...")
            
            num_batches = (total_count + batch_size - 1) // batch_size
            
            def fetch_batch(batch_num):
                offset = batch_num * batch_size
                try:
                    client = create_client(self.url, self.key)
                    response = client.table(safe_table_name)\
                        .select("*")\
                        .order('original_row_number')\
                        .range(offset, offset + batch_size - 1)\
                        .execute()
                    return (batch_num, response.data)
                except Exception as e:
                    logger.error(f"Error fetching batch {batch_num}: {str(e)}")
                    return (batch_num, [])
            
            start_time = time.time()
            all_data = [None] * num_batches
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {executor.submit(fetch_batch, i): i for i in range(num_batches)}
                
                completed = 0
                for future in as_completed(future_to_batch):
                    batch_num, batch_data = future.result()
                    all_data[batch_num] = batch_data
                    completed += 1
                    
                    if completed % 100 == 0:
                        logger.info(f"Progress: {completed}/{num_batches} batches")
            
            flattened = []
            for batch in all_data:
                if batch:
                    flattened.extend(batch)
            
            elapsed = time.time() - start_time
            logger.info(f"✓ Fetched {len(flattened):,} rows in {elapsed:.1f}s ({len(flattened)/elapsed:.0f} rows/sec)")
            
            return flattened
        
        except Exception as e:
            logger.error(f"Error retrieving data: {str(e)}")
            return []

    
    # =========================
    # UTILITY METHODS
    # =========================
    
    def count_records(self, table_name: str, sheet_identifier: str, table_type: str = 'included') -> int:
        """
        Count records in a table with retry logic
        
        Args:
            table_name: Base table name
            sheet_identifier: Sheet identifier
            table_type: 'original', 'included' or 'excluded'
        
        Returns:
            int: Number of records
        """
        safe_table_name = f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_{table_type}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Create fresh client for each attempt
                client = create_client(self.url, self.key)
                response = client.table(safe_table_name).select("*", count='exact').execute()
                return response.count or 0
            
            except Exception as e:
                error_str = str(e)
                
                # If table doesn't exist, return 0 immediately
                if 'PGRST205' in error_str or 'not find the table' in error_str or '404' in error_str:
                    logger.debug(f"Table {safe_table_name} does not exist yet")
                    return 0
                
                # If connection error, retry
                if 'WinError 10054' in error_str or 'connection' in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        logger.warning(f"Connection error counting {safe_table_name}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Failed to count records after {max_retries} attempts: {error_str}")
                        return 0
                else:
                    logger.error(f"Error counting records in {safe_table_name}: {error_str}")
                    return 0
        
        return 0
    
    def count_included_records(self, table_name: str, sheet_identifier: str) -> int:
        """Count included records using direct PostgreSQL query"""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE sheet_identifier = %s AND is_excluded = FALSE",
                (sheet_identifier,)
            )
            count = cursor.fetchone()[0]
            return count
            
        except Exception as e:
            logger.error(f"Error counting included records: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def count_excluded_records(self, table_name: str, sheet_identifier: str) -> int:
        """Count excluded records using direct PostgreSQL query"""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE sheet_identifier = %s AND is_excluded = TRUE",
                (sheet_identifier,)
            )
            count = cursor.fetchone()[0]
            return count
            
        except Exception as e:
            logger.error(f"Error counting excluded records: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def count_total_records(self, table_name: str, sheet_identifier: str) -> int:
        """Count total records using direct PostgreSQL query"""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE sheet_identifier = %s",
                (sheet_identifier,)
            )
            count = cursor.fetchone()[0]
            return count
            
        except Exception as e:
            logger.error(f"Error counting total records: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return 0
            
