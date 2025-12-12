import psycopg2
import psycopg2.extras
import logging
from dotenv import load_dotenv
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'sslmode': 'require'
}


class SupabaseManagerSQL:
    """PostgreSQL manager mimicking the original SupabaseManager sheet_identifier handling"""

    def __init__(self, base_table: str = "clients_2025"):
        self.base_table = base_table

    # ----------------------
    # FETCH RECORDS
    # ----------------------
    def get_records(self, sheet_identifier: str, limit: int = 100, offset: int = 0, excluded=False):
        table_name = f"{self.base_table}_{sheet_identifier}"
        table_name += "_excluded" if excluded else "_included"

        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(
                f"""
                SELECT *
                FROM {table_name}
                ORDER BY id
                LIMIT %s OFFSET %s
                """,
                (limit, offset)
            )
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching records from {table_name}: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    # ----------------------
    # COUNT RECORDS
    # ----------------------
    def count_included_records(self, sheet_identifier: str) -> int:
        table_name = f"{self.base_table}_{sheet_identifier}_included"
        return self._count_records(table_name)

    def count_excluded_records(self, sheet_identifier: str) -> int:
        table_name = f"{self.base_table}_{sheet_identifier}_excluded"
        return self._count_records(table_name)

    def count_total_records(self, sheet_identifier: str) -> int:
        included = self.count_included_records(sheet_identifier)
        excluded = self.count_excluded_records(sheet_identifier)
        return included + excluded

    def _count_records(self, table_name: str) -> int:
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error counting records in {table_name}: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
