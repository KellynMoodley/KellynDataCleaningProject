import os
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsEngine:
    """Optimized Analytics Engine using SQL aggregation"""

    def __init__(self, supabase_url: Optional[str] = None, supabase_key: Optional[str] = None):
        self.url = supabase_url or os.getenv('SUPABASE_URL')
        self.key = supabase_key or os.getenv('SUPABASE_KEY')
        if not self.url or not self.key:
            raise ValueError("Supabase URL and KEY must be provided")
        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized successfully")

    def _safe_table_name(self, table_name: str, sheet_identifier: str, table_type: str) -> str:
        return f"{table_name.lower().replace(' ', '_').replace('-', '_')}_{sheet_identifier}_{table_type}"

    def get_dataset_sizes(self, table_name: str, sheet_identifier: str) -> Dict[str, Any]:
        included_table = self._safe_table_name(table_name, sheet_identifier, 'included')
        excluded_table = self._safe_table_name(table_name, sheet_identifier, 'excluded')

        included_count = self.client.table(included_table).select("*", count="exact").execute().count or 0
        excluded_count = self.client.table(excluded_table).select("*", count="exact").execute().count or 0
        original_count = included_count + excluded_count

        return {
            'original_row_count': original_count,
            'included_row_count': included_count,
            'excluded_row_count': excluded_count,
            'percent_included_vs_original': round(included_count / original_count * 100, 2) if original_count > 0 else 0,
            'percent_excluded_vs_original': round(excluded_count / original_count * 100, 2) if original_count > 0 else 0
        }

    def get_uniqueness_metrics(self, table_name: str, sheet_identifier: str) -> Dict[str, Any]:
        table = self._safe_table_name(table_name, sheet_identifier, 'included')

        # SQL aggregation for uniqueness
        query_unique_names = f"SELECT COUNT(DISTINCT name) as unique_names FROM {table};"
        query_unique_birthday_combos = f"SELECT COUNT(DISTINCT birth_day || '-' || birth_month || '-' || birth_year) as unique_birthday_combos FROM {table};"
        query_unique_name_year = f"SELECT COUNT(DISTINCT name || '-' || birth_year) as unique_name_year FROM {table};"
        query_unique_name_month = f"SELECT COUNT(DISTINCT name || '-' || birth_month) as unique_name_month FROM {table};"
        query_unique_name_day = f"SELECT COUNT(DISTINCT name || '-' || birth_day) as unique_name_day FROM {table};"

        result = {}
        for q, key in [
            (query_unique_names, 'unique_names'),
            (query_unique_birthday_combos, 'unique_birthday_combinations'),
            (query_unique_name_year, 'unique_name_year'),
            (query_unique_name_month, 'unique_name_month'),
            (query_unique_name_day, 'unique_name_day')
        ]:
            res = self.client.rpc('sql', {"query": q}).execute()
            result[key] = res.data[0][key] if res.data else 0

        return result

    def get_birth_year_distribution(self, table_name: str, sheet_identifier: str) -> List[Dict[str, Any]]:
        table = self._safe_table_name(table_name, sheet_identifier, 'included')
        query = f"""
            SELECT birth_year as year, COUNT(*) as count
            FROM {table}
            GROUP BY birth_year
            ORDER BY birth_year;
        """
        res = self.client.rpc('sql', {"query": query}).execute()
        return res.data or []

    def get_birth_month_distribution(self, table_name: str, sheet_identifier: str) -> List[Dict[str, Any]]:
        table = self._safe_table_name(table_name, sheet_identifier, 'included')
        query = f"""
            SELECT birth_month as month, COUNT(*) as count
            FROM {table}
            GROUP BY birth_month
            ORDER BY birth_month;
        """
        res = self.client.rpc('sql', {"query": query}).execute()
        month_names = {1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',
                       7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}
        return [{'month': r['month'], 'month_name': month_names.get(r['month'], 'Unknown'), 'count': r['count']} for r in res.data or []]

    def get_exclusion_reasons(self, table_name: str, sheet_identifier: str) -> List[Dict[str, Any]]:
        table = self._safe_table_name(table_name, sheet_identifier, 'excluded')
        # Split and count reasons directly in SQL using string functions if supported, otherwise fetch counts
        query = f"""
            SELECT exclusion_reason, COUNT(*) as count
            FROM {table}
            WHERE exclusion_reason IS NOT NULL
            GROUP BY exclusion_reason
            ORDER BY count DESC;
        """
        res = self.client.rpc('sql', {"query": query}).execute()
        return [{'reason': r['exclusion_reason'], 'count': r['count']} for r in res.data or []]

    def get_comprehensive_analytics(self, table_name: str, sheet_identifier: str) -> Dict[str, Any]:
        return {
            'dataset_sizes': self.get_dataset_sizes(table_name, sheet_identifier),
            'uniqueness_metrics': self.get_uniqueness_metrics(table_name, sheet_identifier),
            'birth_year_distribution': self.get_birth_year_distribution(table_name, sheet_identifier),
            'birth_month_distribution': self.get_birth_month_distribution(table_name, sheet_identifier),
            'exclusion_reasons': self.get_exclusion_reasons(table_name, sheet_identifier)
        }
