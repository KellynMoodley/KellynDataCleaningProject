"""
Analytics Module
Provides comprehensive analytics and statistics for cleaned datasets.
"""

from typing import List, Dict, Any, Tuple
from collections import Counter, defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """Handles all analytics and statistical computations"""
    
    def __init__(self, included_data: List[Dict[str, Any]], excluded_data: List[Dict[str, Any]], 
                 original_count: int):
        """
        Initialize analytics engine
        
        Args:
            included_data: List of included records
            excluded_data: List of excluded records
            original_count: Total number of original rows
        """
        self.included_data = included_data
        self.excluded_data = excluded_data
        self.original_count = original_count
    
    def get_dataset_sizes(self) -> Dict[str, Any]:
        """
        Calculate dataset size metrics
        
        Returns:
            Dictionary containing size metrics
        """
        included_count = len(self.included_data)
        excluded_count = len(self.excluded_data)
        
        return {
            'original_row_count': self.original_count,
            'included_row_count': included_count,
            'excluded_row_count': excluded_count,
            'percent_included_vs_original': round((included_count / self.original_count * 100), 2) if self.original_count > 0 else 0,
            'percent_excluded_vs_original': round((excluded_count / self.original_count * 100), 2) if self.original_count > 0 else 0
        }
    
    def get_uniqueness_metrics(self) -> Dict[str, Any]:
        """
        Calculate uniqueness and combination metrics
        
        Returns:
            Dictionary containing uniqueness metrics
        """
        if not self.included_data:
            return {
                'unique_names': 0,
                'unique_birthday_combinations': 0,
                'unique_name_year': 0,
                'unique_name_month': 0,
                'unique_name_day': 0
            }
        
        # Extract fields
        names = [row['name'] for row in self.included_data]
        
        # Unique names
        unique_names = len(set(names))
        
        # Unique birthday combinations (day, month, year)
        birthday_combos = set()
        for row in self.included_data:
            combo = (row['birth_day'], row['birth_month'], row['birth_year'])
            birthday_combos.add(combo)
        
        # Unique name + year combinations
        name_year_combos = set()
        for row in self.included_data:
            combo = (row['name'], row['birth_year'])
            name_year_combos.add(combo)
        
        # Unique name + month combinations
        name_month_combos = set()
        for row in self.included_data:
            combo = (row['name'], row['birth_month'])
            name_month_combos.add(combo)
        
        # Unique name + day combinations
        name_day_combos = set()
        for row in self.included_data:
            combo = (row['name'], row['birth_day'])
            name_day_combos.add(combo)
        
        return {
            'unique_names': unique_names,
            'unique_birthday_combinations': len(birthday_combos),
            'unique_name_year': len(name_year_combos),
            'unique_name_month': len(name_month_combos),
            'unique_name_day': len(name_day_combos)
        }
    
    def get_duplicate_analysis(self) -> Dict[str, Any]:
        """
        Analyze duplicate records based on various field combinations
        
        Returns:
            Dictionary containing duplicate analysis
        """
        if not self.included_data:
            return {
                'total_duplicate_records': 0,
                'duplicate_groups': []
            }
        
        # Track combinations with at least 2 matching fields
        duplicate_groups = defaultdict(list)
        
        for row in self.included_data:
            name = row['name']
            day = row['birth_day']
            month = row['birth_month']
            year = row['birth_year']
            
            # Check all 2-field combinations
            combinations = [
                ('name_day', f"{name}|{day}"),
                ('name_month', f"{name}|{month}"),
                ('name_year', f"{name}|{year}"),
                ('day_month', f"{day}|{month}"),
                ('day_year', f"{day}|{year}"),
                ('month_year', f"{month}|{year}")
            ]
            
            for combo_type, combo_key in combinations:
                duplicate_groups[(combo_type, combo_key)].append(row)
        
        # Filter to only groups with 2+ records
        significant_duplicates = []
        total_duplicate_records = 0
        
        for (combo_type, combo_key), records in duplicate_groups.items():
            if len(records) >= 2:
                significant_duplicates.append({
                    'combination_type': combo_type,
                    'combination_value': combo_key,
                    'count': len(records),
                    'records': records[:10]  # Limit to first 10 for display
                })
                total_duplicate_records += len(records)
        
        # Sort by count descending
        significant_duplicates.sort(key=lambda x: x['count'], reverse=True)
        
        return {
            'total_duplicate_records': total_duplicate_records,
            'unique_duplicate_groups': len(significant_duplicates),
            'duplicate_groups': significant_duplicates[:50]  # Return top 50 groups
        }
    
    def get_birth_year_distribution(self) -> List[Dict[str, Any]]:
        """
        Get distribution of records by birth year
        
        Returns:
            List of dictionaries with year and count
        """
        if not self.included_data:
            return []
        
        year_counts = Counter([row['birth_year'] for row in self.included_data])
        
        distribution = [
            {'year': year, 'count': count}
            for year, count in sorted(year_counts.items())
        ]
        
        return distribution
    
    def get_birth_month_distribution(self) -> List[Dict[str, Any]]:
        """
        Get distribution of records by birth month
        
        Returns:
            List of dictionaries with month and count
        """
        if not self.included_data:
            return []
        
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
        month_counts = Counter([row['birth_month'] for row in self.included_data])
        
        distribution = [
            {'month': month, 'month_name': month_names.get(month, 'Unknown'), 'count': count}
            for month, count in sorted(month_counts.items())
        ]
        
        return distribution
    
    def get_exclusion_reasons_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary of exclusion reasons
        
        Returns:
            List of dictionaries with reason and count
        """
        if not self.excluded_data:
            return []
        
        # Count each individual reason (split combined reasons)
        reason_counts = Counter()
        
        for row in self.excluded_data:
            reasons = row.get('exclusion_reason', '').split(';')
            for reason in reasons:
                reason = reason.strip()
                if reason:
                    reason_counts[reason] += 1
        
        summary = [
            {'reason': reason, 'count': count}
            for reason, count in reason_counts.most_common()
        ]
        
        return summary
    
    def get_comprehensive_analytics(self) -> Dict[str, Any]:
        """
        Get all analytics in a single comprehensive report
        
        Returns:
            Dictionary containing all analytics
        """
        return {
            'dataset_sizes': self.get_dataset_sizes(),
            'uniqueness_metrics': self.get_uniqueness_metrics(),
            'duplicate_analysis': self.get_duplicate_analysis(),
            'birth_year_distribution': self.get_birth_year_distribution(),
            'birth_month_distribution': self.get_birth_month_distribution(),
            'exclusion_reasons': self.get_exclusion_reasons_summary()
        }