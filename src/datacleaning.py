"""
Data Cleaning Module
Implements validation rules and data cleaning logic for client datasets.
"""

import uuid
import re
from typing import List, Dict, Any, Tuple
import logging
import os
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


class DataCleaner:
    """Handles data validation and cleaning operations"""
    
    def __init__(self):
        """Initialize the DataCleaner"""
        self.included_data = []
        self.excluded_data = []
    
    @staticmethod
    def is_valid_name(name: str) -> Tuple[bool, str]:
        """
        Validate name field - allows letters, spaces, and Unicode characters (including emojis)
        
        Args:
            name: Name string to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name or not name.strip():
            return False, "missing name"
        
        name = name.strip()
        
        if len(name) < 2:
            return False, "name too short"
        
        # Check for characters other than A-Z, a-z, and spaces
        if not re.match(r'^[A-Za-z\s]+$', name):
            return False, "special character in name"
        
        return True, ""
    
    @staticmethod
    def is_valid_numeric(value: str, field_name: str) -> Tuple[bool, str, int]:
        """
        Validate numeric fields
        
        Args:
            value: Value to validate
            field_name: Name of the field for error messages
        
        Returns:
            Tuple of (is_valid, error_message, parsed_value)
        """
        if not value or str(value).strip() == "":
            return False, f"missing {field_name}", 0
        
        try:
            num = int(float(str(value).strip()))
            return True, "", num
        except (ValueError, TypeError):
            return False, f"invalid {field_name} (not numeric)", 0
    
    @staticmethod
    def is_valid_day(day: int) -> Tuple[bool, str]:
        """Validate day is between 1-31"""
        if day < 1 or day > 31:
            return False, "invalid day (not 1-31)"
        return True, ""
    
    @staticmethod
    def is_valid_month(month: int) -> Tuple[bool, str]:
        """Validate month is between 1-12"""
        if month < 1 or month > 12:
            return False, "invalid month (not 1-12)"
        return True, ""
    
    @staticmethod
    def is_valid_year(year: int) -> Tuple[bool, str]:
        """Validate year is 1940 or later"""
        if year < 1940:
            return False, "birth_year older than 1940"
        return True, ""
    
    def clean_row(self, row: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        """
        Clean and validate a single row
        
        Args:
            row: Dictionary containing row data (must include row_id and original_row_number)
        
        Returns:
            Tuple of (is_valid, cleaned_row, error_messages)
        """
        errors = []
        cleaned = {}
        
        # Preserve row_id from input (generated during Google Sheets read)
        cleaned['row_id'] = row.get('row_id')
        # Preserve original row number
        cleaned['original_row_number'] = row.get('original_row_number')
        
        # Extract original values
        name = row.get('firstname', '') or ''
        birth_day = row.get('birthday', '') or ''
        birth_month = row.get('birthmonth', '') or ''
        birth_year = row.get('birthyear', '') or ''
        
        # Validate name
        name_valid, name_error = self.is_valid_name(str(name))
        if not name_valid:
            errors.append(name_error)
        else:
            cleaned['name'] = str(name).strip()
        
        # Validate day
        day_valid, day_error, day_value = self.is_valid_numeric(birth_day, 'birth_day')
        if not day_valid:
            errors.append(day_error)
        else:
            day_range_valid, day_range_error = self.is_valid_day(day_value)
            if not day_range_valid:
                errors.append(day_range_error)
            else:
                cleaned['birth_day'] = day_value
        
        # Validate month
        month_valid, month_error, month_value = self.is_valid_numeric(birth_month, 'birth_month')
        if not month_valid:
            errors.append(month_error)
        else:
            month_range_valid, month_range_error = self.is_valid_month(month_value)
            if not month_range_valid:
                errors.append(month_range_error)
            else:
                cleaned['birth_month'] = month_value
        
        # Validate year
        year_valid, year_error, year_value = self.is_valid_numeric(birth_year, 'birth_year')
        if not year_valid:
            errors.append(year_error)
        else:
            year_range_valid, year_range_error = self.is_valid_year(year_value)
            if not year_range_valid:
                errors.append(year_range_error)
            else:
                cleaned['birth_year'] = year_value
        
        # Store original values for excluded data
        cleaned['original_name'] = str(name) if name else None
        cleaned['original_birth_day'] = str(birth_day) if birth_day else None
        cleaned['original_birth_month'] = str(birth_month) if birth_month else None
        cleaned['original_birth_year'] = str(birth_year) if birth_year else None
        
        is_valid = len(errors) == 0
        return is_valid, cleaned, errors
    
    
    def clean_dataset(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Clean entire dataset
        
        Args:
            data: List of row dictionaries (each with row_id and original_row_number)
        
        Returns:
            Tuple of (included_data, excluded_data)
        """
        self.included_data = []
        self.excluded_data = []
        
        logger.info(f"Starting to clean {len(data)} rows...")
        
        for idx, row in enumerate(data):
            is_valid, cleaned_row, errors = self.clean_row(row)
            
            if is_valid:
                # Only include valid fields for included data
                included_row = {
                    'row_id': cleaned_row['row_id'],
                    'original_row_number': cleaned_row['original_row_number'],
                    'name': cleaned_row['name'],
                    'birth_day': cleaned_row['birth_day'],
                    'birth_month': cleaned_row['birth_month'],
                    'birth_year': cleaned_row['birth_year']
                }
                self.included_data.append(included_row)
            else:
                # Create exclusion record with all reasons
                excluded_row = {
                    'row_id': cleaned_row['row_id'],
                    'original_row_number': cleaned_row['original_row_number'],
                    'original_name': cleaned_row['original_name'],
                    'original_birth_day': cleaned_row['original_birth_day'],
                    'original_birth_month': cleaned_row['original_birth_month'],
                    'original_birth_year': cleaned_row['original_birth_year'],
                    'exclusion_reason': '; '.join(errors)
                }
                self.excluded_data.append(excluded_row)
            
            if (idx + 1) % 10000 == 0:
                logger.info(f"Processed {idx + 1} rows...")
        
        logger.info(f"Cleaning complete: {len(self.included_data)} included, {len(self.excluded_data)} excluded")
        
        return self.included_data, self.excluded_data

    
    def get_cleaning_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of the cleaning process
        
        Returns:
            Dictionary containing summary statistics
        """
        total = len(self.included_data) + len(self.excluded_data)
        
        return {
            'total_rows': total,
            'included_count': len(self.included_data),
            'excluded_count': len(self.excluded_data),
            'included_percentage': (len(self.included_data) / total * 100) if total > 0 else 0,
            'excluded_percentage': (len(self.excluded_data) / total * 100) if total > 0 else 0
        }