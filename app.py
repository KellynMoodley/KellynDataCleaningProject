"""
Flask Application for Data Cleaning Dashboard
Main application file with all routes and endpoints.
"""

from flask import Flask, render_template, request, jsonify, send_file
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import time
import os
import io
from src import SupabaseManager, DataCleaner, ReportGenerator, AnalyticsEngine

app = Flask(__name__)

# Google Sheets setup
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

# Sheet configurations
SHEETS_CONFIG = {
    'sheet1': {
        'spreadsheet_id': "1kKprUOWWZ8kFP2CkMhzdqiD0iHKBH7et7aOnuVF_miY",
        'range_name': "01_jan",
        'identifier': 'jan',
        'display_name': '01_jan (January Data)'
    },
    'sheet2': {
        'spreadsheet_id': "1V9MQrvQS8N4Di3exRvNwhrwgwfccNmK5TwF1mV_jHdk",
        'range_name': "04_apr",
        'identifier': 'apr',
        'display_name': '04_apr (April Data)'
    }
}

# Cache setup
cache = {"sheet1": None, "sheet2": None, "timestamp": 0}
CACHE_DURATION = 300  # 5 minutes
PAGE_SIZE = 1000  # rows per page

# Cleaning results cache
cleaning_results = {}

# Initialize Supabase (will be initialized when needed)
supabase_manager = None


def init_supabase():
    """Initialize Supabase manager"""
    global supabase_manager
    if supabase_manager is None:
        try:
            supabase_manager = SupabaseManager()
            print("Supabase initialized successfully")
        except Exception as e:
            print(f"Warning: Supabase initialization failed: {e}")
            print("App will continue without Supabase functionality")


def get_sheet_data(spreadsheet_id, range_name):
    """Fetch data from Google Sheets"""
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    return result.get("values", [])


def get_cached_sheet_data(sheet_key):
    """Get cached sheet data or fetch if needed"""
    current_time = time.time()
    if cache[sheet_key] is None or current_time - cache["timestamp"] > CACHE_DURATION:
        config = SHEETS_CONFIG[sheet_key]
        cache[sheet_key] = get_sheet_data(config['spreadsheet_id'], config['range_name'])
        cache["timestamp"] = current_time
    return cache[sheet_key]


# =========================
# Frontend Routes
# =========================

@app.route('/')
def index():
    """Serve main page"""
    return render_template('index.html', sheets=SHEETS_CONFIG)


# =========================
# API Routes - Original Data
# =========================

@app.route('/get_sheet_info')
def get_sheet_info():
    """Get sheet information"""
    sheet = request.args.get('sheet')
    data = get_cached_sheet_data(sheet)
    return jsonify({"totalRows": len(data), "pageSize": PAGE_SIZE})


@app.route('/get_page')
def get_page():
    """Get paginated sheet data"""
    sheet = request.args.get('sheet')
    page = int(request.args.get('page', 0))
    data = get_cached_sheet_data(sheet)

    if not data:
        return jsonify({"header": [], "rows": []})

    header = data[0]
    start = 1 + page * PAGE_SIZE
    end = start + PAGE_SIZE
    rows = data[start:end]
    return jsonify({"header": header, "rows": rows})


# =========================
# API Routes - Data Cleaning
# =========================

@app.route('/api/clean_data', methods=['POST'])
def clean_data():
    """Clean data for a specific sheet"""
    try:
        data = request.get_json()
        sheet_key = data.get('sheet')
        
        if not sheet_key or sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        config = SHEETS_CONFIG[sheet_key]
        
        # Get raw data
        raw_data = get_cached_sheet_data(sheet_key)
        
        if not raw_data or len(raw_data) < 2:
            return jsonify({'error': 'No data to clean'}), 400
        
        # Parse data into dictionaries
        headers = raw_data[0]
        rows = []
        for row_data in raw_data[1:]:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = row_data[i] if i < len(row_data) else ''
            rows.append(row_dict)
        
        # Clean data
        cleaner = DataCleaner()
        included_data, excluded_data = cleaner.clean_dataset(rows)
        
        # Initialize Supabase if not already done
        init_supabase()
        
        # Store in Supabase if available
        if supabase_manager:
            try:
                # Create tables
                supabase_manager.create_table_if_not_exists('clients_2025', config['identifier'])
                
                # Insert data
                if included_data:
                    supabase_manager.insert_included_data('clients_2025', config['identifier'], included_data)
                if excluded_data:
                    supabase_manager.insert_excluded_data('clients_2025', config['identifier'], excluded_data)
            except Exception as e:
                print(f"Supabase error: {e}")
                # Continue without Supabase
        
        # Generate analytics
        analytics = AnalyticsEngine(included_data, excluded_data, len(rows))
        analytics_data = analytics.get_comprehensive_analytics()
        
        # Cache results
        cleaning_results[sheet_key] = {
            'included_data': included_data,
            'excluded_data': excluded_data,
            'analytics': analytics_data,
            'sheet_name': config['display_name']
        }
        
        return jsonify({
            'success': True,
            'message': 'Data cleaned successfully',
            'summary': analytics_data['dataset_sizes']
        })
    
    except Exception as e:
        print(f"Error cleaning data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/check_existing_data/<sheet_key>')
def check_existing_data(sheet_key):
    """Check if cleaned data already exists in Supabase"""
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'exists': False})
        
        # Initialize Supabase if not already done
        init_supabase()
        
        if not supabase_manager:
            return jsonify({'exists': False})
        
        config = SHEETS_CONFIG[sheet_key]
        
        # Check if data exists (will return 0 if tables don't exist)
        included_count = supabase_manager.count_records('clients_2025', config['identifier'], 'included')
        excluded_count = supabase_manager.count_records('clients_2025', config['identifier'], 'excluded')
        
        # Only proceed if we have actual data
        if included_count > 0 or excluded_count > 0:
            try:
                # Fetch the data
                included_data = supabase_manager.get_included_data('clients_2025', config['identifier'], limit=10000)
                excluded_data = supabase_manager.get_excluded_data('clients_2025', config['identifier'], limit=10000)
                
                # Try to get total rows count, but don't fail if Google Sheets is unavailable
                try:
                    raw_data = get_cached_sheet_data(sheet_key)
                    total_rows = len(raw_data) - 1 if raw_data else len(included_data) + len(excluded_data)
                except Exception as e:
                    print(f"Warning: Could not fetch raw data count: {e}")
                    # Use the sum of included and excluded as fallback
                    total_rows = len(included_data) + len(excluded_data)
                
                # Generate analytics from existing data
                analytics = AnalyticsEngine(included_data, excluded_data, total_rows)
                analytics_data = analytics.get_comprehensive_analytics()
                
                # Cache the results
                cleaning_results[sheet_key] = {
                    'included_data': included_data,
                    'excluded_data': excluded_data,
                    'analytics': analytics_data,
                    'sheet_name': config['display_name']
                }
                
                return jsonify({
                    'exists': True,
                    'included_count': included_count,
                    'excluded_count': excluded_count,
                    'summary': analytics_data['dataset_sizes']
                })
            except Exception as fetch_error:
                print(f"Error fetching existing data: {fetch_error}")
                import traceback
                traceback.print_exc()
                return jsonify({'exists': False})
        
        # No data found
        return jsonify({'exists': False})
    
    except Exception as e:
        print(f"Error checking existing data: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return false instead of error to allow normal operation
        return jsonify({'exists': False})


# Modify the existing get_cleaned_data route to support pagination
@app.route('/api/get_cleaned_data/<sheet_key>')
def get_cleaned_data(sheet_key):
    """Get cleaned data for a sheet with pagination support"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        data_type = request.args.get('type', 'all')  # 'included', 'excluded', or 'all'
        
        if sheet_key not in cleaning_results:
            return jsonify({'error': 'Data not cleaned yet'}), 404
        
        result = cleaning_results[sheet_key]
        
        response_data = {
            'analytics': result['analytics'],
            'total_included': len(result['included_data']),
            'total_excluded': len(result['excluded_data'])
        }
        
        # Calculate pagination for included data
        if data_type in ['included', 'all']:
            included_start = (page - 1) * per_page
            included_end = included_start + per_page
            response_data['included_data'] = result['included_data'][included_start:included_end]
            response_data['included_page'] = page
            response_data['included_total_pages'] = (len(result['included_data']) + per_page - 1) // per_page
        
        # Calculate pagination for excluded data
        if data_type in ['excluded', 'all']:
            excluded_start = (page - 1) * per_page
            excluded_end = excluded_start + per_page
            response_data['excluded_data'] = result['excluded_data'][excluded_start:excluded_end]
            response_data['excluded_page'] = page
            response_data['excluded_total_pages'] = (len(result['excluded_data']) + per_page - 1) // per_page
        
        return jsonify(response_data)
    
    except Exception as e:
        print(f"Error getting cleaned data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_analytics/<sheet_key>')
def get_analytics(sheet_key):
    """Get analytics for a cleaned sheet"""
    if sheet_key not in cleaning_results:
        return jsonify({'error': 'Data not cleaned yet'}), 404
    
    return jsonify(cleaning_results[sheet_key]['analytics'])


# =========================
# Download Routes
# =========================

@app.route('/api/download/included_csv/<sheet_key>')
def download_included_csv(sheet_key):
    """Download included data as CSV"""
    if sheet_key not in cleaning_results:
        return jsonify({'error': 'Data not cleaned yet'}), 404
    
    result = cleaning_results[sheet_key]
    report_gen = ReportGenerator()
    
    columns = ['row_id', 'name', 'birth_day', 'birth_month', 'birth_year']
    csv_data = report_gen.generate_csv(result['included_data'], columns)
    
    return send_file(
        io.BytesIO(csv_data.encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'included_data_{sheet_key}.csv'
    )


@app.route('/api/download/excluded_csv/<sheet_key>')
def download_excluded_csv(sheet_key):
    """Download excluded data as CSV"""
    if sheet_key not in cleaning_results:
        return jsonify({'error': 'Data not cleaned yet'}), 404
    
    result = cleaning_results[sheet_key]
    report_gen = ReportGenerator()
    
    columns = ['row_id', 'original_name', 'original_birth_day', 'original_birth_month', 
               'original_birth_year', 'exclusion_reason']
    csv_data = report_gen.generate_csv(result['excluded_data'], columns)
    
    return send_file(
        io.BytesIO(csv_data.encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'excluded_data_{sheet_key}.csv'
    )


@app.route('/api/download/included_pdf/<sheet_key>')
def download_included_pdf(sheet_key):
    """Download included data report as PDF"""
    if sheet_key not in cleaning_results:
        return jsonify({'error': 'Data not cleaned yet'}), 404
    
    result = cleaning_results[sheet_key]
    report_gen = ReportGenerator()
    
    pdf_bytes = report_gen.generate_included_pdf(
        result['included_data'],
        result['analytics'],
        result['sheet_name']
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'included_report_{sheet_key}.pdf'
    )


@app.route('/api/download/excluded_pdf/<sheet_key>')
def download_excluded_pdf(sheet_key):
    """Download excluded data report as PDF"""
    if sheet_key not in cleaning_results:
        return jsonify({'error': 'Data not cleaned yet'}), 404
    
    result = cleaning_results[sheet_key]
    report_gen = ReportGenerator()
    
    pdf_bytes = report_gen.generate_excluded_pdf(
        result['excluded_data'],
        result['analytics'],
        result['sheet_name']
    )
    
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'excluded_report_{sheet_key}.pdf'
    )


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)