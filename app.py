"""
Flask Application for Data Cleaning Dashboard
Main application file with all routes and endpoints.
ENHANCED VERSION - Stores original data in Supabase, supports Excel config
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

# Sheet configurations - Now supporting both Google Sheets and Excel
SHEETS_CONFIG = {
    'sheet1': {
        'type': 'google_sheets',
        'spreadsheet_id': "1kKprUOWWZ8kFP2CkMhzdqiD0iHKBH7et7aOnuVF_miY",
        'range_name': "01_jan",
        'identifier': 'jan',
        'display_name': '01_jan (January Data)'
    },
    'sheet2': {
        'type': 'google_sheets',
        'spreadsheet_id': "1V9MQrvQS8N4Di3exRvNwhrwgwfccNmK5TwF1mV_jHdk",
        'range_name': "04_apr",
        'identifier': 'apr',
        'display_name': '04_apr (April Data)'
    },
    # Example Excel config (uncomment and configure as needed)
    # 'sheet3': {
    #     'type': 'excel',
    #     'spreadsheet_id': "YOUR_EXCEL_SPREADSHEET_ID",
    #     'range_name': "Sheet1",
    #     'identifier': 'excel_data',
    #     'display_name': 'Excel Import Data'
    # }
}

# Initialize Supabase
supabase_manager = None

def init_supabase():
    """Initialize Supabase manager"""
    global supabase_manager
    if supabase_manager is None:
        try:
            supabase_manager = SupabaseManager()
            print("Supabase initialized successfully")
        except Exception as e:
            print(f"Error: Supabase initialization failed: {e}")
            raise


def get_sheet_data(spreadsheet_id, range_name):
    """Fetch data from Google Sheets with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            return result.get("values", [])
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Retry {attempt + 1}/{max_retries} for sheet data: {e}")
                time.sleep(1)
            else:
                print(f"Failed to fetch sheet data after {max_retries} attempts: {e}")
                raise


def store_original_data_in_supabase(sheet_key):
    """
    Fetch original data from Google Sheets and store in Supabase
    Returns the number of rows stored
    """
    init_supabase()
    
    config = SHEETS_CONFIG[sheet_key]
    
    # Fetch raw data from Google Sheets
    raw_data = get_sheet_data(config['spreadsheet_id'], config['range_name'])
    
    if not raw_data or len(raw_data) < 2:
        raise ValueError("No data found in sheet")
    
    # Parse data into dictionaries with row_id
    headers = raw_data[0]
    rows = []
    for idx, row_data in enumerate(raw_data[1:], start=1):
        row_dict = {'original_row_number': idx}
        for i, header in enumerate(headers):
            row_dict[header] = row_data[i] if i < len(row_data) else ''
        rows.append(row_dict)
    
    # Store in Supabase
    supabase_manager.create_original_table('clients_2025', config['identifier'])
    supabase_manager.insert_original_data('clients_2025', config['identifier'], rows)
    
    # Create indexes after bulk insert
    #supabase_manager.create_indexes('clients_2025', config['identifier'])
    
    return len(rows)


# =========================
# Frontend Routes
# =========================

@app.route('/')
def index():
    """Serve main page"""
    return render_template('index.html', sheets=SHEETS_CONFIG)


# =========================
# API Routes - Sheet Management
# =========================

@app.route('/api/load_sheet/<sheet_key>')
def load_sheet(sheet_key):
    """
    Load original data from Google Sheets and store in Supabase
    Returns status and row count
    """
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Check if original data already exists
        existing_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        
        if existing_count > 0:
            return jsonify({
                'success': True,
                'message': 'Data already loaded',
                'row_count': existing_count,
                'already_exists': True
            })
        
        # Load and store original data
        row_count = store_original_data_in_supabase(sheet_key)
        
        return jsonify({
            'success': True,
            'message': 'Data loaded successfully',
            'row_count': row_count,
            'already_exists': False
        })
    
    except Exception as e:
        print(f"Error loading sheet: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/get_original_data/<sheet_key>')
def get_original_data(sheet_key):
    """Get original data with pagination"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Get paginated data
        offset = (page - 1) * per_page
        data = supabase_manager.get_original_data('clients_2025', config['identifier'], 
                                                   limit=per_page, offset=offset)
        
        # Get total count
        total_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        total_pages = (total_count + per_page - 1) // per_page
        
        return jsonify({
            'success': True,
            'data': data,
            'page': page,
            'per_page': per_page,
            'total_records': total_count,
            'total_pages': total_pages
        })
    
    except Exception as e:
        print(f"Error getting original data: {str(e)}")
        return jsonify({'error': str(e)}), 500


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
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Get ALL original data from Supabase
        original_data = supabase_manager.get_all_original_data('clients_2025', config['identifier'])
        
        if not original_data:
            return jsonify({'error': 'No original data found. Please load the sheet first.'}), 400
        
        # Clean data
        cleaner = DataCleaner()
        included_data, excluded_data = cleaner.clean_dataset(original_data)
        
        # Create tables
        supabase_manager.create_table_if_not_exists('clients_2025', config['identifier'])
        
        # Insert data with matching row IDs
        if included_data:
            supabase_manager.insert_included_data('clients_2025', config['identifier'], included_data)
        if excluded_data:
            supabase_manager.insert_excluded_data('clients_2025', config['identifier'], excluded_data)
            
        # Create indexes after bulk insert
        #supabase_manager.create_indexes('clients_2025', config['identifier'])
        
        # Generate analytics
        analytics = AnalyticsEngine(included_data, excluded_data, len(original_data))
        analytics_data = analytics.get_comprehensive_analytics()
        
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


@app.route('/api/check_cleaning_status/<sheet_key>')
def check_cleaning_status(sheet_key):
    """Check if sheet has been cleaned and return counts"""
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'cleaned': False})
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Check counts
        original_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        included_count = supabase_manager.count_records('clients_2025', config['identifier'], 'included')
        excluded_count = supabase_manager.count_records('clients_2025', config['identifier'], 'excluded')
        
        if included_count > 0 or excluded_count > 0:
            # Get data for analytics
            included_data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
            excluded_data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
            
            
            # Generate analytics
            analytics = AnalyticsEngine(included_data, excluded_data, original_count)
            analytics_data = analytics.get_comprehensive_analytics()
            
            return jsonify({
                'success': True,
                'original_loaded': original_count > 0,
                'cleaned': True,
                'original_count': original_count,
                'included_count': included_count,
                'excluded_count': excluded_count,
                'analytics': analytics_data
            })
        
        return jsonify({
            'success': True,
            'original_loaded': original_count > 0,
            'cleaned': False,
            'original_count': original_count
        })
    
    except Exception as e:
        print(f"Error checking cleaning status: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/get_cleaned_data/<sheet_key>')
def get_cleaned_data(sheet_key):
    """Get cleaned data for a sheet with pagination support"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        data_type = request.args.get('type', 'included')  # 'included' or 'excluded'
        
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 404
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        offset = (page - 1) * per_page
        
        if data_type == 'included':
            data = supabase_manager.get_included_data('clients_2025', config['identifier'], 
                                                     limit=per_page, offset=offset)
            total_count = supabase_manager.count_records('clients_2025', config['identifier'], 'included')
        else:
            data = supabase_manager.get_excluded_data('clients_2025', config['identifier'], 
                                                     limit=per_page, offset=offset)
            total_count = supabase_manager.count_records('clients_2025', config['identifier'], 'excluded')
        
        total_pages = (total_count + per_page - 1) // per_page
        
        return jsonify({
            'success': True,
            'data': data,
            'page': page,
            'per_page': per_page,
            'total_records': total_count,
            'total_pages': total_pages,
            'type': data_type
        })
    
    except Exception as e:
        print(f"Error getting cleaned data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_analytics/<sheet_key>')
def get_analytics(sheet_key):
    """Get comprehensive analytics for a cleaned sheet"""
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 404
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Get all data for analytics
        included_data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
        excluded_data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
        original_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        
        if not included_data and not excluded_data:
            return jsonify({
                'success': False,
                'error': 'No cleaned data found. Please clean the sheet first.'
            }), 404
        
        # Generate analytics
        analytics = AnalyticsEngine(included_data, excluded_data, original_count)
        analytics_data = analytics.get_comprehensive_analytics()
        
        return jsonify({
            'success': True,
            'analytics': analytics_data,
            'sheet_key': sheet_key
        })
        
    except Exception as e:
        print(f"Error getting analytics for {sheet_key}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# =========================
# Download Routes
# =========================

@app.route('/api/download/included_csv/<sheet_key>')
def download_included_csv(sheet_key):
    """Download included data as CSV"""
    try:
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
        report_gen = ReportGenerator()
        
        columns = ['row_id', 'name', 'birth_day', 'birth_month', 'birth_year']
        csv_data = report_gen.generate_csv(data, columns)
        
        return send_file(
            io.BytesIO(csv_data.encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'included_data_{sheet_key}.csv'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/excluded_csv/<sheet_key>')
def download_excluded_csv(sheet_key):
    """Download excluded data as CSV"""
    try:
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
        report_gen = ReportGenerator()
        
        columns = ['row_id', 'original_name', 'original_birth_day', 'original_birth_month', 
                   'original_birth_year', 'exclusion_reason']
        csv_data = report_gen.generate_csv(data, columns)
        
        return send_file(
            io.BytesIO(csv_data.encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'excluded_data_{sheet_key}.csv'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/included_pdf/<sheet_key>')
def download_included_pdf(sheet_key):
    """Download included data report as PDF"""
    try:
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
        
        # Get analytics
        excluded_data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
        original_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        analytics = AnalyticsEngine(data, excluded_data, original_count)
        analytics_data = analytics.get_comprehensive_analytics()
        
        report_gen = ReportGenerator()
        pdf_bytes = report_gen.generate_included_pdf(data, analytics_data, config['display_name'])
        
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'included_report_{sheet_key}.pdf'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/excluded_pdf/<sheet_key>')
def download_excluded_pdf(sheet_key):
    """Download excluded data report as PDF"""
    try:
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
        
        # Get analytics
        included_data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
        original_count = supabase_manager.count_records('clients_2025', config['identifier'], 'original')
        analytics = AnalyticsEngine(included_data, data, original_count)
        analytics_data = analytics.get_comprehensive_analytics()
        
        report_gen = ReportGenerator()
        pdf_bytes = report_gen.generate_excluded_pdf(data, analytics_data, config['display_name'])
        
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'excluded_report_{sheet_key}.pdf'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)