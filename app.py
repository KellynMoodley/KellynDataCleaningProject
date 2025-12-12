"""
Flask Application for Data Cleaning Dashboard
Main application file with all routes and endpoints.
OPTIMIZED VERSION - Processes Google Sheets directly without intermediate storage
"""

from flask import Flask, render_template, request, jsonify, send_file
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import time
import os
import io
import uuid
from src import SupabaseManager, DataCleaner, ReportGenerator, AnalyticsEngine, SupabaseManagerSQL
import logging

app = Flask(__name__)

# Google Sheets setup
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sheet configurations
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
    }
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


def process_and_clean_sheet_data(sheet_key, batch_size=100000, store_original=True):
    """
    Process Google Sheets data directly with row_id generation and cleaning
    NOW WITH PARALLEL INSERTS for included + excluded tables
    
    Args:
        sheet_key: Sheet configuration key
        batch_size: Number of rows to process per batch (increased to 100K)
        store_original: Whether to store original data (optional, for audit trail)
    
    Returns:
        Tuple of (total_original, total_included, total_excluded)
    """
    config = SHEETS_CONFIG[sheet_key]
    
    # Step 1: Fetch ALL data from Google Sheets (this is fast!)
    print(f"Fetching data from Google Sheets: {config['display_name']}...")
    fetch_start = time.time()
    raw_data = get_sheet_data(config['spreadsheet_id'], config['range_name'])
    fetch_time = time.time() - fetch_start
    
    if not raw_data or len(raw_data) < 2:
        raise ValueError("No data found in sheet")
    
    headers = raw_data[0]
    total_rows = len(raw_data) - 1
    print(f"✓ Fetched {total_rows:,} rows from Google Sheets in {fetch_time:.1f}s")
    
    # Step 2: Create tables
    init_supabase()
    supabase_manager.create_table_if_not_exists('clients_2025', config['identifier'])
    if store_original:
        supabase_manager.create_original_table('clients_2025', config['identifier'])
    
    # Step 3: Clear existing data
    safe_table_name = 'clients_2025'.lower().replace(' ', '_').replace('-', '_')
    included_table = f"{safe_table_name}_{config['identifier']}_included"
    excluded_table = f"{safe_table_name}_{config['identifier']}_excluded"
    
    print("Clearing existing data...")
    supabase_manager.client.table(included_table).delete().neq('row_id', '00000000-0000-0000-0000-000000000000').execute()
    supabase_manager.client.table(excluded_table).delete().neq('row_id', '00000000-0000-0000-0000-000000000000').execute()
    
    if store_original:
        original_table = f"{safe_table_name}_{config['identifier']}_original"
        supabase_manager.client.table(original_table).delete().neq('row_id', '00000000-0000-0000-0000-000000000000').execute()
    
    # Step 4: Process in batches
    total_included = 0
    total_excluded = 0
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Processing {total_rows:,} rows in {num_batches} batches of {batch_size:,}...")
    overall_start = time.time()
    
    for batch_num in range(num_batches):
        batch_start_time = time.time()
        batch_start = batch_num * batch_size + 1  # +1 to skip header
        batch_end = min(batch_start + batch_size, len(raw_data))
        
        print(f"\nProcessing batch {batch_num + 1}/{num_batches} (rows {batch_start} to {batch_end - 1})...")
        
        # Parse batch with row_id generation
        batch_with_ids = []
        original_batch = []  # For optional storage
        
        for idx in range(batch_start, batch_end):
            row_data = raw_data[idx]
            row_id = str(uuid.uuid4())  # Generate once, use everywhere
            original_row_number = idx  # 1-based row number
            
            # Parse row data
            parsed_row = {
                'row_id': row_id,
                'original_row_number': original_row_number,
                'firstname': row_data[0] if len(row_data) > 0 else '',
                'birthday': row_data[1] if len(row_data) > 1 else '',
                'birthmonth': row_data[2] if len(row_data) > 2 else '',
                'birthyear': row_data[3] if len(row_data) > 3 else ''
            }
            batch_with_ids.append(parsed_row)
            
            # Store for original table if needed
            if store_original:
                original_batch.append({
                    'row_id': row_id,
                    'original_row_number': original_row_number,
                    'firstname': parsed_row['firstname'],
                    'birthday': parsed_row['birthday'],
                    'birthmonth': parsed_row['birthmonth'],
                    'birthyear': parsed_row['birthyear']
                })
        
        # Clean this batch
        clean_start = time.time()
        cleaner = DataCleaner()
        included_data, excluded_data = cleaner.clean_dataset(batch_with_ids)
        clean_time = time.time() - clean_start
        
        print(f"Batch {batch_num + 1}: Cleaned in {clean_time:.1f}s - {len(included_data)} included, {len(excluded_data)} excluded")
        
        # ============================================
        # PARALLEL INSERTS - Insert both tables simultaneously!
        # ============================================
        insert_start = time.time()
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            
            # Submit included insert job
            if included_data:
                futures.append(
                    executor.submit(
                        supabase_manager.append_included_data,
                        'clients_2025',
                        config['identifier'],
                        included_data,
                        5000,  # batch_size
                        8      # max_workers
                    )
                )
            
            # Submit excluded insert job
            if excluded_data:
                futures.append(
                    executor.submit(
                        supabase_manager.append_excluded_data,
                        'clients_2025',
                        config['identifier'],
                        excluded_data,
                        5000,  # batch_size
                        8      # max_workers
                    )
                )
            
            # Submit original insert job (if enabled)
            if store_original and original_batch:
                futures.append(
                    executor.submit(
                        supabase_manager.append_original_data,
                        'clients_2025',
                        config['identifier'],
                        original_batch,
                        5000,  # batch_size
                        8      # max_workers
                    )
                )
            
            # Wait for all inserts to complete
            for future in as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions that occurred
                except Exception as e:
                    print(f"Error in parallel insert: {e}")
                    raise
        
        insert_time = time.time() - insert_start
        
        # Update totals
        total_included += len(included_data)
        total_excluded += len(excluded_data)
        
        # Batch summary
        batch_total = time.time() - batch_start_time
        print(f"Batch {batch_num + 1}: Inserted in {insert_time:.1f}s (parallel)")
        print(f"Batch {batch_num + 1} total time: {batch_total:.1f}s")
    
    # Final summary
    total_time = time.time() - overall_start
    print(f"\n✓ Processing complete!")
    print(f"Total: {total_rows:,} rows")
    print(f"Included: {total_included:,} rows")
    print(f"Excluded: {total_excluded:,} rows")
    print(f"Total processing time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"Average rate: {total_rows/total_time:.0f} rows/sec")
    
    return total_rows, total_included, total_excluded


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
    Check if sheet data exists - no longer stores original separately
    """
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Check if cleaned data exists
        included_count = supabase_manager.count_included_records('clients_2025', config['identifier'], 'included')
        excluded_count = supabase_manager.count_excluded_records('clients_2025', config['identifier'], 'excluded')
        
        if included_count > 0 or excluded_count > 0:
            return jsonify({
                'success': True,
                'message': 'Data already processed',
                'row_count': included_count + excluded_count,
                'already_exists': True
            })
        
        return jsonify({
            'success': True,
            'message': 'Ready to clean',
            'row_count': 0,
            'already_exists': False
        })
    
    except Exception as e:
        print(f"Error checking sheet: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/get_original_data/<sheet_key>')
def get_original_data(sheet_key):
    """Get sample data from Google Sheets directly"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        config = SHEETS_CONFIG[sheet_key]
        
        # Fetch from Google Sheets
        raw_data = get_sheet_data(config['spreadsheet_id'], config['range_name'])
        
        if not raw_data or len(raw_data) < 2:
            return jsonify({
                'success': True,
                'data': [],
                'page': page,
                'per_page': per_page,
                'total_records': 0,
                'total_pages': 0
            })
        
        headers = raw_data[0]
        total_count = len(raw_data) - 1
        
        # Paginate
        start_idx = (page - 1) * per_page + 1
        end_idx = min(start_idx + per_page, len(raw_data))
        
        # Format data
        formatted_data = []
        for idx in range(start_idx, end_idx):
            if idx < len(raw_data):
                row_data = raw_data[idx]
                formatted_data.append({
                    'original_row_number': idx,
                    'firstname': row_data[0] if len(row_data) > 0 else '',
                    'birthday': row_data[1] if len(row_data) > 1 else '',
                    'birthmonth': row_data[2] if len(row_data) > 2 else '',
                    'birthyear': row_data[3] if len(row_data) > 3 else ''
                })
        
        total_pages = (total_count + per_page - 1) // per_page
        
        return jsonify({
            'success': True,
            'data': formatted_data,
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
    """Clean data by processing Google Sheets directly"""
    try:
        data = request.get_json()
        sheet_key = data.get('sheet')
        store_original = data.get('store_original', True)  # Optional parameter
        
        if not sheet_key or sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 400
        
        config = SHEETS_CONFIG[sheet_key]
        
        # Process and clean directly from Google Sheets
        print(f"Starting direct processing for {config['display_name']}...")
        total_original, total_included, total_excluded = process_and_clean_sheet_data(
            sheet_key,
            batch_size=100000,
            store_original=store_original
        )
        
        # Generate analytics
        init_supabase()
        included_data = supabase_manager.get_all_included_data('clients_2025', config['identifier'])
        excluded_data = supabase_manager.get_all_excluded_data('clients_2025', config['identifier'])
        
        analytics = AnalyticsEngine(included_data, excluded_data, total_original)
        analytics_data = analytics.get_comprehensive_analytics()
        
        return jsonify({
            'success': True,
            'message': 'Data cleaned successfully',
            'original_count': total_original,
            'included_count': total_included,
            'excluded_count': total_excluded,
            'analytics': analytics_data
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
        
        # Check counts ONLY
        included_count = supabase_manager.count_included_records('clients_2025', config['identifier'], 'included')
        excluded_count = supabase_manager.count_excluded_records('clients_2025', config['identifier'], 'excluded')
        original_count = included_count + excluded_count
        
        if included_count > 0 or excluded_count > 0:
            # DON'T fetch all data - just return counts
            # Analytics will be loaded separately when user clicks Analytics tab
            
            return jsonify({
                'success': True,
                'original_loaded': True,
                'cleaned': True,
                'original_count': original_count,
                'included_count': included_count,
                'excluded_count': excluded_count,
                'analytics': None  # Will load separately
            })
        
        return jsonify({
            'success': True,
            'original_loaded': False,
            'cleaned': False,
            'original_count': 0
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
        
        config = SHEETS_CONFIG[sheet_key]
        offset = (page - 1) * per_page

        if data_type == 'included':
            data = supabase_manager.get_records('clients_2025', config['identifier'], limit=per_page, offset=offset, excluded=False)

            total_count = supabase_manager.count_included_records('clients_2025', config['identifier'])
        else:
            data = supabase_manager.get_records('clients_2025', config['identifier'], limit=per_page, offset=offset, excluded=True)
            total_count = supabase_manager.count_excluded_records('clients_2025', config['identifier'])

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
        logger.error(f"Error getting cleaned data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/check_original_in_supabase/<sheet_key>')
def check_original_in_supabase(sheet_key):
    """Check if original data is stored in Supabase"""
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'exists': False})
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Check if original table has data
        original_count = supabase_manager.count_total_records('clients_2025', config['identifier'], 'original')
        
        return jsonify({
            'exists': original_count > 0,
            'count': original_count
        })
    
    except Exception as e:
        print(f"Error checking original data: {str(e)}")
        return jsonify({'exists': False}), 500


@app.route('/api/get_original_data_from_supabase/<sheet_key>')
def get_original_data_from_supabase(sheet_key):
    """Get original data from Supabase (if stored)"""
    try:
        if sheet_key not in SHEETS_CONFIG:
            return jsonify({'error': 'Invalid sheet'}), 404
        
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]
        
        # Get pagination params
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Get data from Supabase original table
        data = supabase_manager.get_original_data(
            'clients_2025',
            config['identifier'],
            limit=per_page,
            offset=offset
        )
        
        # Get total count
        total = supabase_manager.count_total_records('clients_2025', config['identifier'], 'original')
        
        # Calculate total pages
        total_pages = (total + per_page - 1) // per_page
        
        return jsonify({
            'success': True,
            'data': data,
            'total': total,
            'total_records': total,  # Add this for consistency
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages
        })
    
    except Exception as e:
        print(f"Error getting original data from Supabase: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get_analytics/<sheet_key>')
def get_analytics(sheet_key):
    try:
        init_supabase()
        config = SHEETS_CONFIG[sheet_key]

        print(f"Generating analytics for {sheet_key}...")
        start_time = time.time()

        # Use the new SQL-based AnalyticsEngine
        analytics_engine = AnalyticsEngine()
        analytics_data = analytics_engine.get_comprehensive_analytics(
            table_name='clients_2025',
            sheet_identifier=config['identifier']
        )

        elapsed = time.time() - start_time
        print(f"✓ Analytics generated in {elapsed:.1f}s")

        return jsonify({
            'success': True,
            'analytics': analytics_data,
            'sheet_key': sheet_key
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =========================
# Download Routes
# =========================

@app.route('/api/download/included_csv/<sheet_key>')
def download_included_csv(sheet_key):
    """Download included data as CSV using direct PostgreSQL COPY"""
    try:
        config = SHEETS_CONFIG[sheet_key]
        report_gen = ReportGenerator()
        
        # Use the fast COPY method
        csv_data = report_gen.generate_csv_direct(
            table_name='clients_2025',
            sheet_identifier=config['identifier'],
            is_excluded=False
        )
        
        return send_file(
            io.BytesIO(csv_data.encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'included_data_{sheet_key}.csv'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/excluded_csv/<sheet_key>')
def download_excluded_csv(sheet_key):
    """Download excluded data as CSV using direct PostgreSQL COPY"""
    try:
        config = SHEETS_CONFIG[sheet_key]
        report_gen = ReportGenerator()
        
        # Use the fast COPY method
        csv_data = report_gen.generate_csv_direct(
            table_name='clients_2025',
            sheet_identifier=config['identifier'],
            is_excluded=True
        )
        
        return send_file(
            io.BytesIO(csv_data.encode('utf-8-sig')),
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
        config = SHEETS_CONFIG[sheet_key]
        report_gen = ReportGenerator()
        
        # Generate PDF directly from database
        pdf_bytes = report_gen.generate_included_pdf_from_db(
            table_name='clients_2025',
            sheet_identifier=config['identifier'],
            sheet_name=config['display_name']
        )
        
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
        config = SHEETS_CONFIG[sheet_key]
        report_gen = ReportGenerator()
        
        # Generate PDF directly from database
        pdf_bytes = report_gen.generate_excluded_pdf_from_db(
            table_name='clients_2025',
            sheet_identifier=config['identifier'],
            sheet_name=config['display_name']
        )
        
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