from flask import Flask, render_template, request, jsonify
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import time

app = Flask(__name__)

# Google Sheets setup
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

SPREADSHEET_ID1 = "1kKprUOWWZ8kFP2CkMhzdqiD0iHKBH7et7aOnuVF_miY"
RANGE_NAME1 = "01_jan"

SPREADSHEET_ID2 = "1V9MQrvQS8N4Di3exRvNwhrwgwfccNmK5TwF1mV_jHdk"
RANGE_NAME2 = "04_apr"

# Cache setup
cache = {"sheet1": None, "sheet2": None, "timestamp": 0}
CACHE_DURATION = 300  # 5 minutes
PAGE_SIZE = 1000  # rows per page

def get_sheet_data(spreadsheet_id, range_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    return result.get("values", [])

def get_cached_sheet_data(sheet_key, spreadsheet_id, range_name):
    current_time = time.time()
    if cache[sheet_key] is None or current_time - cache["timestamp"] > CACHE_DURATION:
        cache[sheet_key] = get_sheet_data(spreadsheet_id, range_name)
        cache["timestamp"] = current_time
    return cache[sheet_key]

# =========================
# Serve the frontend HTML
# =========================
@app.route('/')
def index():
    return render_template('webpage.html')  # <- serve the HTML from templates folder

# =========================
# API routes
# =========================
@app.route('/get_sheet_info')
def get_sheet_info():
    sheet = request.args.get('sheet')
    if sheet == 'sheet1':
        data = get_cached_sheet_data("sheet1", SPREADSHEET_ID1, RANGE_NAME1)
    else:
        data = get_cached_sheet_data("sheet2", SPREADSHEET_ID2, RANGE_NAME2)
    return jsonify({"totalRows": len(data), "pageSize": PAGE_SIZE})

@app.route('/get_page')
def get_page():
    sheet = request.args.get('sheet')
    page = int(request.args.get('page', 0))
    if sheet == 'sheet1':
        data = get_cached_sheet_data("sheet1", SPREADSHEET_ID1, RANGE_NAME1)
    else:
        data = get_cached_sheet_data("sheet2", SPREADSHEET_ID2, RANGE_NAME2)

    if not data:
        return jsonify({"header": [], "rows": []})

    header = data[0]
    start = 1 + page*PAGE_SIZE
    end = start + PAGE_SIZE
    rows = data[start:end]
    return jsonify({"header": header, "rows": rows})

if __name__ == '__main__':
    app.run(debug=True)