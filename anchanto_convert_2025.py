import pandas as pd
import numpy as np
import io
import json
import time
import os
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# ========================= CONFIGURATION =========================
SOURCE_FOLDER_ID = "1qYlq95IOlH0WOwGpJXSAH2hPfjmpFj_0"
DEST_FOLDER_ID = "1rpHkaU_jeWpmmkonWZ6NeuRjdIa4G_sh"
ROWS_PER_SHEET = 300000   # rows per Google Sheet
# =================================================================

# Load secrets from environment variables
CLIENT_SECRET_JSON = json.loads(os.environ["CLIENT_SECRET_JSON"])
REFRESH_TOKEN = os.environ["REFRESH_TOKEN"]

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

creds = Credentials(
    token=None,
    refresh_token=REFRESH_TOKEN,
    client_id=CLIENT_SECRET_JSON['installed']['client_id'],
    client_secret=CLIENT_SECRET_JSON['installed']['client_secret'],
    token_uri='https://oauth2.googleapis.com/token',
    scopes=SCOPES
)

creds.refresh(Request())

gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

# ===================== Helper functions =====================
PROCESSED_FILE = "processed_timestamps.json"

def get_processed_timestamps(folder_id):
    query = f"'{folder_id}' in parents and name='{PROCESSED_FILE}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    if files:
        file_id = files[0]['id']
        content = drive_service.files().get_media(fileId=file_id).execute()
        return json.loads(content.decode('utf-8'))
    else:
        return {}

def save_processed_timestamps(folder_id, timestamps):
    content = json.dumps(timestamps).encode('utf-8')
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype='application/json', resumable=True)
    query = f"'{folder_id}' in parents and name='{PROCESSED_FILE}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    if files:
        drive_service.files().update(
            fileId=files[0]['id'],
            media_body=media,
            fields='id'
        ).execute()
    else:
        file_metadata = {
            'name': PROCESSED_FILE,
            'mimeType': 'application/json',
            'parents': [folder_id]
        }
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

def list_xlsx_files(folder_id):
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def split_excel_to_sheets(file_id, file_name, dest_folder_id):
    request = drive_service.files().get_media(fileId=file_id)
    excel_bytes = request.execute()
    df = pd.read_excel(io.BytesIO(excel_bytes), engine='openpyxl')
    if df.empty:
        return 0
    if df.shape[1] > 16:
        df = df.iloc[:, :16]
    df = df.replace([np.inf, -np.inf], np.nan).fillna('')
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
        df[col] = df[col].astype(str)

    total_rows = len(df)
    parts = (total_rows + ROWS_PER_SHEET - 1) // ROWS_PER_SHEET
    base_name = file_name.replace('.xlsx', '')
    created = 0
    for part_num in range(1, parts + 1):
        start = (part_num - 1) * ROWS_PER_SHEET
        chunk = df.iloc[start:start + ROWS_PER_SHEET]
        new_name = f"{base_name} ({part_num:02d})"

        query = f"'{dest_folder_id}' in parents and name='{new_name}' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        existing = drive_service.files().list(q=query, fields="files(id)").execute().get('files', [])
        if existing:
            drive_service.files().delete(fileId=existing[0]['id']).execute()

        file_metadata = {
            'name': new_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [dest_folder_id]
        }
        new_file = drive_service.files().create(body=file_metadata, fields='id').execute()
        sheet_id = new_file['id']

        data = [chunk.columns.tolist()] + chunk.values.tolist()
        rows_total = len(data)
        cols = len(data[0]) if data else 0

        sh = gc.open_by_key(sheet_id)
        worksheet = sh.get_worksheet(0)
        worksheet.resize(rows=rows_total, cols=cols)

        batch_size = 100000
        for batch_start in range(0, rows_total, batch_size):
            batch_end = min(batch_start + batch_size, rows_total)
            batch_data = data[batch_start:batch_end]
            start_cell = f"A{batch_start + 1}"
            worksheet.update(range_name=start_cell, values=batch_data, value_input_option='USER_ENTERED')

        created += 1
    return created

# ===================== Main execution =====================
xlsx_files = list_xlsx_files(SOURCE_FOLDER_ID)
timestamps = get_processed_timestamps(SOURCE_FOLDER_ID)
print(f"Loaded {len(timestamps)} previously processed files.")

for file in xlsx_files:
    file_id = file['id']
    file_name = file['name']
    file_info = drive_service.files().get(fileId=file_id, fields='modifiedTime').execute()
    modified_time = file_info['modifiedTime']

    if file_id in timestamps and timestamps[file_id] == modified_time:
        print(f"Skipping: {file_name} (unchanged)")
        continue

    print(f"Processing: {file_name} (modified: {modified_time}) ...", end=' ')
    try:
        start_time = time.time()
        parts = split_excel_to_sheets(file_id, file_name, DEST_FOLDER_ID)
        elapsed = time.time() - start_time
        print(f"done → {parts} sheet(s) in {elapsed:.1f}s")
        timestamps[file_id] = modified_time
    except Exception as e:
        print(f"FAILED: {e}")

save_processed_timestamps(SOURCE_FOLDER_ID, timestamps)
print("Conversion finished. Timestamps saved.")
