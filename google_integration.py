# google_integration.py
import pandas as pd
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def init_google_service(credentials_json_path: str, token_path: str = "token.json"):
    """
    Returns Sheets service without re-auth every run.
    - Uses token_path if it exists
    - Refreshes token silently if expired
    - Opens browser only if no token yet
    """
    creds = None

    # Load existing token if available
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # If missing/invalid, refresh or run OAuth once
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_json_path, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save token so next run won't ask again
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


# ---------- CONFIG ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheets_service(token_path):
    """
    Create a Sheets API service using user OAuth credentials
    token_path: path to token.json (or the OAuth credentials JSON)
    """
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"{token_path} not found. Please authenticate first.")
    
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service

def find_deal_row_by_id(service, sheet_id, deal_id, sheet_name="Sheet1"):
    """
    Returns the sheet row number (1-based) where Deal - ID == deal_id.
    Returns None if not found.
    """
    sheet = service.spreadsheets()

    # Read all values
    resp = sheet.values().get(
        spreadsheetId=sheet_id,
        range=sheet_name
    ).execute()

    values = resp.get("values", [])
    if not values:
        return None

    headers = values[0]
    if "Deal - ID" not in headers:
        raise ValueError("Column 'Deal - ID' not found in the sheet headers.")

    deal_id_col = headers.index("Deal - ID")

    # Data starts at row 2 in Google Sheets
    for i, row in enumerate(values[1:], start=2):
        cell = row[deal_id_col] if deal_id_col < len(row) else ""
        if str(cell).strip() == str(deal_id).strip():
            return i

    return None

def col_to_letter(n: int) -> str:
    """1 -> A, 2 -> B, ..., 26 -> Z, 27 -> AA, etc."""
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

def is_row_uploaded(service, sheet_id, row_number, sheet_name="Sheet1"):
    """
    Returns True if the given row has Uploaded == YES (case-insensitive).
    Returns False if Uploaded column doesn't exist or value is not YES.
    """
    sheet = service.spreadsheets()

    resp = sheet.values().get(
        spreadsheetId=sheet_id,
        range=sheet_name
    ).execute()

    values = resp.get("values", [])
    if not values:
        return False

    headers = values[0]
    if "Uploaded" not in headers:
        return False

    uploaded_col = headers.index("Uploaded")

    idx_in_data = row_number - 2  # row 2 is first data row
    data_rows = values[1:]

    if idx_in_data < 0 or idx_in_data >= len(data_rows):
        return False

    row = data_rows[idx_in_data]
    val = row[uploaded_col] if uploaded_col < len(row) else ""
    return str(val).strip().upper() == "YES"

def update_deal_row_in_sheet(service, deal_dict, sheet_id, row_number, sheet_name="Sheet1"):
    """
    Updates an existing row by aligning values to headers (row 1).
    Keeps existing values for headers not present in deal_dict.
    """
    sheet = service.spreadsheets()

    resp = sheet.values().get(
        spreadsheetId=sheet_id,
        range=sheet_name
    ).execute()

    values = resp.get("values", [])
    if not values:
        raise ValueError("Sheet is empty; cannot update row.")

    headers = values[0]
    data_rows = values[1:]
    idx_in_data = row_number - 2  # convert sheet row -> data_rows index

    if idx_in_data < 0 or idx_in_data >= len(data_rows):
        raise ValueError(f"Row {row_number} is out of range.")

    existing_row = data_rows[idx_in_data]
    existing_map = {headers[i]: (existing_row[i] if i < len(existing_row) else "") for i in range(len(headers))}

    updated_row = []
    for h in headers:
        if h in deal_dict:
            updated_row.append(deal_dict.get(h, ""))
        else:
            updated_row.append(existing_map.get(h, ""))

    end_col_letter = col_to_letter(len(headers))
    write_range = f"{sheet_name}!A{row_number}:{end_col_letter}{row_number}"

    sheet.values().update(
        spreadsheetId=sheet_id,
        range=write_range,
        valueInputOption="RAW",
        body={"values": [updated_row]}
    ).execute()


def append_deal_to_sheet(service, deal_dict, sheet_id, sheet_name="Sheet1"):
    sheet = service.spreadsheets()

    # 1) Read header row (Row 1)
    header_range = f"{sheet_name}!1:1"
    header_resp = sheet.values().get(
        spreadsheetId=sheet_id,
        range=header_range
    ).execute()

    headers = (header_resp.get("values") or [[]])[0]
    if not headers:
        raise ValueError("No headers found in row 1. Please set headers first.")

    # 2) Build row aligned to headers
    row = []
    for h in headers:
        row.append(deal_dict.get(h, ""))  # blank if field not found

    # 3) Append aligned row
    body = {"values": [row]}
    sheet.values().append(
        spreadsheetId=sheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()



def sheet_values_exist(sheet, sheet_id, worksheet_name):
    """
    Check if sheet already has values (to avoid re-writing header)
    """
    result = sheet.values().get(spreadsheetId=sheet_id, range=worksheet_name).execute()
    return bool(result.get("values"))

def read_unuploaded_rows(token_path, sheet_id, worksheet_name="Sheet1"):
    service = get_sheets_service(token_path)
    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=sheet_id, range=worksheet_name).execute()
    all_values = result.get("values", [])
    if not all_values:
        return []

    headers = all_values[0]
    uploaded_idx = headers.index("Uploaded") if "Uploaded" in headers else None

    rows = []
    for r in all_values[1:]:
        row_dict = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}

        uploaded_val = ""
        if uploaded_idx is not None and uploaded_idx < len(r):
            uploaded_val = str(r[uploaded_idx]).strip().upper()

        if uploaded_idx is None or uploaded_val != "YES":
            rows.append(row_dict)

    return rows


def mark_uploaded(token_path, sheet_id, deal_ids, worksheet_name="Sheet1"):
    service = get_sheets_service(token_path)
    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=sheet_id, range=worksheet_name).execute()
    all_values = result.get("values", [])
    if not all_values:
        return

    headers = all_values[0]

    if "Deal - ID" not in headers:
        raise ValueError("Column 'Deal - ID' not found in headers.")

    deal_id_col = headers.index("Deal - ID")

    if "Uploaded" in headers:
        uploaded_idx = headers.index("Uploaded")
    else:
        uploaded_idx = len(headers)
        headers.append("Uploaded")
        sheet.values().update(
            spreadsheetId=sheet_id,
            range=f"{worksheet_name}!A1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()

    # Normalize IDs to strings for comparison
    deal_ids_set = {str(x).strip() for x in deal_ids}

    updates = []
    for row in all_values[1:]:
        # pad the row so indexes are safe
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        if str(row[deal_id_col]).strip() in deal_ids_set:
            row[uploaded_idx] = "YES"

        updates.append(row)

    sheet.values().update(
        spreadsheetId=sheet_id,
        range=f"{worksheet_name}!A2",
        valueInputOption="RAW",
        body={"values": updates}
    ).execute()
