# google_integration.py
import pandas as pd
import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def init_google_service_service_account(service_account_json_path: str):
    creds = Credentials.from_service_account_file(
        service_account_json_path,
        scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

MAX_SHEETS_CELL_LEN = 50000
TRIM_COLS = {"Deal - Full Info", "Deal - Full Info (Raw)"}

def _trim_for_google_sheets(value, *, deal_id=None, col_name=None, log_fn=None, max_len: int = MAX_SHEETS_CELL_LEN) -> str:
    """
    Trim only if beyond max_len. If trimmed, log to Activity Log (if log_fn provided).
    """
    if value is None:
        s = ""
    elif isinstance(value, (dict, list)):
        s = json.dumps(value, ensure_ascii=False)
    else:
        s = str(value)

    s = s.replace("\r\n", "\n").replace("\r", "\n")

    original_len = len(s)
    if original_len > max_len:
        if callable(log_fn):
            log_fn(f"[WARN] Deal {deal_id} → trimmed column '{col_name}' from {original_len} to {max_len} chars (Sheets limit).")
        s = s[:max_len]

    return s

# ---------- CONFIG ----------

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

def update_deal_row_in_sheet(service, deal_dict, sheet_id, row_number, sheet_name="Sheet1", log_fn=None):
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

    deal_id = deal_dict.get("Deal - ID", "")

    updated_row = []
    for h in headers:
        if h in deal_dict:
            v = deal_dict.get(h, "")
            if h in TRIM_COLS:
                v = _trim_for_google_sheets(v, deal_id=deal_id, col_name=h, log_fn=log_fn)
            updated_row.append(v)
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


def append_deal_to_sheet(service, deal_dict, sheet_id, sheet_name="Sheet1", log_fn=None):
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

    # 2) Build row aligned to headers (TRIM long fields)
    deal_id = deal_dict.get("Deal - ID", "")

    row = []
    for h in headers:
        v = deal_dict.get(h, "")
        if h in TRIM_COLS:
            v = _trim_for_google_sheets(v, deal_id=deal_id, col_name=h, log_fn=log_fn)
        row.append(v)

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

def read_unuploaded_rows(service, sheet_id, worksheet_name):
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


def mark_uploaded(service, sheet_id, deal_ids, worksheet_name="Sheet1"):
    sheet = service.spreadsheets()

    # Read headers only
    header_result = sheet.values().get(
        spreadsheetId=sheet_id,
        range=f"{worksheet_name}!1:1"
    ).execute()

    headers = (header_result.get("values") or [[]])[0]
    if not headers:
        return

    if "Deal - ID" not in headers:
        raise ValueError("Column 'Deal - ID' not found in headers.")

    deal_id_col = headers.index("Deal - ID") + 1
    deal_id_col_letter = col_to_letter(deal_id_col)

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

    uploaded_col_letter = col_to_letter(uploaded_idx + 1)
    deal_ids_set = {str(x).strip() for x in deal_ids}

    # Read only Deal - ID column, not the full sheet
    id_result = sheet.values().get(
        spreadsheetId=sheet_id,
        range=f"{worksheet_name}!{deal_id_col_letter}2:{deal_id_col_letter}"
    ).execute()

    id_values = id_result.get("values", [])

    data = []
    for offset, row in enumerate(id_values, start=2):
        deal_id_value = row[0] if row else ""

        if str(deal_id_value).strip() in deal_ids_set:
            data.append({
                "range": f"{worksheet_name}!{uploaded_col_letter}{offset}",
                "values": [["YES"]]
            })

    if not data:
        return

    sheet.values().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "valueInputOption": "RAW",
            "data": data
        }
    ).execute()