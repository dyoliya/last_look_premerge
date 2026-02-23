import mysql.connector
import pandas as pd
import json


# Map: Google Sheet column name -> MySQL column name
SHEET_TO_DB = {
    "Deal - ID": "deal_id",
    "Deal - Creator": "deal_creator",
    "Deal - Deal created": "deal_created_at",
    "Deal - Owner": "deal_owner",
    "Deal - Pipeline": "deal_pipeline",
    "Deal - Stage": "deal_stage",
    "Deal - Title": "deal_title",
    "Deal - Deal Size Category": "deal_size_category",
    "Deal - Value": "deal_value",
    "Deal - Status": "deal_status",
    "Deal - Label": "deal_label",
    "Deal - County": "deal_county",
    "Deal - Deal Status": "deal_deal_status",
    "Deal - Deal Summary": "deal_deal_summary",
    "Deal - Inbound Medium": "deal_inbound_medium",
    "Deal - Marketing Medium": "deal_marketing_medium",
    "Person - Name": "person_name",
    "Person - Mailing Address": "person_mailing_address",
    "Person - Phones": "person_phones",
    "Person - Emails": "person_emails",
    "Person - Archive - Phones": "person_archive_phones",
    "Person - Archive - Emails": "person_archive_emails",
    "Deal - Offer Generated Date": "deal_offer_generated_date",
    "Deal - Preferred Communication Method": "deal_preferred_communication_method",
    "Deal - Unique Database ID": "deal_unique_database_id",
    "Deal - Serial Number": "deal_serial_number",
    "Deal - BU Database ID": "deal_bu_database_id",
    "Deal - Contact Group ID": "deal_contact_group_id",
    "Deal - STOP Marketing": "deal_stop_marketing",
    "Deal - Email messages count": "deal_email_messages_count",
    "Deal - Total activities": "deal_total_activities",
    "Deal - Done activities": "deal_done_activities",
    "Deal - Activities to do": "deal_activities_to_do",
    "Merged with Deal ID": "merged_with_deal_id",
    "Snapshot Date": "snapshot_date",
    "Deal - Full Info": "deal_full_info",
    "Deal - Full Info (Raw)": "deal_full_info_raw"
}


REQUIRED_DB_COLS = {"deal_id", "merged_with_deal_id", "snapshot_date"}


def _to_int(val):
    if pd.isna(val) or val == "":
        return None
    try:
        return int(float(val))
    except Exception:
        return None


def _to_decimal(val):
    if pd.isna(val) or val == "":
        return None
    try:
        return float(val)
    except Exception:
        return None


def _to_datetime(val):
    if pd.isna(val) or val == "":
        return None
    # pandas handles lots of date formats
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()

def insert_df_to_mysql(df: pd.DataFrame, mysql_config: dict, table_name: str):
    """
    Insert DataFrame rows into an existing MySQL table with a fixed schema.
    This avoids CSV round-trip parsing issues and is safer for JSON-heavy columns.
    """
    if df is None or df.empty:
        return

    # Never allow upload_date to be inserted from source
    if "upload_date" in df.columns:
        df = df.drop(columns=["upload_date"])

    # Rename columns to DB column names (drop anything unmapped)
    mapped_cols = {c: SHEET_TO_DB[c] for c in df.columns if c in SHEET_TO_DB}
    df = df.rename(columns=mapped_cols)

    # Keep only mapped DB columns (in DB column names)
    df = df[list(mapped_cols.values())]

    # Ensure required columns exist
    missing_required = [c for c in REQUIRED_DB_COLS if c not in df.columns]
    if missing_required:
        raise ValueError(
            f"Missing required columns needed for MySQL insert: {missing_required}\n"
            f"Make sure these exist in Google Sheet and are included in the export."
        )

    # ---- Type conversions (same as your CSV version) ----
    if "deal_id" in df.columns:
        df["deal_id"] = df["deal_id"].apply(_to_int)

    if "merged_with_deal_id" in df.columns:
        df["merged_with_deal_id"] = df["merged_with_deal_id"].apply(_to_int)

    if "deal_value" in df.columns:
        df["deal_value"] = df["deal_value"].apply(_to_decimal)

    if "deal_email_messages_count" in df.columns:
        df["deal_email_messages_count"] = df["deal_email_messages_count"].apply(_to_int)

    for c in ["deal_total_activities", "deal_done_activities", "deal_activities_to_do"]:
        if c in df.columns:
            df[c] = df[c].apply(_to_int)

    for c in ["deal_created_at", "deal_offer_generated_date", "snapshot_date"]:
        if c in df.columns:
            df[c] = df[c].apply(_to_datetime)

    # ---- JSON safety: validate deal_full_info if column exists ----
    if "deal_full_info" in df.columns:
        def _validate_json_literal(val):
            if pd.isna(val) or val == "":
                return None

            if isinstance(val, (dict, list)):
                # convert dict → string once (no formatting change beyond default)
                return json.dumps(val, ensure_ascii=False)

            s = str(val)

            # Validate only — do NOT reformat
            try:
                json.loads(s)
                return s  # return EXACT original string
            except Exception:
                return None  # invalid JSON → NULL

        df["deal_full_info"] = df["deal_full_info"].apply(_validate_json_literal)

    # Final required NOT NULL safety check
    for c in REQUIRED_DB_COLS:
        if df[c].isna().any():
            bad = df[df[c].isna()]
            raise ValueError(f"Some rows have NULL for required column '{c}'. Example row(s):\n{bad.head(3)}")

    conn = mysql.connector.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["db"],
    )
    cursor = conn.cursor()

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join([f"`{c}`" for c in cols])

    sql = f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({placeholders})"

    data = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()