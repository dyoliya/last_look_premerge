# pipedrive_integration.py
import json
import os
import re
import requests
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

BASE_URL = "https://api.pipedrive.com/v1"

# -------------------------
# Config / flags
# -------------------------
DEBUG_SAVE_RAW_DEAL = False
DEBUG_PRINT_SELECTED = False

# Path to your attached reference file (FieldKey/FieldName/OptionID/OptionLabel)
FIELDS_XLSX_PATH = os.path.join("config", "List of Deal Fields.xlsx")


# -------------------------
# HTTP + shared utilities
# -------------------------
def get_headers() -> Dict[str, str]:
    return {"Accept": "application/json"}


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None

# -------------------------
# Debug helpers
# -------------------------
def save_raw_deal_to_file(deal_data: dict, deal_id: int) -> str:
    os.makedirs("debug_output", exist_ok=True)
    file_path = f"debug_output/deal_{deal_id}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(deal_data, f, indent=2, ensure_ascii=False)
    return file_path

def save_raw_person_to_file(person_data: dict, person_id: int) -> str:
    os.makedirs("debug_output", exist_ok=True)
    file_path = f"debug_output/person_{person_id}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(person_data, f, indent=2, ensure_ascii=False)
    return file_path

# -------------------------
# Reference loader (YOUR EXCEL)
# -------------------------
@lru_cache(maxsize=1)
def load_fields_reference_from_xlsx(xlsx_path: str) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Returns:
      - name_map:    FieldKey(str) -> FieldName(str)
      - options_map: FieldKey(str) -> {OptionID(str): OptionLabel(str)}
      - type_map:    FieldKey(str) -> Type(str)   (from the sheet)
    """
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(
            f"Missing fields reference Excel: {xlsx_path}\n"
            f"Place your 'List of Deal Fields.xlsx' in config/ or update FIELDS_XLSX_PATH."
        )

    df = pd.read_excel(xlsx_path, sheet_name="List of Fields")

    required = {"FieldName", "FieldKey", "Type", "OptionLabel", "OptionID"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Excel is missing required columns: {sorted(missing)}")

    name_map: Dict[str, str] = {}
    type_map: Dict[str, str] = {}
    options_map: Dict[str, Dict[str, str]] = {}

    # Normalize to strings for safe dict keys
    for _, row in df.iterrows():
        field_key = row.get("FieldKey")
        field_name = row.get("FieldName")
        field_type = row.get("Type")

        if pd.isna(field_key) or str(field_key).strip() == "":
            continue

        k = str(field_key).strip()
        if not pd.isna(field_name) and str(field_name).strip():
            name_map[k] = str(field_name).strip()

        if not pd.isna(field_type) and str(field_type).strip():
            type_map[k] = str(field_type).strip()

        opt_id = row.get("OptionID")
        opt_label = row.get("OptionLabel")

        if not pd.isna(opt_id) and not pd.isna(opt_label):
            options_map.setdefault(k, {})[str(opt_id).strip()] = str(opt_label).strip()

    return name_map, options_map, type_map


# -------------------------
# Deal fields metadata (API) + merging with Excel
# -------------------------
@lru_cache(maxsize=1)
def get_deal_fields_meta(api_token: str) -> Dict[str, dict]:
    """
    Pulls DealFields from Pipedrive and merges/overrides with your Excel
    so:
      - every field has a human FieldName
      - enum/set fields translate OptionID -> OptionLabel
    """
    # 1) From API
    url = f"{BASE_URL}/dealFields"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    fields = r.json().get("data", []) or []

    api_meta: Dict[str, dict] = {}
    for f in fields:
        key = f.get("key")
        if not key:
            continue

        options_map: Dict[str, str] = {}
        for opt in (f.get("options") or []):
            oid = opt.get("id")
            label = opt.get("label")
            if oid is not None and label is not None:
                options_map[str(oid)] = str(label)

        api_meta[str(key)] = {
            "name": f.get("name", str(key)),
            "field_type": f.get("field_type", ""),
            "options_map": options_map,
        }

    # 2) From YOUR Excel (source of truth for naming + option labels)
    excel_name_map, excel_options_map, excel_type_map = load_fields_reference_from_xlsx(FIELDS_XLSX_PATH)

    # 3) Merge: ensure all excel keys exist, override names/options when present
    merged: Dict[str, dict] = dict(api_meta)

    for k, human_name in excel_name_map.items():
        merged.setdefault(k, {"name": k, "field_type": excel_type_map.get(k, ""), "options_map": {}})
        merged[k]["name"] = human_name  # Excel wins

    for k, opt_map in excel_options_map.items():
        merged.setdefault(k, {"name": excel_name_map.get(k, k), "field_type": excel_type_map.get(k, ""), "options_map": {}})
        merged[k].setdefault("options_map", {})
        # Excel wins on option labels too
        merged[k]["options_map"].update(opt_map)

    # If Excel includes Type, use it as field_type fallback (helps normalization)
    for k, t in excel_type_map.items():
        merged.setdefault(k, {"name": excel_name_map.get(k, k), "field_type": t, "options_map": excel_options_map.get(k, {})})
        if not merged[k].get("field_type"):
            merged[k]["field_type"] = t

    return merged

@lru_cache(maxsize=1)
def get_person_fields_meta(api_token: str) -> Dict[str, dict]:
    url = f"{BASE_URL}/personFields"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    fields = r.json().get("data", []) or []

    meta: Dict[str, dict] = {}
    for f in fields:
        key = f.get("key")
        if not key:
            continue

        options_map: Dict[str, str] = {}
        for opt in (f.get("options") or []):
            oid = opt.get("id")
            label = opt.get("label")
            if oid is not None and label is not None:
                options_map[str(oid)] = str(label)

        meta[str(key)] = {
            "name": f.get("name", str(key)),
            "field_type": f.get("field_type", ""),
            "options_map": options_map,
        }
    return meta

def translate_custom_field_value(raw_value: Any, field_key: str, fields_meta: Dict[str, dict]) -> str:
    """
    Translate option IDs into labels when options_map exists.
    Works for:
      - single option stored as "493"
      - multi options stored as "7889,7718"
      - lists
    """
    if raw_value is None:
        return ""

    field_meta = fields_meta.get(field_key) or {}
    options_map = field_meta.get("options_map") or {}

    if not options_map:
        # no translation available
        if raw_value == "":
            return ""
        return str(raw_value)

    # Normalize tokens
    if isinstance(raw_value, list):
        tokens = [str(x).strip() for x in raw_value if str(x).strip()]
    else:
        s = str(raw_value).strip()
        if not s:
            return ""
        tokens = [t.strip() for t in s.split(",") if t.strip()]

    translated = [options_map.get(t, t) for t in tokens]
    return ", ".join(translated)

def extract_person_numbered_fields(full_person: dict, person_fields_meta: Dict[str, dict]) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Reverse lookup: FieldName -> FieldKey
    name_to_key = {v.get("name", ""): k for k, v in person_fields_meta.items()}

    def get_by_name(field_name: str) -> str:
        key = name_to_key.get(field_name)
        if not key:
            return ""
        raw = full_person.get(key)
        return normalize_value(raw, person_fields_meta[key].get("field_type", ""))

    # Phones 1..10 + their Data Source + Opt Out
    for i in range(1, 11):
        out[f"Person - Phone - {i}"] = get_by_name(f"Phone {i}")
        out[f"Person - Phone - {i} - Data Source"] = get_by_name(f"Phone {i} - Data Source")

        # Note: you have one weird label: "Phone 8 -  Opt Out" (double space)
        opt_out_name = f"Phone {i} - Opt Out"
        if i == 8 and "Phone 8 -  Opt Out" in name_to_key and opt_out_name not in name_to_key:
            opt_out_name = "Phone 8 -  Opt Out"

        out[f"Person - Phone - {i} - Opt Out"] = get_by_name(opt_out_name)

    # Emails 1..17 + their Data Source + Opt Out
    for i in range(1, 18):
        out[f"Person - Email - {i}"] = get_by_name(f"Email {i}")
        out[f"Person - Email - {i} - Data Source"] = get_by_name(f"Email {i} - Data Source")
        out[f"Person - Email - {i} - Opt Out"] = get_by_name(f"Email {i} - Opt Out")

    return out


# -------------------------
# Pipeline / Stage mapping
# -------------------------
@lru_cache(maxsize=1)
def get_pipelines(api_token: str) -> Dict[int, str]:
    url = f"{BASE_URL}/pipelines"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    pipelines = r.json().get("data", []) or []
    return {int(p["id"]): str(p["name"]) for p in pipelines if p.get("id") is not None and p.get("name") is not None}


@lru_cache(maxsize=1)
def get_stages(api_token: str) -> Dict[int, str]:
    url = f"{BASE_URL}/stages"
    params = {"api_token": api_token, "start": 0, "limit": 500}
    out: Dict[int, str] = {}

    while True:
        r = requests.get(url, params=params, headers=get_headers())
        r.raise_for_status()
        payload = r.json() or {}
        stages = payload.get("data") or []

        for s in stages:
            sid = s.get("id")
            name = s.get("name")
            if sid is not None and name is not None:
                out[int(sid)] = str(name)

        pagination = (payload.get("additional_data") or {}).get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        params["start"] = int(pagination.get("next_start") or 0)

    return out


# -------------------------
# Person/contact extraction
# -------------------------
PERSON_MAILING_ADDRESS_FMT_KEY = "81236c1018abb6b48bf16c2c44efdce57b59c010"
ARCHIVE_PERSON_PHONE_KEY = "7cc2d51bbbd082af46ecdc71590642bac71ce58f"
ARCHIVE_PERSON_EMAIL_KEY = "de1f8783418ce27a892042d82843f428b784845a"
ARCHIVE_PERSON_ADDRESS_KEY = "4720c93f987bec9a27edefca95de1dfd18add279"
OTHER_INFO_COL = "Deal - Full Info"
OTHER_INFO_RAW_COL = "Deal - Full Info (Raw)"



def _join_labeled(items: List[dict], sep: str = " | ") -> str:
    out = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        label = (it.get("label") or "").strip()
        value = (it.get("value") or "").strip()
        if not value:
            continue
        out.append(f"{label}:{value}" if label else value)
    return sep.join(out)


@lru_cache(maxsize=512)
def get_person(person_id: int, api_token: str) -> dict:
    url = f"{BASE_URL}/persons/{person_id}"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    person = r.json().get("data", {}) or {}

    if DEBUG_SAVE_RAW_DEAL:
        path = save_raw_person_to_file(person, person_id)
        print(f"[DEBUG] Raw person JSON saved to {path}")

    return person

def clean_archive_text(x: Any) -> str:
    """
    Pipedrive archive fields sometimes start with ', ' or whitespace.
    This removes leading commas + spaces/tabs/newlines.
    Example: ', 123 | 456' -> '123 | 456'
    """
    if x is None:
        return ""
    s = str(x)

    # remove leading whitespace first
    s = s.lstrip()

    # then remove leading commas/spaces repeatedly
    # (handles cases like ", ,  value" or ",value" or ",   value")
    s = re.sub(r"^[,\s]+", "", s)

    return s

def _extract_person_contact(person_from_deal: dict, full_person: dict) -> Dict[str, str]:
    emails = person_from_deal.get("email") or full_person.get("email") or []
    phones = person_from_deal.get("phone") or full_person.get("phone") or []

    address = (
        full_person.get(PERSON_MAILING_ADDRESS_FMT_KEY)
        or full_person.get("postal_address_formatted_address")
        or person_from_deal.get("address")
        or ""
    )
    archive_phones = clean_archive_text(full_person.get(ARCHIVE_PERSON_PHONE_KEY))
    archive_emails = clean_archive_text(full_person.get(ARCHIVE_PERSON_EMAIL_KEY))
    archive_address = clean_archive_text(full_person.get(ARCHIVE_PERSON_ADDRESS_KEY))


    return {
        "Person - Name": person_from_deal.get("name") or full_person.get("name") or "",
        "Person - Emails": _join_labeled(emails),
        "Person - Phones": _join_labeled(phones),
        "Person - Mailing Address": address,
        "Person - Archive - Phones": archive_phones,
        "Person - Archive - Emails": archive_emails,
        "Person - Archive - Address": archive_address
    }


# -------------------------
# Normalization helpers (make dict-ish fields readable)
# -------------------------
def normalize_value(raw: Any, field_type: str = "") -> str:
    if raw is None:
        return ""

    # common pipedrive patterns: {"value":123,"name":"X"}, {"id":123,"name":"X"}
    if isinstance(raw, dict):
        if field_type in {"user", "org", "person"}:
            return str(raw.get("name") or raw.get("value") or raw.get("id") or "")
        # fallback for arbitrary dicts
        return json.dumps(raw, ensure_ascii=False)

    if isinstance(raw, list):
        # list of dicts -> try label/value
        if raw and all(isinstance(x, dict) for x in raw):
            return _join_labeled(raw)
        return ", ".join([str(x) for x in raw if str(x).strip()])

    return str(raw)

def build_deal_mapped_payload(data: dict, fields_meta: Dict[str, dict]) -> dict:
    """
    Returns a copy of the deal payload where:
      - custom field keys are replaced with their FieldName (when known)
      - option/set fields are translated to labels when options_map exists
      - unknown keys remain as-is
    """
    mapped: Dict[str, Any] = {}

    for k, raw in (data or {}).items():
        ks = str(k)

        meta = fields_meta.get(ks)
        if meta:
            # Use the same human field name you use for sheet columns
            field_name = meta.get("name", ks)
            out_key = f"Deal - {field_name}"

            translated = translate_custom_field_value(raw, ks, fields_meta)
            if translated != "" and (meta.get("options_map") or {}):
                mapped[out_key] = translated
            else:
                mapped[out_key] = normalize_value(raw, meta.get("field_type", ""))
        else:
            # Not in fields_meta (system keys like id, stage_id, etc.) -> keep readable
            mapped[ks] = raw

    return mapped

def _norm_phone(s: str) -> str:
    if not s:
        return ""
    digits = re.sub(r"\D+", "", str(s))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) in (10, 11) else digits  # keep if your data includes 11 non-US

def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out

# -------------------------
# Deal retrieval (ALL FIELDS)
# -------------------------
def get_deal(deal_id: int, api_token: str) -> Dict[str, Any]:
    """
    Returns ALL fields listed in your Excel (FieldKey),
    with human-readable names + translated options when possible,
    plus friendly Pipeline/Stage names and extracted Person contact details.

    Note: Deal 'label' values are returned as comma-separated IDs as strings. :contentReference[oaicite:1]{index=1}
    We translate them using the options for the 'label' field from DealFields metadata.
    """
    url = f"{BASE_URL}/deals/{deal_id}"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    data = r.json().get("data", {}) or {}
    if not data:
        return {}

    if DEBUG_SAVE_RAW_DEAL:
        path = save_raw_deal_to_file(data, deal_id)
        print(f"[DEBUG] Raw deal JSON saved to {path}")

    fields_meta = get_deal_fields_meta(api_token)

    unmapped_customish = [k for k in data.keys() if str(k) not in fields_meta and len(str(k)) >= 20]
    print("[DEBUG] Unmapped keys count:", len(unmapped_customish))
    print("[DEBUG] Sample unmapped keys:", unmapped_customish[:20])

    stages_map = get_stages(api_token)
    pipelines_map = get_pipelines(api_token)

    # Person block (deal returns person_id as dict)
    person_from_deal = data.get("person_id")
    person_from_deal = person_from_deal if isinstance(person_from_deal, dict) else {}

    person_id = person_from_deal.get("value") or person_from_deal.get("id")
    person_id_int = _safe_int(person_id)

    full_person = {}
    if person_id_int:
        full_person = get_person(person_id_int, api_token)

    person_fields_meta = get_person_fields_meta(api_token)
    person_bits = _extract_person_contact(person_from_deal, full_person)
    person_bits.update(extract_person_numbered_fields(full_person, person_fields_meta))

    # 1) collect all phones from base + numbered fields
    phones = []
    # from the existing "Person - Phones" (already joined labeled)
    base_phones = person_bits.get("Person - Phones", "").strip()
    if base_phones:
        phones += re.split(r"\s*\|\s*", base_phones)

    # from numbered
    for i in range(1, 11):
        v = person_bits.get(f"Person - Phone - {i}", "")
        if v:
            phones.append(v)

    # normalize + dedupe (you can choose normalized vs original display)
    phones_norm = []
    for p in phones:
        n = _norm_phone(p)
        phones_norm.append(n if n else p.strip())

    phones_norm = _dedupe_keep_order(phones_norm)
    person_bits["Person - Phones"] = " | ".join(phones_norm)

    # 2) collect all emails from base + numbered fields
    emails = []
    base_emails = person_bits.get("Person - Emails", "").strip()
    if base_emails:
        emails += re.split(r"\s*\|\s*", base_emails)

    for i in range(1, 18):
        v = person_bits.get(f"Person - Email - {i}", "")
        if v:
            emails.append(v.strip())

    # clean + dedupe emails (case-insensitive)
    emails_clean = _dedupe_keep_order([e.strip() for e in emails if e.strip()])
    person_bits["Person - Emails"] = " | ".join(emails_clean)

    # 3) optional: remove numbered fields so they don't clutter your output/json
    for i in range(1, 11):
        person_bits.pop(f"Person - Phone - {i}", None)
        person_bits.pop(f"Person - Phone - {i} - Data Source", None)
        person_bits.pop(f"Person - Phone - {i} - Opt Out", None)

    for i in range(1, 18):
        person_bits.pop(f"Person - Email - {i}", None)
        person_bits.pop(f"Person - Email - {i} - Data Source", None)
        person_bits.pop(f"Person - Email - {i} - Opt Out", None)


    # pipeline / stage readable
    stage_id_raw = data.get("stage_id")
    pipeline_id_raw = data.get("pipeline_id")
    stage_id = _safe_int(stage_id_raw)
    pipeline_id = _safe_int(pipeline_id_raw)

    stage_name = stages_map.get(stage_id, str(stage_id_raw) if stage_id_raw is not None else "")
    pipeline_name = pipelines_map.get(pipeline_id, str(pipeline_id_raw) if pipeline_id_raw is not None else "")

    # Build output:
    # 1) Always include these friendly computed fields
    out: Dict[str, Any] = {
        "Deal - ID": data.get("id"),
        "Deal - Pipeline": pipeline_name,
        "Deal - Stage": stage_name,
        **person_bits,
    }

    # 2) Add every field from Excel (via fields_meta keys)
    # Use FieldName from meta and translate options where possible
    # Store the FULL raw payload (deal + person) for preservation
    # This is what will go into MySQL LONGTEXT column deal_other_raw_info
    deal_mapped = build_deal_mapped_payload(data, fields_meta)

    # Readable payload (NO fieldkeys) -> safe for Google Sheet AND MySQL readable column
    out[OTHER_INFO_COL] = json.dumps(
        {
            "deal": deal_mapped,
            "person": person_bits
        },
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":")
    )

    # Raw payload (fieldkeys) -> store in MySQL only
    out[OTHER_INFO_RAW_COL] = json.dumps(
        {
            "deal_raw": data,
            "person_raw": full_person
        },
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":")
    )

    # Only keep a limited set of important fields as individual columns
    IMPORTANT_DEAL_FIELDS = {
        "Deal - Creator",
        "Deal - Deal created",
        "Deal - Owner",
        "Deal - Title",
        "Deal - Deal Size Category",
        "Deal - Value",
        "Deal - Status",
        "Deal - Label",
        "Deal - County",
        "Deal - Deal Status",
        "Deal - Deal Summary",
        "Deal - Inbound Medium",
        "Deal - Marketing Medium",
        "Deal - Offer Generated Date",
        "Deal - Preferred Communication Method",
        "Deal - Unique Database ID",
        "Deal - Serial Number",
        "Deal - BU Database ID",
        "Deal - Contact Group ID",
        "Deal - STOP Marketing",
        "Deal - Email messages count",
        "Deal - Total activities",
        "Deal - Done activities",
        "Deal - Activities to do",
    }

    # Add only the important fields from Excel/API meta
    for field_key, meta in fields_meta.items():
        field_name = meta.get("name", field_key)
        field_type = meta.get("field_type", "")

        out_key = f"Deal - {field_name}"
        if out_key not in IMPORTANT_DEAL_FIELDS:
            continue

        raw = data.get(field_key)

        translated = translate_custom_field_value(raw, field_key, fields_meta)
        if translated != "" and (meta.get("options_map") or {}):
            new_val = translated
        else:
            new_val = normalize_value(raw, field_type)

        # Protect existing non-empty values from being overwritten by blanks
        if out_key in out and str(out.get(out_key) or "").strip() and not str(new_val or "").strip():
            continue

        out[out_key] = new_val

    if DEBUG_PRINT_SELECTED:
        print("\n[DEBUG] DEAL OUTPUT (ALL FIELDS)")
        for k, v in out.items():
            print(f"  {k}: {v!r}")
        print("[DEBUG] END DEAL OUTPUT\n")

    return out