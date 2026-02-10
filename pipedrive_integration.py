# pipedrive_integration.py
import json
import os
import requests
from functools import lru_cache
from typing import Any, Dict, List, Optional

BASE_URL = "https://api.pipedrive.com/v1"


# -------------------------
# Config / flags
# -------------------------
DEBUG_SAVE_RAW_DEAL = False      # writes debug_output/deal_<id>.json
DEBUG_PRINT_SELECTED = False    # prints a compact summary of the selected fields

# Your required custom field keys (from your debug output)
CF_DEAL_SIZE_CATEGORY = "1a14f5e5c2e242b7a728c4df967221806dcbe6ee"
CF_OFFER_GENERATED_DATE = "4bfee3eb0f1ad6bce92e17085868ad849d4d8ae5"
CF_PREF_COMM_METHOD = "d462aa4246f640664cacfcb458bb6c071cc2a534"
CF_COUNTY = "4b2ded61764c4316a493b6f58134620c136819d5"
CF_INBOUND_MEDIUM = "022c6b0d32fdf8ef896cb7ca81b71158f277255e"
CF_UNIQUE_DATABASE_ID = "cf55ab58ba9377b340fe91a7886591cac6cafabd"
CF_SERIAL_NUMBER = "7abd4430ed77a16a77eefc92fb02acab71f231ec"
CF_DEAL_STATUS = "a8b479cb304320c246021ded79cb84243dd67b6f"
CF_DEAL_SUMMARY = "bdfdc9dd9211808904695212df02131493425272"
CF_BU_DATABASE_ID = "db8c9e444c82a5460fb846b4be8b3eb6b0b5a7bc"
CF_CONTACT_GROUP_ID = "5d99400ee3e45b14b9966140c6b14cbaac803888"
CF_STOP_MARKETING = "af26aa38da97a8b00272a17e1c556847318bc6f7"
CF_MARKETING_MEDIUM = "9dbd9123f73f5919067b360f992b130451051751"
PERSON_MAILING_ADDRESS_FMT_KEY = "81236c1018abb6b48bf16c2c44efdce57b59c010"


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

def debug_find_address_fields(obj: dict, label: str):
    if not isinstance(obj, dict):
        print(f"[DEBUG] {label}: not a dict")
        return

    hits = []
    for k, v in obj.items():
        if "address" in str(k).lower():
            hits.append((k, v))

    print(f"[DEBUG] {label}: address-like keys found = {len(hits)}")
    for k, v in hits[:15]:
        print(f"  - {k}: {v!r}")

# -------------------------
# Person/contact extraction
# -------------------------
def _join_labeled(items: List[dict], sep: str = " | ") -> str:
    """
    Turns [{'label':'work','value':'x'}, {'label':'home','value':'y'}]
    into 'work:x | home:y'
    """
    out = []
    for it in items or []:
        label = (it.get("label") or "").strip()
        value = (it.get("value") or "").strip()
        if not value:
            continue
        # If label is missing, just store the value
        out.append(f"{label}:{value}" if label else value)
    return sep.join(out)

def _extract_person_contact(person_from_deal: dict, full_person: dict) -> Dict[str, str]:
    """
    Single column for email + phone.
    Mailing Address comes from the full person endpoint if missing in embedded deal person.
    """
    emails = person_from_deal.get("email") or full_person.get("email") or []
    phones = person_from_deal.get("phone") or full_person.get("phone") or []

    address = (
        full_person.get(PERSON_MAILING_ADDRESS_FMT_KEY)
        or full_person.get("postal_address_formatted_address")
        or person_from_deal.get("address")
        or ""
    )

    return {
        "Deal - Contact person": person_from_deal.get("name") or full_person.get("name") or "",
        "Person - Emails": _join_labeled(emails),
        "Person - Phones": _join_labeled(phones),
        "Person - Mailing Address": address,
    }


# -------------------------
# Deal custom fields (metadata + value translation)
# -------------------------
@lru_cache(maxsize=1)
def get_deal_fields_meta(api_token: str) -> Dict[str, dict]:
    """
    Map custom-field key => {name, field_type, options_map}
    options_map maps option id -> label for enum/set fields.
    """
    url = f"{BASE_URL}/dealFields"
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
                options_map[str(oid)] = label

        meta[key] = {
            "name": f.get("name", key),
            "field_type": f.get("field_type", ""),
            "options_map": options_map,
        }

    return meta

def translate_custom_field_value(raw_value: Any, field_key: str, fields_meta: Dict[str, dict]) -> str:
    """
    Translate enum-like stored values into readable labels using dealFields metadata.
    Handles:
      - single option stored as "493"
      - multi options stored as "7889,7718"
      - lists
    """
    if raw_value is None:
        return ""

    field_meta = fields_meta.get(field_key) or {}
    options_map = field_meta.get("options_map") or {}

    # If no options map, return raw as string (preserve commas etc.)
    if not options_map:
        return str(raw_value) if raw_value != "" else ""

    # Normalize to list of tokens
    if isinstance(raw_value, list):
        tokens = [str(x).strip() for x in raw_value if str(x).strip()]
    else:
        s = str(raw_value).strip()
        if not s:
            return ""
        tokens = [t.strip() for t in s.split(",") if t.strip()]

    translated = [options_map.get(t, t) for t in tokens]
    return ", ".join(translated)


# -------------------------
# Pipedrive mapping helpers
# -------------------------
@lru_cache(maxsize=1)
def get_pipelines(api_token: str) -> Dict[int, str]:
    url = f"{BASE_URL}/pipelines"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    pipelines = r.json().get("data", []) or []
    return {int(p["id"]): p["name"] for p in pipelines if p.get("id") is not None}


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

        additional = (payload.get("additional_data") or {}).get("pagination") or {}
        more = additional.get("more_items_in_collection")
        if not more:
            break

        params["start"] = int(additional.get("next_start") or 0)

    return out

def _get_owner_name(data: dict) -> str:
    """
    Raw deal shows owner info can be in:
      - data["user_id"]["name"]
      - data["owner_name"]
    Prefer user_id.name.
    """
    user_obj = data.get("user_id")
    if isinstance(user_obj, dict) and user_obj.get("name"):
        return str(user_obj.get("name"))
    if data.get("owner_name"):
        return str(data.get("owner_name"))
    return ""

def _get_creator_name(data: dict) -> str:
    creator_obj = data.get("creator_user_id")
    if isinstance(creator_obj, dict) and creator_obj.get("name"):
        return str(creator_obj.get("name"))
    return ""

@lru_cache(maxsize=512)
def get_person(person_id: int, api_token: str) -> dict:
    url = f"{BASE_URL}/persons/{person_id}"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    person = r.json().get("data", {}) or {}

    if DEBUG_SAVE_RAW_DEAL:
        # DEBUG: save full person payload
        path = save_raw_person_to_file(person, person_id)
        print(f"[DEBUG] Raw person JSON saved to {path}")

    return person

def extract_deal_labels_text(data: dict, fields_meta: dict) -> str:
    # raw is like: "1233,976,1192,10781"
    raw = data.get("label")

    # deal labels are stored under the built-in deal field key "label"
    translated = translate_custom_field_value(raw, "label", fields_meta)  # -> "Hot, Warm, 1234, Something"

    # keep only non-numeric label texts
    labels = []
    for t in (translated or "").split(","):
        t = t.strip()
        if not t:
            continue
        if t.isdigit():          # remove labels like "1234"
            continue
        labels.append(t)

    return " | ".join(labels)

# -------------------------
# Deal retrieval (FINAL)
# -------------------------
def get_deal(deal_id: int, api_token: str) -> Dict[str, Any]:
    """
    Returns a dictionary with the exact fields you listed, translated into readable values.
    - Stage name is resolved via /stages mapping
    - Custom fields are translated via /dealFields options (where applicable)
    - Person fields are extracted from person_id dict
    """
    url = f"{BASE_URL}/deals/{deal_id}"
    r = requests.get(url, params={"api_token": api_token}, headers=get_headers())
    r.raise_for_status()
    data = r.json().get("data", {}) or {}
    if not data:
        return {}

    if DEBUG_SAVE_RAW_DEAL:
        path = save_raw_deal_to_file(data, deal_id)
        # keep this short (no huge prints)
        print(f"[DEBUG] Raw deal JSON saved to {path}")

    # cached maps/meta
    stages_map = get_stages(api_token)
    pipelines_map = get_pipelines(api_token)
    fields_meta = get_deal_fields_meta(api_token)

    stage_id_raw = data.get("stage_id")
    pipeline_id_raw = data.get("pipeline_id")

    stage_id = _safe_int(stage_id_raw)
    pipeline_id = _safe_int(pipeline_id_raw)

    # Try both int and str keys just in case
    stage_name = (
        stages_map.get(stage_id)
        or stages_map.get(str(stage_id_raw))  # defensive
        or (str(stage_id_raw) if stage_id_raw is not None else "")
    )

    pipeline_name = (
        pipelines_map.get(pipeline_id)
        or pipelines_map.get(str(pipeline_id_raw))  # defensive
        or (str(pipeline_id_raw) if pipeline_id_raw is not None else "")
    )

    deal_labels = extract_deal_labels_text(data, fields_meta)

    # Person block (your raw JSON uses person_id as dict)
    person_from_deal = data.get("person_id")
    person_from_deal = person_from_deal if isinstance(person_from_deal, dict) else {}

    # person id is commonly in person_from_deal["value"]
    person_id = person_from_deal.get("value") or person_from_deal.get("id")
    person_id_int = _safe_int(person_id)

    full_person = {}
    if person_id_int:
        full_person = get_person(person_id_int, api_token)

    person_bits = _extract_person_contact(person_from_deal, full_person)

    if DEBUG_PRINT_SELECTED:
        print(f"[DEBUG] person_id_raw from deal person_id.value: {person_id!r}")
        print(f"[DEBUG] full_person keys count: {len(full_person) if isinstance(full_person, dict) else 0}")
        debug_find_address_fields(full_person, "FULL PERSON")

    # Build final dict (only what you said you need)
    deal_dict: Dict[str, Any] = {
        # core
        "Deal - ID": data.get("id"),
        "Deal - Creator": _get_creator_name(data),
        "Deal - Owner": _get_owner_name(data),

        # pipeline/stage
        "Deal - Pipeline": pipeline_name,
        "Deal - Stage": stage_name,

        # title/value/time/status/counts
        "Deal - Title": data.get("title") or "",
        "Deal - Value": data.get("value") or "",
        "Deal - Deal created": data.get("add_time") or "",
        "Deal - Status": (str(data.get("status")).title() if data.get("status") else ""),
        "Deal - Email messages count": data.get("email_messages_count") or 0,
        "Deal - Total activities": data.get("activities_count") or 0,
        "Deal - Done activities": data.get("done_activities_count") or 0,
        "Deal - Activities to do": data.get("undone_activities_count") or 0,

        # person info (label + value)
        **person_bits,

        # custom fields you listed (translated when possible)
        "Deal - Size Category": translate_custom_field_value(data.get(CF_DEAL_SIZE_CATEGORY), CF_DEAL_SIZE_CATEGORY, fields_meta),
        "Deal - Offer Generated Date": translate_custom_field_value(data.get(CF_OFFER_GENERATED_DATE), CF_OFFER_GENERATED_DATE, fields_meta),
        "Deal - Preferred Communication Method": translate_custom_field_value(data.get(CF_PREF_COMM_METHOD), CF_PREF_COMM_METHOD, fields_meta),
        "Deal - County": translate_custom_field_value(data.get(CF_COUNTY), CF_COUNTY, fields_meta),
        "Deal - Inbound Medium": translate_custom_field_value(data.get(CF_INBOUND_MEDIUM), CF_INBOUND_MEDIUM, fields_meta),
        "Deal - Marketing Medium": translate_custom_field_value(data.get(CF_MARKETING_MEDIUM), CF_MARKETING_MEDIUM, fields_meta),
        "Deal - Unique Database ID": translate_custom_field_value(data.get(CF_UNIQUE_DATABASE_ID), CF_UNIQUE_DATABASE_ID, fields_meta),
        "Deal - Serial Number": translate_custom_field_value(data.get(CF_SERIAL_NUMBER), CF_SERIAL_NUMBER, fields_meta),
        "Deal - Deal Status": translate_custom_field_value(data.get(CF_DEAL_STATUS), CF_DEAL_STATUS, fields_meta),
        "Deal - Deal Summary": translate_custom_field_value(data.get(CF_DEAL_SUMMARY), CF_DEAL_SUMMARY, fields_meta),
        "Deal - BU Database ID": translate_custom_field_value(data.get(CF_BU_DATABASE_ID), CF_BU_DATABASE_ID, fields_meta),
        "Deal - Contact Group ID": translate_custom_field_value(data.get(CF_CONTACT_GROUP_ID), CF_CONTACT_GROUP_ID, fields_meta),
        "Deal - STOP Marketing": translate_custom_field_value(data.get(CF_STOP_MARKETING), CF_STOP_MARKETING, fields_meta),
        "Deal - Label": deal_labels,
    }

    if DEBUG_PRINT_SELECTED:
        print("\n[DEBUG] SELECTED DEAL OUTPUT")
        for k, v in deal_dict.items():
            print(f"  {k}: {v!r}")
        print("[DEBUG] END SELECTED DEAL OUTPUT\n")

    return deal_dict
