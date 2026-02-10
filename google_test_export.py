# google_test_export.py
import json
import os

def export_deal_to_json(deal: dict, filename: str = "deal_output.json"):
    """
    Exports a single deal dict to JSON for safe inspection.
    - Flattens lists (phones, emails, labels)
    - Converts all values to strings to avoid Google Sheets numeric issues
    """
    deal_copy = deal.copy()

    # Flatten phones and emails
    phone_fields = ["Person - Phone - Work", "Person - Phone - Home", "Person - Phone - Mobile", "Person - Phone - Other"]
    email_fields = ["Person - Email - Work", "Person - Email - Home", "Person - Email - Other"]
    for f in phone_fields + email_fields:
        if isinstance(deal_copy.get(f), list):
            deal_copy[f] = ", ".join([str(p) for p in deal_copy[f]])

    # Flatten labels if list
    if isinstance(deal_copy.get("Deal - Label"), list):
        deal_copy["Deal - Label"] = ", ".join([str(l) for l in deal_copy["Deal - Label"]])

    # Convert all other values to string (safe for Sheets)
    for k, v in deal_copy.items():
        if v is None:
            deal_copy[k] = ""
        elif not isinstance(v, str):
            deal_copy[k] = str(v)

    # Save JSON
    os.makedirs("test_output", exist_ok=True)
    path = os.path.join("test_output", filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(deal_copy, f, indent=2, ensure_ascii=False)
    
    print(f"Deal exported to {path}")
    return path
