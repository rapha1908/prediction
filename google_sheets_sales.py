"""
Google Sheets sales export.
Pushes WooCommerce order data to TCCHE FB DATA sheet, Página1.
Only adds orders not already in the sheet.
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_credentials():
    """Get credentials from GA4_CREDENTIALS_FILE or GOOGLE_SHEETS_CREDENTIALS."""
    val = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GA4_CREDENTIALS_FILE", "")
    val = (val or "").strip()
    if val.startswith("{") or val.startswith('{"'):
        creds = json.loads(val)
    else:
        from pathlib import Path
        path = Path(val)
        if not path.is_absolute():
            path = Path(__file__).parent / path
        with open(path, "r", encoding="utf-8") as f:
            creds = json.load(f)
    return creds


def _get_sheets_client():
    """Create gspread client with service account."""
    import gspread
    from google.oauth2.service_account import Credentials

    creds_dict = _get_credentials()
    credentials = Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)
    return gspread.authorize(credentials)


def _fetch_orders_from_wc() -> list[dict]:
    """Fetch all completed/processing orders from WooCommerce API."""
    url = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
    key = os.getenv("WOOCOMMERCE_KEY", "")
    secret = os.getenv("WOOCOMMERCE_SECRET", "")
    if not key or not secret:
        raise ValueError("WOOCOMMERCE_KEY and WOOCOMMERCE_SECRET required")

    all_orders = []
    for status in ["completed", "processing"]:
        page = 1
        while True:
            resp = requests.get(
                f"{url}orders",
                params={
                    "consumer_key": key,
                    "consumer_secret": secret,
                    "status": status,
                    "per_page": 100,
                    "page": page,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_orders.extend(data)
            page += 1

    return all_orders


def _order_to_row(order: dict) -> list:
    """Convert WooCommerce order to sheet row: id, date_created, total, currency, event_name, first_name, last_name, email, phone."""
    oid = order.get("id", "")
    date_created = order.get("date_created", "")[:10] if order.get("date_created") else ""
    total = str(order.get("total", ""))
    currency = order.get("currency", "USD")

    # event_name = product names (comma-separated if multiple)
    line_items = order.get("line_items", []) or []
    event_name = ", ".join(li.get("name", "") for li in line_items if li.get("name"))

    billing = order.get("billing", {}) or {}
    first_name = billing.get("first_name", "") or ""
    last_name = billing.get("last_name", "") or ""
    email = billing.get("email", "") or ""
    phone = billing.get("phone", "") or ""

    return [oid, date_created, total, currency, event_name, first_name, last_name, email, phone]


def update_sheet() -> tuple[int, str]:
    """
    Fetch orders from WooCommerce, get existing IDs from sheet, append only new rows.
    Returns (rows_added, message).
    """
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        return 0, "Configure GOOGLE_SHEETS_SPREADSHEET_ID (ID da planilha na URL)."

    try:
        gc = _get_sheets_client()
        sh = gc.open_by_key(spreadsheet_id)
        worksheet = sh.worksheet("Página1")
    except Exception as e:
        return 0, f"Erro ao abrir planilha: {e}. Compartilhe 'TCCHE FB DATA' com o email da service account."

    # Get existing order IDs from column A
    try:
        existing = set()
        all_rows = worksheet.get_all_values()
        if all_rows:
            header = all_rows[0]
            for row in all_rows[1:]:
                if row and row[0].strip().isdigit():
                    existing.add(int(row[0]))
    except Exception as e:
        return 0, f"Erro ao ler planilha: {e}"

    # Fetch orders from WooCommerce
    try:
        orders = _fetch_orders_from_wc()
    except Exception as e:
        return 0, f"Erro ao buscar pedidos WooCommerce: {e}"

    # Filter to only new orders
    new_rows = []
    for order in orders:
        oid = order.get("id")
        if oid and int(oid) not in existing:
            new_rows.append(_order_to_row(order))
            existing.add(int(oid))

    if not new_rows:
        return 0, "Nenhuma venda nova para adicionar."

    # Ensure header exists
    try:
        current = worksheet.get_all_values()
        if not current or current[0] != ["id", "date_created", "total", "currency", "event_name", "first_name", "last_name", "email", "phone"]:
            worksheet.update("A1:I1", [["id", "date_created", "total", "currency", "event_name", "first_name", "last_name", "email", "phone"]])
    except Exception:
        pass

    # Append new rows
    try:
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")
    except Exception as e:
        return 0, f"Erro ao adicionar linhas: {e}"

    return len(new_rows), f"{len(new_rows)} vendas adicionadas ao Google Sheet."
