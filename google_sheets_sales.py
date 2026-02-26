"""
Google Sheets sales export.
Pushes WooCommerce order data to TCCHE FB DATA sheet, Página1.
Only adds orders not already in the sheet.
"""

import logging
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    log.addHandler(h)


def _log(msg: str) -> None:
    """Log que aparece imediatamente no terminal (flush)."""
    print(f"[Sheets] {msg}", flush=True)

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_credentials():
    """Get credentials from GA4_CREDENTIALS_FILE or GOOGLE_SHEETS_CREDENTIALS."""
    val = os.getenv("GOOGLE_SHEETS_CREDENTIALS") or os.getenv("GA4_CREDENTIALS_FILE", "")
    val = (val or "").strip()
    if val.startswith("{") or val.startswith('{"'):
        creds = json.loads(val)
        # private_key do .env pode vir com \n literal; google-auth precisa de newline real
        if "private_key" in creds and isinstance(creds["private_key"], str):
            creds["private_key"] = creds["private_key"].replace("\\n", "\n")
    else:
        from pathlib import Path
        path = Path(val)
        if not path.is_absolute():
            path = Path(__file__).parent / path
        with open(path, "r", encoding="utf-8") as f:
            creds = json.load(f)
    return creds


def _get_service_account_email() -> str:
    """Return the service account email for sharing the sheet."""
    creds = _get_credentials()
    return creds.get("client_email", "")


def _get_sheets_client():
    """Create gspread client with service account."""
    import gspread
    from google.oauth2.service_account import Credentials

    creds_dict = _get_credentials()
    credentials = Credentials.from_service_account_info(creds_dict, scopes=SHEETS_SCOPES)
    return gspread.authorize(credentials)


def _fetch_orders_from_wc(min_id: int | None = None) -> list[dict]:
    """Fetch completed/processing orders from WooCommerce API.
    Se min_id for passado, para quando encontrar IDs <= min_id (otimização).
    """
    url = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
    key = os.getenv("WOOCOMMERCE_KEY", "")
    secret = os.getenv("WOOCOMMERCE_SECRET", "")
    if not key or not secret:
        raise ValueError("WOOCOMMERCE_KEY and WOOCOMMERCE_SECRET required")

    all_orders = []
    for status in ["completed", "processing"]:
        page = 1
        while True:
            params = {
                "consumer_key": key,
                "consumer_secret": secret,
                "status": status,
                "per_page": 100,
                "page": page,
                "orderby": "id",
                "order": "desc",
            }
            resp = requests.get(f"{url}orders", params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            if min_id is not None:
                # Parar quando chegar em IDs já na planilha (ordem desc)
                new_only = [o for o in data if int(o.get("id", 0)) > min_id]
                all_orders.extend(new_only)
                _log(f"WooCommerce {status} p.{page}: +{len(new_only)} novos (total: {len(all_orders)})")
                if any(int(o.get("id", 0)) <= min_id for o in data):
                    break  # Próximas páginas só terão IDs mais antigos
            else:
                all_orders.extend(data)
                _log(f"WooCommerce {status} p.{page}: +{len(data)} (total: {len(all_orders)})")
            page += 1

    return all_orders


def _order_to_row(order: dict) -> list:
    """Convert WooCommerce order to sheet row: id, date_created, total, currency, event_name, first_name, last_name, email, phone."""
    from datetime import datetime

    oid = order.get("id", "")

    # date_created com dia e hora: "19/01/2026 20:34:13"
    raw_date = order.get("date_created", "") or ""
    if raw_date:
        try:
            dt = datetime.fromisoformat(raw_date)
            date_created = dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            date_created = raw_date
    else:
        date_created = ""

    total = str(order.get("total", ""))
    currency = order.get("currency", "USD")

    # event_name: valor estático "purchase"
    event_name = "purchase"

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
    _log("Iniciando atualização da planilha TCCHE FB DATA")

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        _log("ERRO: GOOGLE_SHEETS_SPREADSHEET_ID não configurado")
        return 0, "Configure GOOGLE_SHEETS_SPREADSHEET_ID (ID da planilha na URL)."

    _log(f"Spreadsheet ID: {spreadsheet_id}")

    try:
        _log("Conectando ao Google Sheets...")
        gc = _get_sheets_client()
        sh = gc.open_by_key(spreadsheet_id)
        _log(f"Planilha aberta: {sh.title}")
        try:
            worksheet = sh.worksheet("Página1")
            _log("Aba 'Página1' encontrada")
        except Exception:
            worksheet = sh.worksheet("Sheet1")  # fallback se a aba tiver nome em inglês
            _log("Usando aba 'Sheet1' (fallback)")
    except Exception as e:
        _log(f"ERRO ao abrir planilha: {e}")
        log.exception("[Sheets] Erro ao abrir planilha: %s", e)
        err = str(e) or type(e).__name__
        sa_email = _get_service_account_email()
        share_hint = f" Compartilhe 'TCCHE FB DATA' com {sa_email}" if sa_email else ""
        return 0, f"Erro ao abrir planilha: {err}.{share_hint}"

    # Get existing order IDs from column A
    try:
        existing = set()
        all_rows = worksheet.get_all_values()
        if all_rows:
            header = all_rows[0]
            for row in all_rows[1:]:
                if row and row[0].strip().isdigit():
                    existing.add(int(row[0]))
        _log(f"IDs já na planilha: {len(existing)}")
    except Exception as e:
        _log(f"ERRO ao ler planilha: {e}")
        return 0, f"Erro ao ler planilha: {e}"

    # Fetch orders from WooCommerce (só os novos se já temos IDs na planilha)
    max_existing = max(existing) if existing else None
    try:
        _log("Buscando pedidos no WooCommerce...")
        orders = _fetch_orders_from_wc(min_id=max_existing)
        _log(f"Pedidos WooCommerce a processar: {len(orders)}")
    except Exception as e:
        _log(f"ERRO ao buscar pedidos WooCommerce: {e}")
        return 0, f"Erro ao buscar pedidos WooCommerce: {e}"

    # Filter to only new orders
    new_rows = []
    for order in orders:
        oid = order.get("id")
        if oid and int(oid) not in existing:
            new_rows.append(_order_to_row(order))
            existing.add(int(oid))

    if not new_rows:
        _log("Nenhuma venda nova para adicionar")
        return 0, "Nenhuma venda nova para adicionar."

    _log(f"Novas vendas a adicionar: {len(new_rows)}")

    # Ensure header exists
    try:
        current = worksheet.get_all_values()
        if not current or current[0] != ["id", "date_created", "total", "currency", "event_name", "first_name", "last_name", "email", "phone"]:
            worksheet.update("A1:I1", [["id", "date_created", "total", "currency", "event_name", "first_name", "last_name", "email", "phone"]])
            _log("Cabeçalho atualizado")
    except Exception as ex:
        _log(f"Aviso ao atualizar cabeçalho: {ex}")

    # Append new rows
    try:
        _log("Adicionando linhas na planilha...")
        worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")
        _log(f"{len(new_rows)} linhas adicionadas com sucesso")
    except Exception as e:
        _log(f"ERRO ao adicionar linhas: {e}")
        return 0, f"Erro ao adicionar linhas: {e}"

    return len(new_rows), f"{len(new_rows)} vendas adicionadas ao Google Sheet."