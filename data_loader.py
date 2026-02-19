"""
Data loading, preprocessing, exchange rates, lazy loaders, and derived globals.
All shared DataFrames and KPI values live here.
"""

import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

import agent as ai_agent
from config import GENERIC_CATS, parse_categories, build_product_cat_map

load_dotenv()

# ── Currency utilities (from agent) ──

DISPLAY_CURRENCY = ai_agent.DISPLAY_CURRENCY
currency_symbol = ai_agent._sym
_format_converted_total = ai_agent._format_converted_total
convert_revenue = ai_agent.convert_revenue

# ============================================================
# LOAD DATA
# ============================================================

DATA_DIR = Path(__file__).parent


def _load_from_postgres():
    """Try to load data from PostgreSQL."""
    try:
        import db
        if not db.test_connection():
            return None
        hist, pred, metrics = db.load_for_dashboard()
        print("  [OK] Data loaded from PostgreSQL.")
        return hist, pred, metrics
    except Exception as e:
        print(f"  [WARNING] Could not load from Postgres: {e}")
        return None


def _load_from_csv():
    """Fallback: load data from CSVs."""
    files = {
        "historico": DATA_DIR / "vendas_historicas.csv",
        "previsoes": DATA_DIR / "previsoes_vendas.csv",
        "metricas": DATA_DIR / "metricas_modelos.csv",
    }

    for name, path in files.items():
        if not path.exists():
            print(f"File not found: {path}")
            print("Run first: py main.py")
            sys.exit(1)

    hist = pd.read_csv(files["historico"], parse_dates=["order_date"])
    pred = pd.read_csv(files["previsoes"], parse_dates=["order_date"])
    metrics = pd.read_csv(files["metricas"])

    print("  [OK] Data loaded from CSVs (fallback).")
    return hist, pred, metrics


def load_data():
    """Load data from PostgreSQL (preferred) or CSVs (fallback)."""
    result = _load_from_postgres()
    if result is None:
        hist, pred, metrics = _load_from_csv()
    else:
        hist, pred, metrics = result

    for df in [hist, pred, metrics]:
        if "category" not in df.columns:
            df["category"] = "Uncategorized"

    if "currency" not in hist.columns:
        hist["currency"] = "USD"

    if not metrics.empty and "product_id" in metrics.columns:
        metrics = metrics.drop_duplicates(subset=["product_id"], keep="first")

    for df in [hist, pred, metrics]:
        if "ticket_end_date" in df.columns:
            df["ticket_end_date"] = pd.to_datetime(df["ticket_end_date"], errors="coerce")

    return hist, pred, metrics


hist_df, pred_df, metrics_df = load_data()

# ============================================================
# EXCHANGE RATES & REVENUE CONVERSION
# ============================================================

_currencies_in_data = list(hist_df["currency"].dropna().unique()) if "currency" in hist_df.columns else []

_fallback_rates = ai_agent._FALLBACK_RATES_TO_USD
_target_cur = DISPLAY_CURRENCY
_startup_rates = {_target_cur: 1.0}
for cur in _currencies_in_data:
    if cur != _target_cur:
        usd_per_cur = _fallback_rates.get(cur, 1.0)
        usd_per_target = _fallback_rates.get(_target_cur, 1.0)
        _startup_rates[cur] = usd_per_cur / usd_per_target if usd_per_target > 0 else usd_per_cur

exchange_rates = _startup_rates
hist_df = convert_revenue(hist_df, exchange_rates)

_exchange_rate_cache = {"rates": None, "ts": 0}
_EXCHANGE_RATE_TTL = 3600


def get_exchange_rates():
    """Return live exchange rates, cached for 1 hour. Falls back to startup rates."""
    import time
    now = time.time()
    if _exchange_rate_cache["rates"] and (now - _exchange_rate_cache["ts"]) < _EXCHANGE_RATE_TTL:
        return _exchange_rate_cache["rates"]
    try:
        live = ai_agent.fetch_exchange_rates(_currencies_in_data)
        _exchange_rate_cache["rates"] = live
        _exchange_rate_cache["ts"] = now
        return live
    except Exception:
        return exchange_rates


print(f"  Display currency: {DISPLAY_CURRENCY}")
if len(_currencies_in_data) > 1:
    print(f"  Currencies found: {', '.join(sorted(_currencies_in_data))}")
    print(f"  Using fallback rates at startup (live rates fetched lazily)")

# ============================================================
# LAZY-LOADED SECONDARY DATA
# ============================================================

_lazy_cache = {}


def _get_db():
    """Get db module (import once)."""
    if "db" not in _lazy_cache:
        import db as _db
        _lazy_cache["db"] = _db
    return _lazy_cache["db"]


def get_hourly_df():
    """Lazy-load hourly sales data."""
    if "hourly_df" not in _lazy_cache:
        try:
            df = _get_db().load_hourly_sales()
            if not df.empty:
                df = convert_revenue(df, get_exchange_rates())
            _lazy_cache["hourly_df"] = df
            print(f"  [OK] Hourly sales loaded: {len(df)} rows")
        except Exception as e:
            print(f"  [WARNING] Could not load hourly sales: {e}")
            _lazy_cache["hourly_df"] = pd.DataFrame(columns=[
                "hour", "product_id", "product_name", "category",
                "ticket_end_date", "ticket_start_date",
                "quantity_sold", "revenue", "currency",
            ])
    return _lazy_cache["hourly_df"]


LOW_STOCK_THRESHOLD = 5


def get_low_stock_df():
    """Lazy-load low stock data."""
    if "low_stock_df" not in _lazy_cache:
        try:
            _lazy_cache["low_stock_df"] = _get_db().load_low_stock(LOW_STOCK_THRESHOLD)
        except Exception:
            _lazy_cache["low_stock_df"] = pd.DataFrame(
                columns=["product_id", "product_name", "category", "stock_quantity", "status", "price"])
    return _lazy_cache["low_stock_df"]


def get_source_df():
    """Lazy-load sales by source data (with category)."""
    if "source_df" not in _lazy_cache:
        try:
            _lazy_cache["source_df"] = _get_db().load_sales_by_source()
        except Exception:
            _lazy_cache["source_df"] = pd.DataFrame(
                columns=["source", "category", "quantity_sold", "revenue", "order_count"])
    return _lazy_cache["source_df"]


def get_cross_sell_df():
    """Lazy-load cross-sell (product pairs) data."""
    if "cross_sell_df" not in _lazy_cache:
        try:
            _lazy_cache["cross_sell_df"] = _get_db().load_cross_sell_data()
        except Exception:
            _lazy_cache["cross_sell_df"] = pd.DataFrame(columns=[
                "product_a_id", "product_a_name", "product_b_id", "product_b_name",
                "category_a", "category_b", "pair_count", "total_qty", "total_revenue",
            ])
    return _lazy_cache["cross_sell_df"]


def get_multi_product_orders_df():
    """Lazy-load multi-product orders detail."""
    if "multi_orders_df" not in _lazy_cache:
        try:
            _lazy_cache["multi_orders_df"] = _get_db().load_multi_product_orders()
        except Exception:
            _lazy_cache["multi_orders_df"] = pd.DataFrame(columns=[
                "order_id", "order_date", "product_id", "product_name",
                "quantity", "total", "currency", "billing_country",
                "billing_city", "category",
            ])
    return _lazy_cache["multi_orders_df"]


def get_multi_order_stats():
    """Lazy-load multi-order summary stats."""
    if "multi_order_stats" not in _lazy_cache:
        try:
            _lazy_cache["multi_order_stats"] = _get_db().load_multi_order_stats()
        except Exception:
            _lazy_cache["multi_order_stats"] = {"total_orders": 0, "multi_orders": 0, "max_products": 0, "avg_products": 0}
    return _lazy_cache["multi_order_stats"]


def get_geo_sales_df():
    """Lazy-load geo sales data with geocoding."""
    if "geo_sales_df" not in _lazy_cache:
        try:
            _db = _get_db()
            geo_df = _db.load_sales_by_location()
            if not geo_df.empty:
                _geo_cache = _db.load_geocache()
                if _geo_cache:
                    def _apply_geocode(row):
                        key = f"{str(row['country']).strip()}|{str(row['state']).strip()}|{str(row['city']).strip()}"
                        coords = _geo_cache.get(key)
                        if coords:
                            return pd.Series(coords, index=["lat", "lng"])
                        return pd.Series([None, None], index=["lat", "lng"])
                    geo_df[["lat", "lng"]] = geo_df.apply(_apply_geocode, axis=1)
                    geo_df = geo_df.dropna(subset=["lat", "lng"])
                else:
                    geo_df = geo_df.iloc[0:0]
            _lazy_cache["geo_sales_df"] = geo_df
        except Exception:
            _lazy_cache["geo_sales_df"] = pd.DataFrame(columns=[
                "country", "state", "city", "product_id", "product_name",
                "category", "quantity_sold", "revenue", "currency",
            ])
    return _lazy_cache["geo_sales_df"]


def invalidate_lazy_cache():
    """Clear all lazy-loaded data (called after sync)."""
    _lazy_cache.clear()


# ============================================================
# PREPROCESSING
# ============================================================

product_cat_map = build_product_cat_map(hist_df)

# ALL ORDERS DATA
try:
    import db as _db_orders
    all_orders_df = _db_orders.load_all_orders()
    print(f"  [OK] All orders loaded: {len(all_orders_df)} rows")
except Exception as _e:
    print(f"  [WARNING] Could not load orders: {_e}")
    all_orders_df = pd.DataFrame(columns=[
        "order_id", "order_date", "product_id", "product_name",
        "quantity", "total", "currency", "order_status",
        "billing_country", "billing_city", "order_source", "category",
    ])

orders_cat_map = build_product_cat_map(all_orders_df) if not all_orders_df.empty else {}

TODAY = pd.Timestamp.now().normalize()
ONLINE_COURSE_CATS = {"ONLINE COURSE"}


def build_event_status_map():
    """
    Create map product_id -> 'active', 'past', or 'course' based on
    ticket_end_date and category.
    """
    pid_cat_str = hist_df.groupby("product_id")["category"].first().to_dict()

    date_by_pid = {}
    for df in [metrics_df, pred_df, hist_df]:
        if "ticket_end_date" not in df.columns:
            continue
        end_dates = df.groupby("product_id")["ticket_end_date"].first().dropna()
        date_by_pid.update(end_dates.to_dict())

    status_map = {}
    no_date_pids = set()
    all_pids = set(hist_df["product_id"].unique())
    all_pids |= set(pred_df["product_id"].unique()) if "product_id" in pred_df.columns else set()

    course_pids = set()
    for pid in all_pids:
        cats = set(parse_categories(pid_cat_str.get(pid, "")))
        if cats & ONLINE_COURSE_CATS:
            status_map[pid] = "course"
            course_pids.add(pid)

    remaining_pids = all_pids - course_pids

    for pid in remaining_pids:
        if pid in date_by_pid and pd.notna(date_by_pid[pid]):
            status_map[pid] = "active" if date_by_pid[pid] >= TODAY else "past"
        else:
            no_date_pids.add(pid)

    cat_has_active = {}
    for pid_val, st in status_map.items():
        if st == "course":
            continue
        cat_str = pid_cat_str.get(pid_val, "")
        if not cat_str:
            continue
        for cat in parse_categories(cat_str):
            if cat not in GENERIC_CATS:
                if st == "active":
                    cat_has_active[cat] = True
                elif cat not in cat_has_active:
                    cat_has_active[cat] = False

    for pid in no_date_pids:
        cat_str = pid_cat_str.get(pid, "")
        if not cat_str:
            status_map[pid] = "past"
            continue
        product_cats = set(parse_categories(cat_str)) - GENERIC_CATS
        if product_cats and any(cat_has_active.get(c, False) for c in product_cats):
            status_map[pid] = "active"
        else:
            status_map[pid] = "past"

    return status_map


event_status_map = build_event_status_map()
n_active = sum(1 for v in event_status_map.values() if v == "active")
n_past = sum(1 for v in event_status_map.values() if v == "past")
n_courses = sum(1 for v in event_status_map.values() if v == "course")
print(f"  Events: {n_active} active, {n_past} past, {n_courses} online courses")

all_categories = sorted(set(
    cat
    for cats_str in hist_df["category"].dropna().unique()
    for cat in parse_categories(cats_str)
    if cat not in GENERIC_CATS
))

product_sales = (
    hist_df.groupby("product_id")
    .agg(
        product_name=("product_name", "first"),
        category=("category", "first"),
        quantity_sold=("quantity_sold", "sum"),
    )
    .reset_index()
    .sort_values("quantity_sold", ascending=False)
)

# General KPIs
total_products = hist_df["product_id"].nunique()
total_sales_qty = int(hist_df["quantity_sold"].sum())
total_revenue = hist_df["revenue"].sum()
total_orders_days = hist_df["order_date"].nunique()
date_min = hist_df["order_date"].min().strftime("%d/%m/%Y")
date_max = hist_df["order_date"].max().strftime("%d/%m/%Y")
pred_total_qty = pred_df["predicted_quantity"].sum()


def filter_by_event_tab(df, tab_value):
    """Filter DataFrame by event status (active/past/course) based on the tab.
    When tab is 'map', show all products (no event filter)."""
    if tab_value == "map" or "product_id" not in df.columns:
        return df
    pids = {pid for pid, st in event_status_map.items() if st == tab_value}
    return df[df["product_id"].isin(pids)]


def filter_by_currency(df, selected_currencies):
    """Filter DataFrame by selected currencies. If empty list, no filter applied."""
    if not selected_currencies or "currency" not in df.columns:
        return df
    return df[df["currency"].isin(selected_currencies)]


def reload_all_data():
    """Reload primary data and all derived globals after a successful sync."""
    global hist_df, pred_df, metrics_df
    global _currencies_in_data, exchange_rates
    global product_cat_map, all_orders_df, orders_cat_map
    global event_status_map, all_categories, product_sales
    global total_products, total_sales_qty, total_revenue
    global total_orders_days, date_min, date_max, pred_total_qty

    print("  [RELOAD] Refreshing all data after sync...")

    hist_df, pred_df, metrics_df = load_data()

    _currencies_in_data = list(hist_df["currency"].dropna().unique()) if "currency" in hist_df.columns else []
    rates = get_exchange_rates()
    exchange_rates = rates
    hist_df = convert_revenue(hist_df, rates)

    product_cat_map = build_product_cat_map(hist_df)

    try:
        all_orders_df = _get_db().load_all_orders()
    except Exception:
        all_orders_df = pd.DataFrame(columns=[
            "order_id", "order_date", "product_id", "product_name",
            "quantity", "total", "currency", "order_status",
            "billing_country", "billing_city", "order_source", "category",
        ])

    orders_cat_map = build_product_cat_map(all_orders_df) if not all_orders_df.empty else {}
    event_status_map = build_event_status_map()

    all_categories = sorted(set(
        cat
        for cats_str in hist_df["category"].dropna().unique()
        for cat in parse_categories(cats_str)
        if cat not in GENERIC_CATS
    ))

    product_sales = (
        hist_df.groupby("product_id")
        .agg(
            product_name=("product_name", "first"),
            category=("category", "first"),
            quantity_sold=("quantity_sold", "sum"),
        )
        .reset_index()
        .sort_values("quantity_sold", ascending=False)
    )

    total_products = hist_df["product_id"].nunique()
    total_sales_qty = int(hist_df["quantity_sold"].sum())
    total_revenue = hist_df["revenue"].sum()
    total_orders_days = hist_df["order_date"].nunique()
    date_min = hist_df["order_date"].min().strftime("%d/%m/%Y") if not hist_df.empty else "N/A"
    date_max = hist_df["order_date"].max().strftime("%d/%m/%Y") if not hist_df.empty else "N/A"
    pred_total_qty = pred_df["predicted_quantity"].sum() if not pred_df.empty else 0

    invalidate_lazy_cache()

    print(f"  [RELOAD] Done. {total_products} products, {total_sales_qty:,} sales loaded.")
