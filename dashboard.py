import os
import json
import subprocess
import pandas as pd
import numpy as np
import dash
from dash import Dash, html, dcc, callback, Output, Input, State, no_update, ctx, ALL
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import sys
from dotenv import load_dotenv
import agent as ai_agent

load_dotenv()

# Re-use shared currency utilities from agent
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

    # Ensure category column exists
    for df in [hist, pred, metrics]:
        if "category" not in df.columns:
            df["category"] = "Uncategorized"

    # Ensure currency column exists (backward compatibility with old data)
    if "currency" not in hist.columns:
        hist["currency"] = "USD"

    # Ensure no duplicate product_id in metrics
    if not metrics.empty and "product_id" in metrics.columns:
        metrics = metrics.drop_duplicates(subset=["product_id"], keep="first")

    # Parse ticket_end_date as datetime in all DataFrames
    for df in [hist, pred, metrics]:
        if "ticket_end_date" in df.columns:
            df["ticket_end_date"] = pd.to_datetime(df["ticket_end_date"], errors="coerce")

    return hist, pred, metrics


hist_df, pred_df, metrics_df = load_data()

# ============================================================
# EXCHANGE RATES & REVENUE CONVERSION (lazy HTTP, fast startup)
# ============================================================

_currencies_in_data = list(hist_df["currency"].dropna().unique()) if "currency" in hist_df.columns else []

# Use fallback rates at startup (no HTTP call) for fast startup
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

# Lazy exchange rate fetcher with 1-hour TTL cache
_exchange_rate_cache = {"rates": None, "ts": 0}
_EXCHANGE_RATE_TTL = 3600  # 1 hour


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
# LAZY-LOADED SECONDARY DATA (loaded on first access)
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
    """Lazy-load sales by source data."""
    if "source_df" not in _lazy_cache:
        try:
            _lazy_cache["source_df"] = _get_db().load_sales_by_source()
        except Exception:
            _lazy_cache["source_df"] = pd.DataFrame(
                columns=["source", "quantity_sold", "revenue", "order_count"])
    return _lazy_cache["source_df"]


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


def reload_all_data():
    """Reload primary data and all derived globals after a successful sync."""
    global hist_df, pred_df, metrics_df
    global _currencies_in_data, exchange_rates
    global product_cat_map, all_orders_df, orders_cat_map
    global event_status_map, all_categories, product_sales
    global total_products, total_sales_qty, total_revenue
    global total_orders_days, date_min, date_max, pred_total_qty

    print("  [RELOAD] Refreshing all data after sync...")

    # 1. Reload primary data from DB/CSV
    hist_df, pred_df, metrics_df = load_data()

    # 2. Re-apply exchange rate conversion
    _currencies_in_data = list(hist_df["currency"].dropna().unique()) if "currency" in hist_df.columns else []
    rates = get_exchange_rates()
    exchange_rates = rates
    hist_df = convert_revenue(hist_df, rates)

    # 3. Rebuild derived structures
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

    # 4. Clear lazy caches so they reload with fresh data
    invalidate_lazy_cache()

    print(f"  [RELOAD] Done. {total_products} products, {total_sales_qty:,} sales loaded.")


# Low stock data is now loaded dynamically via callback (supports archive/unarchive)


# ============================================================
# MULTI-CATEGORY HELPERS
# ============================================================

def parse_categories(cat_str):
    """Extract list of categories from a pipe-separated string."""
    if pd.isna(cat_str) or str(cat_str).strip() == "":
        return ["Uncategorized"]
    return [c.strip() for c in str(cat_str).split("|") if c.strip()]


def build_product_cat_map(df):
    """Create map product_id -> set of categories."""
    mapping = {}
    for _, row in df.drop_duplicates("product_id").iterrows():
        mapping[row["product_id"]] = set(parse_categories(row["category"]))
    return mapping


def product_matches_cats(product_id, selected_cats, cat_map):
    """Check if a product belongs to any of the selected categories."""
    return bool(cat_map.get(product_id, set()) & set(selected_cats))


def filter_by_categories(df, selected_cats, cat_map):
    """Filter DataFrame for products that belong to any of the categories."""
    matching_pids = {
        pid for pid, cats in cat_map.items()
        if cats & set(selected_cats)
    }
    return df[df["product_id"].isin(matching_pids)]


def explode_categories(df):
    """Expand rows so each category has its own row."""
    return (df.assign(category_list=df["category"].apply(parse_categories))
              .explode("category_list")
              .rename(columns={"category_list": "cat_single"}))


# ============================================================
# PREPROCESSING
# ============================================================

# Generic "type" categories that classify the product kind (EVENTS, LIVESTREAM, etc.)
# These are excluded from chart breakdowns and filters because the specific event
# name categories (e.g. "UK - CONFERENCE - LONDON - 2026") are more useful.
GENERIC_CATS = frozenset({
    "Uncategorized", "Sem categoria",
    "EVENTS", "LIVESTREAM", "ONLINE COURSE",
    "THE BREATHWORK REVOLUTION",
})

# Category map by product
product_cat_map = build_product_cat_map(hist_df)

# ============================================================
# ALL ORDERS DATA
# ============================================================
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

# Product -> active or past event map (based on ticket_end_date)
TODAY = pd.Timestamp.now().normalize()


ONLINE_COURSE_CATS = {"ONLINE COURSE"}


def build_event_status_map():
    """
    Create map product_id -> 'active', 'past', or 'course' based on
    ticket_end_date and category.
    Products in the ONLINE COURSE category are always classified as 'course'.
    Uses groupby for O(1) lookups instead of repeated DataFrame filtering.
    """
    # Build pid -> category string dict via groupby (O(n) once instead of O(n*m))
    pid_cat_str = hist_df.groupby("product_id")["category"].first().to_dict()

    # Collect the most reliable ticket_end_date for each product_id
    # Priority: hist > pred > metrics (hist last = highest priority)
    date_by_pid = {}
    for df in [metrics_df, pred_df, hist_df]:
        if "ticket_end_date" not in df.columns:
            continue
        end_dates = df.groupby("product_id")["ticket_end_date"].first().dropna()
        date_by_pid.update(end_dates.to_dict())

    # Classify each product
    status_map = {}
    no_date_pids = set()
    all_pids = set(hist_df["product_id"].unique())
    all_pids |= set(pred_df["product_id"].unique()) if "product_id" in pred_df.columns else set()

    # --- Pass 0: Online Courses go to their own tab ---
    course_pids = set()
    for pid in all_pids:
        cats = set(parse_categories(pid_cat_str.get(pid, "")))
        if cats & ONLINE_COURSE_CATS:
            status_map[pid] = "course"
            course_pids.add(pid)

    remaining_pids = all_pids - course_pids

    # --- Pass 1: products WITH ticket_end_date ---
    for pid in remaining_pids:
        if pid in date_by_pid and pd.notna(date_by_pid[pid]):
            status_map[pid] = "active" if date_by_pid[pid] >= TODAY else "past"
        else:
            no_date_pids.add(pid)

    # --- Pass 2: products WITHOUT ticket_end_date ---

    # Category map -> has active product? (using pass 1 results + pid_cat_str dict)
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

# Unique categories (expanded from pipe-separated, excluding generic type tags)
all_categories = sorted(set(
    cat
    for cats_str in hist_df["category"].dropna().unique()
    for cat in parse_categories(cats_str)
    if cat not in GENERIC_CATS
))

# Product list (sorted by total sold, grouped by product_id to avoid duplicates)
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

# ============================================================
# STYLE AND COLORS
# ============================================================

COLORS = {
    "bg": "#0b0b14",
    "card": "#131320",
    "card_border": "#1f1f32",
    "text": "#f0ebe3",
    "text_muted": "#8a847a",
    "accent": "#c8a44e",       # Gold – primary accent
    "accent2": "#e0b84a",      # Bright gold
    "accent3": "#5aaa88",      # Sage green
    "accent4": "#b87348",      # Warm copper
    "red": "#d44a4a",
    "grid": "#1a1a2c",
}

FONT = "'Outfit', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family=FONT, color=COLORS["text"], size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor=COLORS["grid"], showline=False, zeroline=False),
    yaxis=dict(gridcolor=COLORS["grid"], showline=False, rangemode="tozero", zeroline=False),
    hovermode="x unified",
    hoverlabel=dict(bgcolor=COLORS["card"], bordercolor=COLORS["accent"], font_color=COLORS["text"]),
)

# High-contrast palette for chart traces – each colour is a distinct hue
CATEGORY_COLORS = [
    "#c8a44e",  # Gold (brand)
    "#e06070",  # Rose / coral red
    "#4db8c7",  # Teal / cyan
    "#a668d8",  # Purple / violet
    "#e0873e",  # Orange
    "#5aaa88",  # Sage green
    "#7b8de0",  # Periwinkle blue
    "#d86890",  # Magenta / pink
    "#50b560",  # Green
    "#d4c84e",  # Lime / yellow-green
    "#c75a5a",  # Brick red
    "#3daaa0",  # Dark teal
    "#c07ed4",  # Orchid
    "#dda04a",  # Amber
    "#6898d0",  # Steel blue
]


def card_style(extra=None):
    base = {
        "backgroundColor": COLORS["card"],
        "border": f"1px solid {COLORS['card_border']}",
        "borderRadius": "14px",
        "padding": "28px",
        "boxShadow": "0 2px 12px rgba(0,0,0,0.25)",
    }
    if extra:
        base.update(extra)
    return base


def section_label(text):
    """Small uppercase label above section titles – tcche.org pattern."""
    return html.P(text, style={
        "color": COLORS["accent"], "fontSize": "11px",
        "textTransform": "uppercase", "letterSpacing": "2px",
        "fontWeight": "600", "margin": "0 0 6px",
    })


def kpi_card(title, value, subtitle="", color=COLORS["accent"]):
    return html.Div(
        style=card_style({"textAlign": "center", "flex": "1", "minWidth": "170px",
                          "borderTop": f"3px solid {color}"}),
        children=[
            html.P(title, style={
                "color": COLORS["text_muted"], "fontSize": "11px",
                "marginBottom": "4px", "textTransform": "uppercase",
                "letterSpacing": "1.5px", "fontWeight": "600",
            }),
            html.H2(value, style={
                "color": color, "margin": "10px 0 4px",
                "fontSize": "28px", "fontWeight": "700",
            }),
            html.P(subtitle, style={
                "color": COLORS["text_muted"], "fontSize": "11px", "margin": "0",
            }) if subtitle else None,
        ],
    )


dropdown_style = {
    "backgroundColor": COLORS["bg"],
    "color": COLORS["text"],
    "border": f"1px solid {COLORS['card_border']}",
    "borderRadius": "8px",
}

H_LEGEND = dict(
    orientation="h", yanchor="bottom", y=1.02,
    xanchor="right", x=1, bgcolor="rgba(0,0,0,0)",
)

# ============================================================
# LAYOUT
# ============================================================

app = Dash(
    __name__,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap"
    ],
    suppress_callback_exceptions=True,
)
app.title = "TCCHE – Sales Forecast Dashboard"

# Inject pulse animation CSS
app.index_string = '''<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.3; }
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            .dash-loading .dash-spinner::before {
                border-top-color: #c8a44e !important;
            }
            ._dash-loading-callback {
                visibility: visible !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>'''

app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"], "minHeight": "100vh",
        "fontFamily": FONT, "color": COLORS["text"], "padding": "0",
    },
    children=[

        # URL routing
        dcc.Location(id="url", refresh=False),
        # Location component for page reload after sync
        dcc.Location(id="page-reload", refresh=True),
        dcc.Store(id="sync-trigger", data=None),
        dcc.Store(id="sync-running", data=False),
        dcc.Interval(id="sync-poll", interval=1500, disabled=True),
        dcc.Download(id="report-download"),
        dcc.Store(id="report-trigger", data=None),
        dcc.Store(id="report-cache", data=None),
        dcc.Store(id="low-stock-refresh", data=0),

        # --- HEADER ---
        html.Div(
            style={
                "background": "linear-gradient(135deg, #13121e 0%, #1a1528 40%, #1e1610 100%)",
                "padding": "36px 48px 32px", "borderBottom": f"1px solid {COLORS['card_border']}",
            },
            children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start"}, children=[
                    html.Div(children=[
                        html.P("TCCHE", style={
                            "color": COLORS["accent"], "fontSize": "11px", "margin": "0 0 6px",
                            "letterSpacing": "3px", "textTransform": "uppercase", "fontWeight": "600",
                        }),
                        html.H1("Sales Forecast", style={
                            "margin": "0 0 6px", "fontSize": "30px", "fontWeight": "700",
                            "background": "linear-gradient(90deg, #c8a44e, #e0c87a, #b87348)",
                            "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                        }),
                        html.P(f"Data from {date_min} to {date_max}", style={
                            "color": COLORS["text_muted"], "margin": "0", "fontSize": "14px",
                            "letterSpacing": "0.5px",
                        }),
                    ]),
                    html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center"}, children=[
                        html.Div(id="sync-status", style={"fontSize": "13px", "color": COLORS["text_muted"]}),
                        html.Button(
                            "Sync & Retrain",
                            id="sync-btn",
                            n_clicks=0,
                            style={
                                "backgroundColor": COLORS["accent3"],
                                "color": "#fff",
                                "border": "none", "borderRadius": "8px",
                                "padding": "10px 24px", "fontSize": "13px",
                                "fontWeight": "700", "cursor": "pointer",
                                "fontFamily": FONT, "letterSpacing": "0.5px",
                                "whiteSpace": "nowrap",
                            },
                        ),
                        dcc.Link(
                            "Stock Manager",
                            href="/stock",
                            style={
                                "color": COLORS["accent"],
                                "fontSize": "12px",
                                "textDecoration": "none",
                                "border": f"1px solid {COLORS['accent']}",
                                "borderRadius": "8px",
                                "padding": "10px 18px",
                                "whiteSpace": "nowrap",
                                "fontFamily": FONT,
                                "fontWeight": "600",
                            },
                        ),
                        html.A(
                            "Logout",
                            href="/logout",
                            style={
                                "color": COLORS["text_muted"],
                                "fontSize": "12px",
                                "textDecoration": "none",
                                "border": f"1px solid {COLORS['card_border']}",
                                "borderRadius": "8px",
                                "padding": "10px 18px",
                                "whiteSpace": "nowrap",
                                "fontFamily": FONT,
                            },
                        ),
                    ]),
                ]),
            ],
        ),

        # --- SYNC LOG PANEL (hidden by default) ---
        html.Div(
            id="sync-log-panel",
            style={"display": "none"},
            children=[
                html.Div(
                    style={
                        "margin": "0 48px", "padding": "16px 20px",
                        "background": "#0b0b14", "borderRadius": "0 0 12px 12px",
                        "border": f"1px solid {COLORS['card_border']}",
                        "borderTop": "none",
                    },
                    children=[
                        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "10px"}, children=[
                            html.Div(style={
                                "width": "8px", "height": "8px", "borderRadius": "50%",
                                "backgroundColor": COLORS["accent3"],
                                "animation": "pulse 1.5s ease-in-out infinite",
                            }),
                            html.Span("SYNC IN PROGRESS", style={
                                "fontSize": "11px", "fontWeight": "700",
                                "letterSpacing": "2px", "color": COLORS["accent3"],
                            }),
                            html.Span(id="sync-step", style={
                                "fontSize": "12px", "color": COLORS["text_muted"],
                                "marginLeft": "auto",
                            }),
                        ]),
                        html.Pre(
                            id="sync-log",
                            style={
                                "fontFamily": "'Courier New', monospace",
                                "fontSize": "11px",
                                "color": COLORS["text_muted"],
                                "backgroundColor": "transparent",
                                "margin": "0",
                                "padding": "0",
                                "maxHeight": "200px",
                                "overflowY": "auto",
                                "whiteSpace": "pre-wrap",
                                "wordBreak": "break-all",
                                "lineHeight": "1.5",
                            },
                        ),
                    ],
                ),
            ],
        ),

        # --- STOCK MANAGER PAGE (hidden by default, shown on /stock) ---
        html.Div(id="stock-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"}, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "28px"}, children=[
                html.Div(children=[
                    dcc.Link("< Back to Dashboard", href="/", style={
                        "color": COLORS["text_muted"], "fontSize": "13px", "textDecoration": "none",
                        "marginBottom": "8px", "display": "block",
                    }),
                    section_label("STOCK MANAGER"),
                    html.H2("Automatic Stock Replenishment", style={
                        "margin": "0", "fontSize": "24px", "fontWeight": "700",
                        "background": "linear-gradient(90deg, #c8a44e, #e0c87a, #b87348)",
                        "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                    }),
                    html.P("Manage artificial scarcity. Products auto-replenish when stock gets low.",
                           style={"color": COLORS["text_muted"], "fontSize": "14px", "margin": "4px 0 0"}),
                ]),
                html.Button("Run Auto-Replenish Now", id="replenish-btn", n_clicks=0, style={
                    "backgroundColor": COLORS["accent3"], "color": "#fff", "border": "none",
                    "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
                    "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                }),
            ]),
            html.Div(id="replenish-result", style={"marginBottom": "16px"}),

            # --- ADD PRODUCT FORM ---
            html.Div(style=card_style({"marginBottom": "24px", "borderLeft": f"3px solid {COLORS['accent']}"}), children=[
                section_label("ADD PRODUCT"),
                html.Div(style={"display": "flex", "gap": "12px", "alignItems": "flex-end", "flexWrap": "wrap"}, children=[
                    html.Div(style={"flex": "2", "minWidth": "250px"}, children=[
                        html.Label("Product:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        dcc.Dropdown(
                            id="stock-product-picker",
                            options=[],
                            placeholder="Search for a product...",
                            style={"backgroundColor": COLORS["card"], "color": COLORS["text"],
                                   "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px"},
                        ),
                    ]),
                    html.Div(style={"flex": "0.5", "minWidth": "100px"}, children=[
                        html.Label("WC Stock:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        html.Div(id="stock-picker-wc-stock", style={
                            "padding": "10px 14px", "backgroundColor": COLORS["bg"],
                            "borderRadius": "8px", "fontSize": "14px", "fontWeight": "600",
                            "border": f"1px solid {COLORS['card_border']}",
                        }, children="--"),
                    ]),
                    html.Div(style={"flex": "0.5", "minWidth": "100px"}, children=[
                        html.Label("Sold:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        html.Div(id="stock-picker-sold", style={
                            "padding": "10px 14px", "backgroundColor": COLORS["bg"],
                            "borderRadius": "8px", "fontSize": "14px", "fontWeight": "600",
                            "border": f"1px solid {COLORS['card_border']}",
                        }, children="--"),
                    ]),
                    html.Div(style={"flex": "0.7", "minWidth": "120px"}, children=[
                        html.Label("Total Stock:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        dcc.Input(id="stock-total-input", type="number", min=1, placeholder="e.g. 200",
                                  style={"width": "100%", "padding": "9px 14px", "backgroundColor": COLORS["bg"],
                                         "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "fontSize": "14px", "fontFamily": FONT}),
                    ]),
                    html.Div(style={"flex": "0.5", "minWidth": "100px"}, children=[
                        html.Label("Replenish:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        dcc.Input(id="stock-replenish-input", type="number", min=1, value=20,
                                  style={"width": "100%", "padding": "9px 14px", "backgroundColor": COLORS["bg"],
                                         "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "fontSize": "14px", "fontFamily": FONT}),
                    ]),
                    html.Div(style={"flex": "0.5", "minWidth": "100px"}, children=[
                        html.Label("Threshold:", style={"fontSize": "12px", "color": COLORS["text_muted"], "display": "block", "marginBottom": "4px"}),
                        dcc.Input(id="stock-threshold-input", type="number", min=1, value=5,
                                  style={"width": "100%", "padding": "9px 14px", "backgroundColor": COLORS["bg"],
                                         "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "fontSize": "14px", "fontFamily": FONT}),
                    ]),
                    html.Button("Add Product", id="stock-add-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent"], "color": "#fff", "border": "none",
                        "borderRadius": "8px", "padding": "10px 24px", "fontSize": "13px",
                        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                        "whiteSpace": "nowrap", "alignSelf": "flex-end",
                    }),
                ]),
                html.Div(id="stock-add-feedback", style={"marginTop": "8px", "fontSize": "13px"}),
            ]),

            # --- MANAGED PRODUCTS TABLE ---
            html.Div(style=card_style({}), children=[
                section_label("MANAGED PRODUCTS"),
                html.Div(id="stock-manager-table", style={"overflowX": "auto"}),
            ]),
            dcc.Store(id="stock-refresh", data=0),
        ]),

        # --- DASHBOARD CONTENT (main page) ---
        html.Div(id="dashboard-page", style={"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"}, children=[

            # KPIs (dinamicos com a tab)
            html.Div(id="kpi-container",
                style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px"},
            ),

            # ============ LOW STOCK + SALES SOURCES (50/50 grid) ============
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "28px"}, children=[

                # --- LEFT: Inventory Alert (dynamic) ---
                html.Div(
                    id="low-stock-container",
                    style={
                        **card_style({"borderLeft": "4px solid #e05555"}),
                        "minHeight": "200px",
                    },
                ),

                # --- RIGHT: Sales Sources ---
                html.Div(
                    style=card_style({"borderLeft": f"4px solid {COLORS['accent']}", "minHeight": "200px"}),
                    children=[
                        html.Div(style={"marginBottom": "14px"}, children=[
                            section_label("ACQUISITION"),
                            html.H3("Sales Sources", style={
                                "margin": "0", "fontSize": "18px", "fontWeight": "700",
                            }),
                        ]),
                        dcc.Graph(
                            id="source-chart",
                            config={"displayModeBar": False},
                            style={"height": "280px"},
                        ),
                    ],
                ),
            ]),

            # ============ AI SALES ASSISTANT ============
            html.Div(style=card_style({"marginBottom": "28px", "borderTop": f"3px solid {COLORS['accent']}"}), children=[
                # Header + quick action buttons
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                                "marginBottom": "18px", "flexWrap": "wrap", "gap": "12px"}, children=[
                    html.Div(children=[
                        section_label("AI ASSISTANT"),
                        html.H3("Sales Intelligence", style={
                            "margin": "0 0 2px", "fontSize": "18px", "fontWeight": "700",
                            "color": COLORS["text"],
                        }),
                        html.P("Ask anything about your sales, products, or forecasts", style={
                            "color": COLORS["text_muted"], "fontSize": "12px", "margin": "0",
                        }),
                    ]),
                    html.Div(style={"display": "flex", "gap": "8px", "flexWrap": "wrap"}, children=[
                        html.Button("Daily Report", id="quick-daily", n_clicks=0, style={
                            "backgroundColor": "rgba(200, 164, 78, 0.1)", "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                            "padding": "7px 16px", "fontSize": "12px", "cursor": "pointer",
                            "fontFamily": FONT, "fontWeight": "500", "letterSpacing": "0.5px",
                        }),
                        html.Button("Weekly Summary", id="quick-weekly", n_clicks=0, style={
                            "backgroundColor": "rgba(90, 170, 136, 0.1)", "color": COLORS["accent3"],
                            "border": f"1px solid {COLORS['accent3']}", "borderRadius": "6px",
                            "padding": "7px 16px", "fontSize": "12px", "cursor": "pointer",
                            "fontFamily": FONT, "fontWeight": "500", "letterSpacing": "0.5px",
                        }),
                        html.Button("Top Products", id="quick-top", n_clicks=0, style={
                            "backgroundColor": "rgba(184, 115, 72, 0.1)", "color": COLORS["accent4"],
                            "border": f"1px solid {COLORS['accent4']}", "borderRadius": "6px",
                            "padding": "7px 16px", "fontSize": "12px", "cursor": "pointer",
                            "fontFamily": FONT, "fontWeight": "500", "letterSpacing": "0.5px",
                        }),
                        html.Button("Forecast Analysis", id="quick-forecast", n_clicks=0, style={
                            "backgroundColor": "rgba(224, 184, 74, 0.1)", "color": COLORS["accent2"],
                            "border": f"1px solid {COLORS['accent2']}", "borderRadius": "6px",
                            "padding": "7px 16px", "fontSize": "12px", "cursor": "pointer",
                            "fontFamily": FONT, "fontWeight": "500", "letterSpacing": "0.5px",
                        }),
                    ]),
                ]),

                # Chat messages area
                html.Div(id="chat-display", style={
                    "maxHeight": "500px", "overflowY": "auto", "marginBottom": "16px",
                    "padding": "18px", "backgroundColor": COLORS["bg"],
                    "borderRadius": "10px", "border": f"1px solid {COLORS['card_border']}",
                    "minHeight": "80px",
                }, children=[
                    html.Div(style={"display": "flex", "gap": "10px", "alignItems": "flex-start"}, children=[
                        html.Div("AI", style={
                            "backgroundColor": COLORS["accent"], "color": COLORS["bg"],
                            "borderRadius": "50%", "width": "30px", "height": "30px",
                            "display": "flex", "alignItems": "center", "justifyContent": "center",
                            "fontSize": "11px", "fontWeight": "700", "flexShrink": "0",
                        }),
                        dcc.Markdown(
                            "Hello! I'm your **AI Sales Assistant**. Ask me anything about your sales, "
                            "products, or forecasts. You can also use the quick action buttons above to "
                            "generate reports instantly.",
                            style={"color": COLORS["text"], "fontSize": "13px", "margin": "0",
                                   "lineHeight": "1.7", "flex": "1"},
                        ),
                    ]),
                ]),

                # Input area
                html.Div(style={"display": "flex", "gap": "12px"}, children=[
                    dcc.Input(
                        id="chat-input",
                        type="text",
                        placeholder="Ask about sales, products, forecasts...",
                        debounce=False,
                        n_submit=0,
                        style={
                            "flex": "1", "backgroundColor": COLORS["bg"],
                            "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                            "borderRadius": "8px", "padding": "12px 16px", "fontSize": "13px",
                            "fontFamily": FONT, "outline": "none",
                        },
                    ),
                    html.Button("Send", id="chat-send", n_clicks=0, style={
                        "backgroundColor": COLORS["accent"], "color": COLORS["bg"],
                        "border": "none", "borderRadius": "8px", "padding": "12px 28px",
                        "fontSize": "13px", "fontWeight": "700", "cursor": "pointer",
                        "fontFamily": FONT, "letterSpacing": "0.5px",
                    }),
                    html.Button("Clear", id="chat-clear", n_clicks=0, style={
                        "backgroundColor": "transparent", "color": COLORS["text_muted"],
                        "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px",
                        "padding": "12px 16px", "fontSize": "13px", "cursor": "pointer",
                        "fontFamily": FONT,
                    }),
                ]),

                # Hidden stores
                dcc.Store(id="chat-history", data=[]),
                dcc.Store(id="chat-loading", data=False),
            ]),

            # ============ TABS: EVENTOS ATIVOS / PASSADOS ============
            dcc.Tabs(
                id="event-tabs",
                value="active",
                style={"marginBottom": "24px"},
                children=[
                    dcc.Tab(
                        label=f"Active Events ({n_active})",
                        value="active",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 28px", "fontFamily": FONT, "fontSize": "13px", "fontWeight": "500",
                               "letterSpacing": "0.5px", "textTransform": "uppercase"},
                        selected_style={"backgroundColor": COLORS["card"], "color": COLORS["accent"],
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 28px",
                                        "fontFamily": FONT, "fontSize": "13px", "fontWeight": "700",
                                        "letterSpacing": "0.5px", "textTransform": "uppercase"},
                    ),
                    dcc.Tab(
                        label=f"Past Events ({n_past})",
                        value="past",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 28px", "fontFamily": FONT, "fontSize": "13px", "fontWeight": "500",
                               "letterSpacing": "0.5px", "textTransform": "uppercase"},
                        selected_style={"backgroundColor": COLORS["card"], "color": COLORS["accent4"],
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 28px",
                                        "fontFamily": FONT, "fontSize": "13px", "fontWeight": "700",
                                        "letterSpacing": "0.5px", "textTransform": "uppercase"},
                    ),
                    dcc.Tab(
                        label=f"Online Courses ({n_courses})",
                        value="course",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 28px", "fontFamily": FONT, "fontSize": "13px", "fontWeight": "500",
                               "letterSpacing": "0.5px", "textTransform": "uppercase"},
                        selected_style={"backgroundColor": COLORS["card"], "color": COLORS["accent3"],
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 28px",
                                        "fontFamily": FONT, "fontSize": "13px", "fontWeight": "700",
                                        "letterSpacing": "0.5px", "textTransform": "uppercase"},
                    ),
                    dcc.Tab(
                        label="Sales Map",
                        value="map",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 28px", "fontFamily": FONT, "fontSize": "13px", "fontWeight": "500",
                               "letterSpacing": "0.5px", "textTransform": "uppercase"},
                        selected_style={"backgroundColor": COLORS["card"], "color": "#6ea8d9",
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 28px",
                                        "fontFamily": FONT, "fontSize": "13px", "fontWeight": "700",
                                        "letterSpacing": "0.5px", "textTransform": "uppercase"},
                    ),
                ],
            ),

            # ============ SALES MAP SECTION ============
            html.Div(id="map-section", style={"display": "none"}, children=[
                html.Div(style=card_style({"marginBottom": "28px"}), children=[
                    section_label("GEOGRAPHY"),
                    html.H3("Sales Map", style={
                        "margin": "0 0 14px", "fontSize": "18px", "fontWeight": "700",
                    }),
                    html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "18px", "flexWrap": "wrap"}, children=[
                        html.Div(style={"minWidth": "280px", "flex": "1"}, children=[
                            html.Label("Filter by Category:", style={
                                "fontSize": "13px", "color": COLORS["text_muted"],
                                "marginBottom": "4px", "display": "block",
                            }),
                            dcc.Dropdown(
                                id="map-category-filter",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="All categories (select to filter)...",
                                style={
                                    "backgroundColor": COLORS["bg"], "color": COLORS["text"],
                                    "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "8px", "fontSize": "13px",
                                },
                            ),
                        ]),
                        html.Div(style={"minWidth": "280px", "flex": "1"}, children=[
                            html.Label("Filter by Product:", style={
                                "fontSize": "13px", "color": COLORS["text_muted"],
                                "marginBottom": "4px", "display": "block",
                            }),
                            dcc.Dropdown(
                                id="map-product-filter",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="All products (select to filter)...",
                                style={
                                    "backgroundColor": COLORS["bg"], "color": COLORS["text"],
                                    "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "8px", "fontSize": "13px",
                                },
                            ),
                        ]),
                    ]),
                    dcc.Graph(id="sales-map", config={"displayModeBar": True, "scrollZoom": True},
                              style={"height": "600px"}),
                ]),
            ]),

            # ============ REPORTE DIARIO ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("DAILY OVERVIEW"),
                html.H3("Sales & 7-Day Forecast", style={
                    "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                }),
                html.P("Recent sales per product and daily forecast for the next 7 days", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "18px",
                }),
                html.Div(id="daily-report", style={"overflowX": "auto", "maxHeight": "600px", "overflowY": "auto"}),
            ]),

            # ============ FILTROS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("FILTERS"),
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                                "marginBottom": "14px", "flexWrap": "wrap", "gap": "10px"}, children=[
                    html.H3("Filter by Category & Currency", style={
                        "margin": "0", "fontSize": "18px", "fontWeight": "700",
                    }),
                    html.Button(
                        "Generate Report",
                        id="report-btn",
                        n_clicks=0,
                        style={
                            "backgroundColor": "transparent",
                            "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}",
                            "borderRadius": "8px",
                            "padding": "8px 20px", "fontSize": "12px",
                            "fontWeight": "600", "cursor": "pointer",
                            "fontFamily": FONT, "letterSpacing": "0.5px",
                            "whiteSpace": "nowrap",
                            "transition": "all 0.2s ease",
                        },
                    ),
                ]),
                html.Div(style={"display": "flex", "gap": "16px", "alignItems": "center", "flexWrap": "wrap"}, children=[
                    html.Div(style={"flex": "1", "minWidth": "300px"}, children=[
                        html.Label("Categories:", style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block"}),
                        dcc.Dropdown(
                            id="category-filter",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select categories...",
                            style=dropdown_style,
                        ),
                    ]),
                    html.Div(style={"minWidth": "180px"}, children=[
                        html.Label("Currency:", style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block"}),
                        dcc.Dropdown(
                            id="currency-filter",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="All currencies",
                            style=dropdown_style,
                        ),
                    ]),
                    html.Div(style={"minWidth": "180px"}, children=[
                        html.Label("Granularity:", style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block"}),
                        dcc.RadioItems(
                            id="time-granularity",
                            options=[
                                {"label": " Daily", "value": "daily"},
                                {"label": " Weekly", "value": "weekly"},
                            ],
                            value="daily",
                            inline=True,
                            style={"fontSize": "13px"},
                            inputStyle={"marginRight": "4px"},
                            labelStyle={"marginRight": "16px", "cursor": "pointer"},
                        ),
                    ]),
                ]),
            ]),

            # ============ REPORT MODAL ============
            html.Div(
                id="report-modal",
                style={"display": "none"},
                children=[
                    html.Div(
                        style={
                            "position": "fixed", "top": "0", "left": "0",
                            "width": "100vw", "height": "100vh",
                            "backgroundColor": "rgba(0,0,0,0.7)",
                            "zIndex": "9998",
                        },
                        id="report-overlay",
                        n_clicks=0,
                    ),
                    html.Div(
                        style={
                            "position": "fixed", "top": "3vh", "left": "5vw",
                            "width": "90vw", "height": "94vh",
                            "backgroundColor": COLORS["bg"],
                            "border": f"1px solid {COLORS['card_border']}",
                            "borderRadius": "16px",
                            "zIndex": "9999",
                            "display": "flex", "flexDirection": "column",
                            "boxShadow": "0 8px 40px rgba(0,0,0,0.5)",
                        },
                        children=[
                            # Modal header
                            html.Div(
                                style={
                                    "display": "flex", "justifyContent": "space-between",
                                    "alignItems": "center", "padding": "20px 28px",
                                    "borderBottom": f"1px solid {COLORS['card_border']}",
                                    "flexShrink": "0",
                                },
                                children=[
                                    html.Div(children=[
                                        section_label("REPORT"),
                                        html.H2("Sales Report", style={
                                            "margin": "0", "fontSize": "22px", "fontWeight": "700",
                                        }),
                                    ]),
                                    html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center"}, children=[
                                        html.Button(
                                            id="report-download-btn",
                                            n_clicks=0,
                                            style={
                                                "backgroundColor": COLORS["accent"],
                                                "color": "#0b0b14",
                                                "border": "none", "borderRadius": "8px",
                                                "padding": "8px 20px", "fontSize": "12px",
                                                "fontWeight": "700", "cursor": "pointer",
                                                "fontFamily": FONT,
                                                "minWidth": "145px",
                                                "display": "flex", "alignItems": "center",
                                                "justifyContent": "center", "gap": "8px",
                                            },
                                            children=[
                                                html.Span(id="pdf-spinner", style={"display": "none"},
                                                          children=html.Span(style={
                                                              "width": "14px", "height": "14px",
                                                              "border": "2px solid rgba(11,11,20,0.3)",
                                                              "borderTop": "2px solid #0b0b14",
                                                              "borderRadius": "50%",
                                                              "display": "inline-block",
                                                              "animation": "spin 0.8s linear infinite",
                                                          })),
                                                html.Span("Download PDF", id="pdf-btn-text"),
                                            ],
                                        ),
                                        html.Button(
                                            "Close",
                                            id="report-close-btn",
                                            n_clicks=0,
                                            style={
                                                "backgroundColor": "transparent",
                                                "color": COLORS["text_muted"],
                                                "border": f"1px solid {COLORS['card_border']}",
                                                "borderRadius": "8px",
                                                "padding": "8px 18px", "fontSize": "12px",
                                                "fontWeight": "600", "cursor": "pointer",
                                                "fontFamily": FONT,
                                            },
                                        ),
                                    ]),
                                ],
                            ),
                            # Modal body (scrollable)
                            html.Div(
                                style={"flex": "1", "overflowY": "auto", "padding": "28px"},
                                children=[
                                    dcc.Loading(
                                        id="report-loading",
                                        type="dot",
                                        color=COLORS["accent"],
                                        children=html.Div(id="report-content"),
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),

            # ============ VENDAS POR CATEGORIA AO LONGO DO TEMPO ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("TIMELINE"),
                html.H3("Sales by Category Over Time", style={
                    "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                }),
                dcc.Graph(id="category-timeline", config={"displayModeBar": False}),
            ]),

            # ============ PREVISAO POR CATEGORIA (DIARIA) ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("FORECAST"),
                html.H3("Daily Forecast by Category (Next 30 Days)", style={
                    "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                }),
                dcc.Graph(id="category-forecast", config={"displayModeBar": False}),
            ]),

            # ============ PREVISAO INDIVIDUAL POR PRODUTO (largura total) ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("PRODUCT DETAIL"),
                html.H3("Actual vs Forecast by Product", style={
                    "margin": "0 0 14px", "fontSize": "18px", "fontWeight": "700",
                }),
                dcc.Dropdown(
                    id="product-selector",
                    placeholder="Select a product...",
                    style={**dropdown_style, "marginBottom": "16px"},
                ),
                dcc.Graph(id="product-forecast", style={"height": "420px"}, config={"displayModeBar": False}),
            ]),

            # ============ TOP PRODUTOS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("TOP SELLERS"),
                html.H3("Top 15 Products (Selected Categories)", style={
                    "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                }),
                dcc.Graph(id="top-products-chart", config={"displayModeBar": False}),
            ]),

            # ============ GRID: RECEITA + DIA DA SEMANA ============
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "28px"}, children=[

                html.Div(style=card_style(), children=[
                    section_label("REVENUE"),
                    html.H3("Monthly Revenue", style={
                        "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                    }),
                    dcc.Graph(id="monthly-revenue", config={"displayModeBar": False}),
                ]),

                html.Div(style=card_style(), children=[
                    section_label("PATTERNS"),
                    html.H3("Sales by Day of Week", style={
                        "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                    }),
                    dcc.Graph(id="weekday-chart", config={"displayModeBar": False}),
                ]),

            ]),

            # ============ BEST HOURS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("PATTERNS"),
                html.H3("Best Hours for Sales", style={
                    "margin": "0 0 18px", "fontSize": "18px", "fontWeight": "700",
                }),
                dcc.Graph(id="hourly-chart", config={"displayModeBar": False}),
            ]),

            # ============ TABELA DE METRICAS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("MODEL PERFORMANCE"),
                html.H3("Prediction Metrics", style={
                    "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                }),
                html.P("Sorted by total forecast (30 days)", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "18px",
                }),
                html.Div(id="metrics-table", style={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"}),
            ]),

            # ============ ALL ORDERS TABLE ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                                "marginBottom": "18px", "flexWrap": "wrap", "gap": "12px"}, children=[
                    html.Div(children=[
                        section_label("ORDERS"),
                        html.H3("All Orders", style={
                            "margin": "0 0 2px", "fontSize": "18px", "fontWeight": "700",
                        }),
                        html.P(id="orders-count", style={
                            "color": COLORS["text_muted"], "fontSize": "12px", "margin": "0",
                        }),
                    ]),
                    html.Div(style={"display": "flex", "gap": "12px", "alignItems": "center", "flexWrap": "wrap"}, children=[
                        dcc.Input(
                            id="orders-search",
                            type="text",
                            placeholder="Search by order #, product, city, country...",
                            debounce=True,
                            style={
                                "width": "340px",
                                "backgroundColor": COLORS["bg"],
                                "color": COLORS["text"],
                                "border": f"1px solid {COLORS['card_border']}",
                                "borderRadius": "8px",
                                "padding": "10px 16px",
                                "fontSize": "13px",
                                "fontFamily": FONT,
                                "outline": "none",
                            },
                        ),
                        html.Div(style={"display": "flex", "gap": "6px", "alignItems": "center"}, children=[
                            html.Label("Show:", style={
                                "fontSize": "12px", "color": COLORS["text_muted"],
                            }),
                            dcc.Dropdown(
                                id="orders-page-size",
                                options=[
                                    {"label": "25", "value": 25},
                                    {"label": "50", "value": 50},
                                    {"label": "100", "value": 100},
                                    {"label": "250", "value": 250},
                                ],
                                value=50,
                                clearable=False,
                                style={
                                    "width": "80px",
                                    "backgroundColor": COLORS["bg"],
                                    "color": COLORS["text"],
                                    "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "8px",
                                    "fontSize": "13px",
                                },
                            ),
                        ]),
                    ]),
                ]),
                html.Div(id="orders-table", style={
                    "overflowX": "auto", "maxHeight": "600px", "overflowY": "auto",
                }),
                html.Div(id="orders-pagination", style={
                    "display": "flex", "justifyContent": "center", "alignItems": "center",
                    "gap": "8px", "marginTop": "16px",
                }),
                dcc.Store(id="orders-page", data=1),
            ]),

            # FOOTER
            html.Div(style={"textAlign": "center", "padding": "28px 0 20px",
                            "borderTop": f"1px solid {COLORS['card_border']}", "marginTop": "12px"}, children=[
                html.P("TCCHE", style={
                    "color": COLORS["accent"], "fontSize": "11px", "margin": "0 0 6px",
                    "letterSpacing": "3px", "fontWeight": "600",
                }),
                html.P("Sales Forecast Dashboard", style={
                    "color": COLORS["text_muted"], "fontSize": "12px", "margin": "0",
                }),
            ]),
        ]),
    ],
)


# ============================================================
# CALLBACKS
# ============================================================

# ============================================================
# URL ROUTING (toggle dashboard / stock page visibility)
# ============================================================

@callback(
    Output("dashboard-page", "style"),
    Output("stock-page", "style"),
    Input("url", "pathname"),
)
def route_page(pathname):
    """Show/hide pages based on URL."""
    base_style_dash = {"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"}
    base_style_stock = {**base_style_dash}
    if pathname == "/stock":
        return {**base_style_dash, "display": "none"}, {**base_style_stock, "display": "block"}
    return {**base_style_dash, "display": "block"}, {**base_style_stock, "display": "none"}


# ============================================================
# STOCK MANAGER CALLBACKS
# ============================================================

@callback(
    Output("stock-product-picker", "options"),
    Input("url", "pathname"),
    Input("stock-refresh", "data"),
)
def load_stock_picker_options(pathname, _refresh):
    """Load product options for the stock picker dropdown."""
    if pathname != "/stock":
        return []
    try:
        import db as _db_mod
        df = _db_mod.get_products_for_stock_picker()
        if df.empty:
            print("  [STOCK] No products found for stock picker.")
            return []
        options = [
            {"label": f"{row['product_name']} (stock: {int(row['stock_quantity'] or 0)}, sold: {int(row['total_sales'] or 0)})",
             "value": int(row["product_id"])}
            for _, row in df.iterrows()
        ]
        print(f"  [STOCK] Loaded {len(options)} products for picker.")
        return options
    except Exception as e:
        print(f"  [STOCK ERROR] Could not load products: {e}")
        return []


@callback(
    Output("stock-picker-wc-stock", "children"),
    Output("stock-picker-sold", "children"),
    Input("stock-product-picker", "value"),
)
def show_product_stock_info(product_id):
    """Show current WC stock and total sold for selected product."""
    if not product_id:
        return "--", "--"
    try:
        import db as _db_mod
        eng = _db_mod._get_engine()
        row = pd.read_sql(
            "SELECT stock_quantity, total_sales FROM products WHERE id = %(pid)s",
            eng, params={"pid": int(product_id)},
        )
        if row.empty:
            return "?", "?"
        stock = int(row["stock_quantity"].iloc[0] or 0)
        sold = int(row["total_sales"].iloc[0] or 0)
        return str(stock), str(sold)
    except Exception as e:
        print(f"  [STOCK ERROR] Could not load product info: {e}")
        return "?", "?"


@callback(
    Output("stock-add-feedback", "children"),
    Output("stock-refresh", "data", allow_duplicate=True),
    Input("stock-add-btn", "n_clicks"),
    State("stock-product-picker", "value"),
    State("stock-total-input", "value"),
    State("stock-replenish-input", "value"),
    State("stock-threshold-input", "value"),
    State("stock-refresh", "data"),
    prevent_initial_call=True,
)
def add_product_to_stock_manager(n_clicks, product_id, total_stock,
                                  replenish, threshold, current_refresh):
    """Add a product to the stock manager."""
    if not n_clicks or not product_id or not total_stock:
        return html.Span("Please select a product and set total stock.",
                         style={"color": COLORS["text_muted"]}), no_update
    try:
        import db as _db_mod
        eng = _db_mod._get_engine()
        row = pd.read_sql(
            "SELECT name FROM products WHERE id = %(pid)s",
            eng, params={"pid": int(product_id)},
        )
        pname = row["name"].iloc[0] if not row.empty else f"Product #{product_id}"
        _db_mod.add_stock_manager(
            int(product_id), pname, int(total_stock),
            int(replenish or 20), int(threshold or 5),
        )
        return (
            html.Span(f"Added: {pname}", style={"color": COLORS["accent3"]}),
            (current_refresh or 0) + 1,
        )
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": "#e05555"}), no_update


@callback(
    Output("stock-manager-table", "children"),
    Input("stock-refresh", "data"),
    Input("url", "pathname"),
)
def render_stock_manager_table(_refresh, pathname):
    """Render the table of managed products with live WC stock."""
    if pathname != "/stock":
        return no_update
    try:
        import db as _db_mod
        df = _db_mod.load_stock_manager()
        # Fetch live stock from WooCommerce
        if not df.empty:
            pids = df["product_id"].astype(int).tolist()
            live = _db_mod.wc_get_stock_bulk(pids)
            if live:
                df["current_wc_stock"] = df["product_id"].apply(
                    lambda p: live[int(p)]["stock_quantity"] if int(p) in live else df.loc[df["product_id"] == p, "current_wc_stock"].iloc[0]
                )
                df["total_sales"] = df["product_id"].apply(
                    lambda p: live[int(p)]["total_sales"] if int(p) in live else df.loc[df["product_id"] == p, "total_sales"].iloc[0]
                )
    except Exception as e:
        print(f"  [STOCK ERROR] Could not load stock manager: {e}")
        return html.P("Could not load stock manager data.", style={"color": COLORS["text_muted"]})

    if df.empty:
        return html.P("No products added yet. Use the form above to add products.",
                       style={"color": COLORS["text_muted"], "fontSize": "14px", "padding": "20px 0"})

    th_style = {
        "textAlign": "left", "padding": "10px 12px",
        "borderBottom": f"2px solid {COLORS['card_border']}",
        "color": COLORS["text_muted"], "fontWeight": "600",
        "fontSize": "11px", "textTransform": "uppercase",
        "letterSpacing": "0.5px", "position": "sticky", "top": "0",
        "backgroundColor": COLORS["card"], "whiteSpace": "nowrap",
    }
    td_style = {
        "padding": "8px 12px", "fontSize": "13px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
    }

    headers = ["Product", "WC Stock", "Sold", "Total Stock", "Remaining", "Replenish", "Threshold", "Status", ""]
    header_row = html.Tr([html.Th(h, style=th_style) for h in headers])

    rows = []
    for _, row in df.iterrows():
        pid = int(row["product_id"])
        wc_stock = int(row["current_wc_stock"]) if pd.notna(row.get("current_wc_stock")) else 0
        sold = int(row["total_sales"]) if pd.notna(row.get("total_sales")) else 0
        total = int(row["total_stock"])
        remaining = max(0, total - sold)
        replenish_amt = int(row["replenish_amount"])
        thresh = int(row["low_threshold"])
        enabled = row["enabled"]

        # Status indicator
        if remaining <= 0:
            status_text = "SOLD OUT"
            status_color = "#e05555"
        elif wc_stock <= thresh:
            status_text = "NEEDS REPLENISH"
            status_color = "#e0a030"
        else:
            status_text = "OK"
            status_color = COLORS["accent3"]

        enabled_style = {} if enabled else {"opacity": "0.5"}

        rows.append(html.Tr(style={**enabled_style, "borderBottom": f"1px solid {COLORS['card_border']}"}, children=[
            html.Td(row["product_name"][:50], style={**td_style, "maxWidth": "250px",
                     "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            html.Td(str(wc_stock), style={**td_style, "fontWeight": "700",
                     "color": "#e05555" if wc_stock <= thresh else COLORS["text"]}),
            html.Td(str(sold), style=td_style),
            html.Td(str(total), style={**td_style, "fontWeight": "600"}),
            html.Td(str(remaining), style={**td_style,
                     "color": "#e05555" if remaining <= 0 else "#e0a030" if remaining < replenish_amt else COLORS["text"]}),
            html.Td(f"+{replenish_amt}", style=td_style),
            html.Td(f"< {thresh}", style=td_style),
            html.Td(status_text, style={**td_style, "fontWeight": "700", "color": status_color, "fontSize": "11px"}),
            html.Td(style={"padding": "4px 8px", "display": "flex", "gap": "6px"}, children=[
                html.Button(
                    "Disable" if enabled else "Enable",
                    id={"type": "stock-toggle-btn", "index": pid},
                    n_clicks=0,
                    style={
                        "background": "transparent",
                        "border": f"1px solid {COLORS['text_muted']}",
                        "color": COLORS["text_muted"], "borderRadius": "4px",
                        "cursor": "pointer", "padding": "3px 8px", "fontSize": "11px",
                    },
                ),
                html.Button(
                    "Remove",
                    id={"type": "stock-remove-btn", "index": pid},
                    n_clicks=0,
                    style={
                        "background": "transparent",
                        "border": "1px solid #e05555",
                        "color": "#e05555", "borderRadius": "4px",
                        "cursor": "pointer", "padding": "3px 8px", "fontSize": "11px",
                    },
                ),
            ]),
        ]))

    return html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[html.Thead(header_row), html.Tbody(rows)],
    )


@callback(
    Output("stock-refresh", "data", allow_duplicate=True),
    Input({"type": "stock-toggle-btn", "index": ALL}, "n_clicks"),
    State("stock-refresh", "data"),
    prevent_initial_call=True,
)
def toggle_stock_enabled(n_clicks_list, current):
    """Toggle enabled/disabled for a stock manager product."""
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update
    pid = ctx.triggered_id["index"]
    try:
        df = _get_db().load_stock_manager()
        row = df[df["product_id"] == pid]
        if not row.empty:
            current_enabled = bool(row["enabled"].iloc[0])
            _get_db().update_stock_manager(pid, enabled=not current_enabled)
    except Exception as e:
        print(f"  [WARNING] Could not toggle stock {pid}: {e}")
        return no_update
    return (current or 0) + 1


@callback(
    Output("stock-refresh", "data", allow_duplicate=True),
    Input({"type": "stock-remove-btn", "index": ALL}, "n_clicks"),
    State("stock-refresh", "data"),
    prevent_initial_call=True,
)
def remove_stock_product(n_clicks_list, current):
    """Remove a product from the stock manager."""
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update
    pid = ctx.triggered_id["index"]
    try:
        _get_db().remove_stock_manager(pid)
    except Exception as e:
        print(f"  [WARNING] Could not remove stock {pid}: {e}")
        return no_update
    return (current or 0) + 1


@callback(
    Output("replenish-result", "children"),
    Output("stock-refresh", "data", allow_duplicate=True),
    Input("replenish-btn", "n_clicks"),
    State("stock-refresh", "data"),
    prevent_initial_call=True,
)
def run_auto_replenish(n_clicks, current):
    """Manually trigger auto-replenish for all enabled products."""
    if not n_clicks:
        return no_update, no_update
    try:
        actions = _get_db().auto_replenish_stock()
        if not actions:
            return html.Div(style=card_style({"padding": "12px 18px", "borderLeft": f"3px solid {COLORS['accent']}"}), children=[
                html.Span("No products need replenishment right now.",
                           style={"color": COLORS["text_muted"], "fontSize": "13px"}),
            ]), (current or 0) + 1

        items = []
        for a in actions:
            icon = "+" if a["success"] else "x"
            color = COLORS["accent3"] if a["success"] else "#e05555"
            items.append(html.Div(style={"marginBottom": "4px"}, children=[
                html.Span(f"[{icon}] ", style={"color": color, "fontWeight": "700"}),
                html.Span(f"{a['product_name']}: ", style={"fontWeight": "600"}),
                html.Span(f"{a['old_stock']} -> {a['new_stock']} (+{a['added']})",
                           style={"color": COLORS["text_muted"]}),
            ]))

        return html.Div(style=card_style({"padding": "12px 18px", "borderLeft": f"3px solid {COLORS['accent3']}"}), children=[
            html.Span(f"Replenished {len(actions)} product(s):",
                       style={"fontWeight": "700", "marginBottom": "8px", "display": "block"}),
            *items,
        ]), (current or 0) + 1
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": "#e05555", "fontSize": "13px"}), no_update


# ============================================================
# CALLBACKS
# ============================================================

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


# --- Update categories and currency options based on tab ---
@callback(
    Output("category-filter", "options"),
    Output("category-filter", "value"),
    Output("currency-filter", "options"),
    Output("currency-filter", "value"),
    Input("event-tabs", "value"),
)
def update_filters(tab_value):
    filtered = filter_by_event_tab(hist_df, tab_value)
    if filtered.empty:
        return [], [], [], []

    # Categories (exclude generic type tags like EVENTS, LIVESTREAM, etc.)
    cats = sorted(set(
        cat
        for cats_str in filtered["category"].dropna().unique()
        for cat in parse_categories(cats_str)
        if cat not in GENERIC_CATS
    ))
    cat_options = [{"label": c, "value": c} for c in cats]

    # Currencies
    currencies = sorted(filtered["currency"].dropna().unique()) if "currency" in filtered.columns else []
    cur_options = [{"label": f"{currency_symbol(c)} ({c})", "value": c} for c in currencies]
    # Default: select all currencies
    cur_value = currencies

    return cat_options, cats, cur_options, cur_value


# --- Low Stock Inventory Alert (dynamic with archive/unarchive) ---
def _build_low_stock_table(df, archived=False):
    """Build the HTML table rows for low stock products."""
    th_style = {
        "textAlign": "left", "padding": "8px 12px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
        "color": COLORS["text_muted"], "fontWeight": "600",
        "fontSize": "11px", "textTransform": "uppercase",
        "letterSpacing": "0.5px", "position": "sticky", "top": "0",
        "backgroundColor": COLORS["card"],
    }
    cols = ["Product", "Stock", "Status", ""]
    header = html.Thead(children=[
        html.Tr([html.Th(c, style=th_style) for c in cols])
    ])

    rows = []
    for _, row in df.iterrows():
        pid = int(row["product_id"])
        btn_id = {"type": "unarchive-btn" if archived else "archive-btn", "index": pid}
        btn_label = "Unarchive" if archived else "Archive"
        btn_color = COLORS["accent"] if archived else COLORS["text_muted"]
        rows.append(html.Tr(
            style={"borderBottom": f"1px solid {COLORS['card_border']}"},
            children=[
                html.Td(row["product_name"], style={
                    "padding": "6px 12px", "color": COLORS["text"],
                    "maxWidth": "220px", "overflow": "hidden",
                    "textOverflow": "ellipsis", "whiteSpace": "nowrap",
                }),
                html.Td(
                    str(int(row["stock_quantity"])),
                    style={
                        "padding": "6px 12px", "fontWeight": "700",
                        "color": "#e05555" if row["stock_quantity"] == 0
                                else "#e0a030" if row["stock_quantity"] <= 2
                                else COLORS["text"],
                    },
                ),
                html.Td(str(row.get("status", "")), style={
                    "padding": "6px 12px", "color": COLORS["text_muted"],
                }),
                html.Td(
                    html.Button(btn_label, id=btn_id, n_clicks=0, style={
                        "background": "transparent", "border": f"1px solid {btn_color}",
                        "color": btn_color, "borderRadius": "4px", "cursor": "pointer",
                        "padding": "3px 10px", "fontSize": "11px", "whiteSpace": "nowrap",
                    }),
                    style={"padding": "4px 8px", "textAlign": "right"},
                ),
            ],
        ))
    return html.Table(
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"},
        children=[header, html.Tbody(children=rows)],
    )


@callback(
    Output("low-stock-container", "children"),
    Input("low-stock-refresh", "data"),
)
def render_low_stock(_refresh):
    """Render the low stock inventory alert panel."""
    try:
        active_df = _get_db().load_low_stock(LOW_STOCK_THRESHOLD)
        archived_df = _get_db().load_low_stock_archived(LOW_STOCK_THRESHOLD)
    except Exception:
        active_df = pd.DataFrame()
        archived_df = pd.DataFrame()

    n_active = len(active_df)
    n_archived = len(archived_df)

    children = [
        html.Div(style={"display": "flex", "justifyContent": "space-between",
                         "alignItems": "center", "marginBottom": "14px"}, children=[
            html.Div(children=[
                section_label("INVENTORY ALERT"),
                html.H3(f"Low Stock ({n_active} products)", style={
                    "margin": "0", "fontSize": "18px", "fontWeight": "700", "color": "#e05555",
                }),
            ]),
            html.Span(f"< {LOW_STOCK_THRESHOLD} units", style={
                "fontSize": "12px", "color": COLORS["text_muted"],
            }),
        ]),
    ]

    if active_df.empty and archived_df.empty:
        children.append(html.P(
            "All products have sufficient stock.",
            style={"color": COLORS["text_muted"], "fontSize": "13px"},
        ))
    else:
        if not active_df.empty:
            children.append(
                html.Div(style={"overflowX": "auto", "maxHeight": "260px", "overflowY": "auto"}, children=[
                    _build_low_stock_table(active_df, archived=False),
                ])
            )

        if n_archived > 0:
            children.append(
                html.Details(
                    style={"marginTop": "12px"},
                    children=[
                        html.Summary(
                            f"Archived ({n_archived})",
                            style={
                                "cursor": "pointer", "fontSize": "12px",
                                "color": COLORS["text_muted"], "userSelect": "none",
                            },
                        ),
                        html.Div(
                            style={"overflowX": "auto", "maxHeight": "200px",
                                   "overflowY": "auto", "marginTop": "8px",
                                   "opacity": "0.7"},
                            children=[_build_low_stock_table(archived_df, archived=True)],
                        ),
                    ],
                )
            )

    return children


@callback(
    Output("low-stock-refresh", "data", allow_duplicate=True),
    Input({"type": "archive-btn", "index": ALL}, "n_clicks"),
    State("low-stock-refresh", "data"),
    prevent_initial_call=True,
)
def handle_archive_click(n_clicks_list, current):
    """Archive a product from the low stock alert."""
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update
    pid = ctx.triggered_id["index"]
    try:
        _get_db().archive_low_stock(pid)
    except Exception as e:
        print(f"  [WARNING] Could not archive product {pid}: {e}")
        return no_update
    return (current or 0) + 1


@callback(
    Output("low-stock-refresh", "data", allow_duplicate=True),
    Input({"type": "unarchive-btn", "index": ALL}, "n_clicks"),
    State("low-stock-refresh", "data"),
    prevent_initial_call=True,
)
def handle_unarchive_click(n_clicks_list, current):
    """Unarchive a product back to the low stock alert."""
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update
    pid = ctx.triggered_id["index"]
    try:
        _get_db().unarchive_low_stock(pid)
    except Exception as e:
        print(f"  [WARNING] Could not unarchive product {pid}: {e}")
        return no_update
    return (current or 0) + 1


# --- Dynamic KPIs ---
@callback(
    Output("kpi-container", "children"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_kpis(tab_value, selected_currencies):
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    fp = filter_by_event_tab(pred_df, tab_value)

    n_products = fh["product_id"].nunique() if not fh.empty else 0
    n_sales = int(fh["quantity_sold"].sum()) if not fh.empty else 0

    # Revenue: show converted total in display currency
    sym = currency_symbol(DISPLAY_CURRENCY)
    if not fh.empty and "revenue_converted" in fh.columns:
        rev_total = fh["revenue_converted"].sum()
        rev_display = f"{sym} {rev_total:,.2f}"
        # Subtitle: show breakdown if multi-currency
        currencies = sorted(fh["currency"].dropna().unique()) if "currency" in fh.columns else []
        if len(currencies) > 1:
            breakdown = []
            for cur in currencies:
                cur_total = fh[fh["currency"] == cur]["revenue"].sum()
                if cur_total > 0:
                    breakdown.append(f"{currency_symbol(cur)}{cur_total:,.0f}")
            rev_subtitle = " + ".join(breakdown)
        else:
            rev_subtitle = ""
    elif not fh.empty:
        rev_total = fh["revenue"].sum()
        rev_display = f"{sym} {rev_total:,.2f}"
        rev_subtitle = ""
    else:
        rev_display = f"{sym} 0.00"
        rev_subtitle = ""

    n_cats = len(set(
        cat for cats_str in fh["category"].dropna().unique()
        for cat in parse_categories(cats_str)
    )) if not fh.empty else 0
    pred_total = fp["predicted_quantity"].sum() if not fp.empty else 0

    tab_labels = {"active": "Active", "past": "Past", "course": "Online Courses", "map": "All (Map)"}
    tab_label = tab_labels.get(tab_value, tab_value)

    return [
        kpi_card("Products", str(n_products), color=COLORS["accent"], subtitle=tab_label),
        kpi_card("Total Sales", f"{n_sales:,}".replace(",", "."), color=COLORS["accent3"]),
        kpi_card("Total Revenue", rev_display, color=COLORS["accent2"], subtitle=rev_subtitle),
        kpi_card("Categories", str(n_cats), color=COLORS["accent4"]),
        kpi_card("30d Forecast", f"{pred_total:,.0f} units", color=COLORS["accent4"]),
    ]


# --- Daily report ---
@callback(
    Output("daily-report", "children"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_daily_report(tab_value, selected_currencies):
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    fp = filter_by_event_tab(pred_df, tab_value)
    # Filter predictions to only show products with sales in selected currencies
    if selected_currencies and not fh.empty:
        valid_pids = set(fh["product_id"].unique())
        fp = fp[fp["product_id"].isin(valid_pids)] if not fp.empty else fp

    if fh.empty and fp.empty:
        return html.P("No products found.", style={"color": COLORS["text_muted"]})

    today = pd.Timestamp.now().normalize()

    # Group DataFrames once for O(1) per-product lookups
    hist_groups = dict(list(fh.groupby("product_id"))) if not fh.empty else {}
    pred_groups = dict(list(fp.groupby("product_id"))) if not fp.empty else {}
    all_pids = set(hist_groups) | set(pred_groups)

    # Pre-compute date range for recent sales lookup
    recent_date_range = [today - pd.Timedelta(days=7 - i) for i in range(7)]

    # Build data per product using pre-grouped data
    rows_data = []
    for pid in all_pids:
        ph = hist_groups.get(pid)
        pp = pred_groups.get(pid)

        pname = ph["product_name"].iloc[-1] if ph is not None else (pp["product_name"].iloc[0] if pp is not None else f"#{pid}")

        # Sales from last 7 days (use date-indexed series for O(1) lookup)
        recent_sales = {}
        if ph is not None:
            day_totals = ph.groupby("order_date")["quantity_sold"].sum()
            for d in recent_date_range:
                recent_sales[d] = int(day_totals.get(d, 0))

        # Forecast for next 7 days
        forecast = {}
        if pp is not None:
            pp_sorted = pp.sort_values("order_date")
            for _, row in pp_sorted.head(7).iterrows():
                forecast[row["order_date"]] = round(row["predicted_quantity"], 1)

        total_prev_7d = sum(forecast.values())
        total_recent_7d = sum(recent_sales.values())

        rows_data.append({
            "pid": pid,
            "name": pname,
            "recent_sales": recent_sales,
            "forecast": forecast,
            "total_recent_7d": total_recent_7d,
            "total_prev_7d": total_prev_7d,
        })

    # Sort by 7d forecast desc
    rows_data.sort(key=lambda x: x["total_prev_7d"], reverse=True)
    rows_data = rows_data[:50]  # Limit to 50 products

    if not rows_data:
        return html.P("No data available.", style={"color": COLORS["text_muted"]})

    # Collect dates for columns
    recent_dates = sorted(set(d for r in rows_data for d in r["recent_sales"]))
    forecast_dates = sorted(set(d for r in rows_data for d in r["forecast"]))

    # Table style
    th_style = {
        "padding": "8px 10px", "textAlign": "center", "fontSize": "10px",
        "color": COLORS["text_muted"], "textTransform": "uppercase",
        "letterSpacing": "0.3px", "fontWeight": "600",
        "borderBottom": f"2px solid {COLORS['card_border']}",
        "position": "sticky", "top": "0", "backgroundColor": COLORS["card"],
        "whiteSpace": "nowrap",
    }
    td_style = {
        "padding": "6px 10px", "fontSize": "12px", "textAlign": "center",
        "borderBottom": f"1px solid {COLORS['card_border']}",
    }

    # Header
    header_cells = [
        html.Th("Product", style={**th_style, "textAlign": "left", "minWidth": "200px"}),
    ]
    # Recent sales columns (last 7 days)
    for d in recent_dates:
        day_label = d.strftime("%m/%d")
        header_cells.append(html.Th(day_label, style={**th_style, "backgroundColor": "#16162a"}))
    header_cells.append(html.Th("Total 7d", style={**th_style, "backgroundColor": "#16162a"}))

    # Visual separator
    header_cells.append(html.Th("", style={**th_style, "width": "4px", "padding": "0",
                                            "backgroundColor": COLORS["accent"], "minWidth": "4px"}))

    # Forecast columns (next 7 days)
    for d in forecast_dates:
        day_label = d.strftime("%m/%d")
        header_cells.append(html.Th(day_label, style={**th_style, "backgroundColor": "#1e1812"}))
    header_cells.append(html.Th("Total 7d", style={**th_style, "backgroundColor": "#1e1812"}))

    # Sub-header (section labels)
    sub_cells = [html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}"})]
    for _ in recent_dates:
        sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                             "backgroundColor": "#16162a"}))
    sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                         "backgroundColor": "#16162a"}))
    sub_cells.append(html.Th("", style={**th_style, "width": "4px", "padding": "0",
                                         "backgroundColor": COLORS["accent"], "minWidth": "4px"}))
    for _ in forecast_dates:
        sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                             "backgroundColor": "#1e1812"}))
    sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                         "backgroundColor": "#1e1812"}))

    # Group title row
    n_recent = len(recent_dates) + 1  # +1 para total
    n_forecast = len(forecast_dates) + 1
    group_header = html.Tr([
        html.Th("", style={**th_style, "borderBottom": "none"}),
        html.Th("RECENT SALES", colSpan=n_recent,
                style={**th_style, "borderBottom": "none", "color": COLORS["accent"],
                       "fontSize": "11px", "backgroundColor": "#16162a", "letterSpacing": "1.5px"}),
        html.Th("", style={**th_style, "width": "4px", "padding": "0", "borderBottom": "none",
                            "backgroundColor": COLORS["accent"], "minWidth": "4px"}),
        html.Th("FORECAST", colSpan=n_forecast,
                style={**th_style, "borderBottom": "none", "color": COLORS["accent4"],
                       "fontSize": "11px", "backgroundColor": "#1e1812", "letterSpacing": "1.5px"}),
    ])

    # Rows
    body_rows = []
    for r in rows_data:
        name = r["name"]
        if len(name) > 45:
            name = name[:42] + "..."

        cells = [html.Td(name, style={**td_style, "textAlign": "left", "fontWeight": "500"})]

        # Recent sales
        for d in recent_dates:
            val = r["recent_sales"].get(d, 0)
            bg = "#16162a"
            if val > 0:
                intensity = min(val / 5, 1)
                bg = f"rgba(200, 164, 78, {0.06 + intensity * 0.18})"
            cells.append(html.Td(
                str(val) if val > 0 else "-",
                style={**td_style, "backgroundColor": bg,
                       "color": COLORS["accent"] if val > 0 else COLORS["text_muted"],
                       "fontWeight": "600" if val > 0 else "400"},
            ))

        # Recent total
        tr = r["total_recent_7d"]
        cells.append(html.Td(
            str(tr) if tr > 0 else "-",
            style={**td_style, "fontWeight": "700",
                   "color": COLORS["accent"] if tr > 0 else COLORS["text_muted"],
                   "backgroundColor": "#16162a"},
        ))

        # Separator
        cells.append(html.Td("", style={**td_style, "width": "4px", "padding": "0",
                                         "backgroundColor": COLORS["accent"], "minWidth": "4px"}))

        # Forecast
        for d in forecast_dates:
            val = r["forecast"].get(d, 0)
            bg = "#1e1812"
            if val > 0.1:
                intensity = min(val / 5, 1)
                bg = f"rgba(184, 115, 72, {0.06 + intensity * 0.18})"
            cells.append(html.Td(
                f"{val:.1f}" if val > 0.05 else "-",
                style={**td_style, "backgroundColor": bg,
                       "color": COLORS["accent4"] if val > 0.05 else COLORS["text_muted"],
                       "fontWeight": "600" if val > 0.05 else "400"},
            ))

        # Total forecast
        tp = r["total_prev_7d"]
        cells.append(html.Td(
            f"{tp:.1f}" if tp > 0.05 else "-",
            style={**td_style, "fontWeight": "700",
                   "color": COLORS["accent4"] if tp > 0.05 else COLORS["text_muted"],
                   "backgroundColor": "#1e1812"},
        ))

        body_rows.append(html.Tr(cells))

    return html.Table(
        [html.Thead([group_header, html.Tr(header_cells)]), html.Tbody(body_rows)],
        style={"width": "100%", "borderCollapse": "collapse", "tableLayout": "auto"},
    )


# --- Update product dropdown based on categories, tab, and currency ---
@callback(
    Output("product-selector", "options"),
    Output("product-selector", "value"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_product_options(selected_cats, tab_value, selected_currencies):
    if not selected_cats:
        return [], None
    # Find products that have sales in the selected currencies
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    valid_pids = set(fh["product_id"].unique()) if not fh.empty else set()
    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
    filtered = filter_by_event_tab(filtered, tab_value)
    if selected_currencies:
        filtered = filtered[filtered["product_id"].isin(valid_pids)]
    options = [
        {"label": f"{r['product_name']}  ({int(r['quantity_sold'])} sold)",
         "value": str(r["product_id"])}
        for _, r in filtered.iterrows()
    ]
    first_val = options[0]["value"] if options else None
    return options, first_val


# --- Vendas por categoria ao longo do tempo ---
@callback(
    Output("category-timeline", "figure"),
    Input("category-filter", "value"),
    Input("time-granularity", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_category_timeline(selected_cats, granularity, tab_value, selected_currencies):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Filtrar por tab e moeda
    filtered_hist = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)

    # Explodir categorias para agrupar corretamente por categoria individual
    exploded = explode_categories(filtered_hist)
    exploded = exploded[exploded["cat_single"].isin(selected_cats)]

    for i, cat in enumerate(selected_cats):
        cat_data = exploded[exploded["cat_single"] == cat]
        if cat_data.empty:
            continue
        agg = cat_data.groupby("order_date")["quantity_sold"].sum().reset_index()

        if granularity == "weekly":
            agg["week"] = agg["order_date"].dt.to_period("W").apply(lambda r: r.start_time)
            agg = agg.groupby("week")["quantity_sold"].sum().reset_index()
            x_col = "week"
        else:
            x_col = "order_date"

        color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]
        fig.add_trace(go.Scatter(
            x=agg[x_col], y=agg["quantity_sold"],
            mode="lines", name=cat,
            line=dict(color=color, width=2),
        ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="Date", yaxis_title="Quantity Sold",
        legend=H_LEGEND,
    )
    return fig


# --- Previsao diaria por categoria ---
@callback(
    Output("category-forecast", "figure"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_category_forecast(selected_cats, tab_value, selected_currencies):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Filtrar por tab e moeda
    filtered_hist = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    filtered_pred = filter_by_event_tab(pred_df, tab_value)

    # Explodir historico e previsao
    hist_exp = explode_categories(filtered_hist)
    pred_exp = explode_categories(filtered_pred)

    for i, cat in enumerate(selected_cats):
        # Historico agregado por categoria
        h = hist_exp[hist_exp["cat_single"] == cat]
        h_daily = h.groupby("order_date")["quantity_sold"].sum().reset_index()

        # Previsao agregada por categoria
        p = pred_exp[pred_exp["cat_single"] == cat]
        p_daily = p.groupby("order_date")["predicted_quantity"].sum().reset_index()

        color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]

        # Ultimos 60 dias do historico + previsao
        if not h_daily.empty:
            cutoff = h_daily["order_date"].max() - pd.Timedelta(days=60)
            h_recent = h_daily[h_daily["order_date"] >= cutoff]
            fig.add_trace(go.Scatter(
                x=h_recent["order_date"], y=h_recent["quantity_sold"],
                mode="lines", name=f"{cat} (historical)",
                line=dict(color=color, width=2),
                legendgroup=cat,
            ))

        if not p_daily.empty:
            fig.add_trace(go.Scatter(
                x=p_daily["order_date"], y=p_daily["predicted_quantity"],
                mode="lines+markers", name=f"{cat} (forecast)",
                line=dict(color=color, width=2.5, dash="dash"),
                marker=dict(size=4),
                legendgroup=cat,
            ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="Date", yaxis_title="Quantity",
        legend=H_LEGEND,
    )
    return fig


# --- Top produtos nas categorias selecionadas ---
@callback(
    Output("top-products-chart", "figure"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_top_products(selected_cats, tab_value, selected_currencies):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Filter product_sales by products that have sales in selected currencies
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    valid_pids = set(fh["product_id"].unique()) if not fh.empty else set()
    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
    filtered = filter_by_event_tab(filtered, tab_value)
    if selected_currencies:
        filtered = filtered[filtered["product_id"].isin(valid_pids)]
    filtered = filtered.head(15).iloc[::-1]

    fig.add_trace(go.Bar(
        x=filtered["quantity_sold"], y=filtered["product_name"],
        orientation="h",
        marker_color=COLORS["accent"],
        marker_line_width=0,
        texttemplate="%{x:.0f}", textposition="outside", textfont_size=11,
    ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        margin=dict(l=10, r=40, t=10, b=30),
        showlegend=False,
        yaxis_title="", xaxis_title="Quantity Sold",
    )
    return fig


# --- Previsao individual por produto ---
@callback(
    Output("product-forecast", "figure"),
    Input("product-selector", "value"),
)
def update_product_forecast(product_id):
    fig = go.Figure()
    if product_id is None:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    pid = int(product_id)

    h = hist_df[hist_df["product_id"] == pid].sort_values("order_date")
    p = pred_df[pred_df["product_id"] == pid].sort_values("order_date")

    # --- Linha REAL (gold) - todo o historico diario ---
    if not h.empty:
        h_agg = h.groupby("order_date")["quantity_sold"].sum().reset_index()

        fig.add_trace(go.Scatter(
            x=h_agg["order_date"], y=h_agg["quantity_sold"],
            mode="lines", name="actual",
            line=dict(color=COLORS["accent"], width=1.5),
        ))

    # --- Linha PREDICT (copper) + intervalo de confianca ---
    if not p.empty:
        # Conectar com o ultimo ponto do historico para continuidade visual
        if not h.empty:
            last_hist_date = h_agg["order_date"].iloc[-1]
            last_hist_val = h_agg["quantity_sold"].iloc[-1]
            bridge = pd.DataFrame({
                "order_date": [last_hist_date],
                "predicted_quantity": [float(last_hist_val)],
            })
            p_plot = pd.concat([bridge, p[["order_date", "predicted_quantity"]]], ignore_index=True)
        else:
            p_plot = p

        # Intervalo de confianca (faixa sombreada) se disponivel
        has_ci = "yhat_lower" in p.columns and "yhat_upper" in p.columns
        if has_ci:
            fig.add_trace(go.Scatter(
                x=pd.concat([p["order_date"], p["order_date"][::-1]]),
                y=pd.concat([p["yhat_upper"], p["yhat_lower"][::-1]]),
                fill="toself",
                fillcolor="rgba(184, 115, 72, 0.15)",
                line=dict(color="rgba(0,0,0,0)"),
                name="80% interval",
                showlegend=True,
                hoverinfo="skip",
            ))

        fig.add_trace(go.Scatter(
            x=p_plot["order_date"], y=p_plot["predicted_quantity"],
            mode="lines", name="forecast",
            line=dict(color=COLORS["accent4"], width=2),
        ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="", yaxis_title="",
        legend=dict(
            orientation="h", yanchor="top", y=1.08,
            xanchor="right", x=1, bgcolor="rgba(0,0,0,0)",
            font=dict(size=13),
        ),
        margin=dict(l=50, r=20, t=40, b=40),
    )
    return fig


# --- Receita mensal ---
@callback(
    Output("monthly-revenue", "figure"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_monthly_revenue(selected_cats, tab_value, selected_currencies):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_event_tab(
        filter_by_currency(filter_by_categories(hist_df, selected_cats, product_cat_map), selected_currencies),
        tab_value,
    ).assign(month=lambda d: d["order_date"].dt.to_period("M").apply(lambda r: r.start_time))

    rev_col = "revenue_converted" if "revenue_converted" in filtered.columns else "revenue"
    sym = currency_symbol(DISPLAY_CURRENCY)

    # Group by month and currency (stacked to show composition)
    currencies = sorted(filtered["currency"].dropna().unique()) if "currency" in filtered.columns else [DISPLAY_CURRENCY]
    multi_currency = len(currencies) > 1

    bar_colors = [COLORS["accent3"], COLORS["accent"], COLORS["accent4"],
                  COLORS["accent2"], "#7b8de0", "#e06070"]

    for i, cur in enumerate(currencies):
        cur_data = filtered[filtered["currency"] == cur] if "currency" in filtered.columns else filtered
        monthly = cur_data.groupby("month")[rev_col].sum().reset_index()
        if monthly.empty:
            continue
        cur_sym = currency_symbol(cur)
        fig.add_trace(go.Bar(
            x=monthly["month"], y=monthly[rev_col],
            name=f"from {cur_sym} ({cur})" if multi_currency else "Revenue",
            marker_color=bar_colors[i % len(bar_colors)],
            marker_line_width=0, opacity=0.85,
        ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title=f"Revenue ({sym})",
        showlegend=multi_currency,
        barmode="stack",
    )
    return fig


# --- Vendas por dia da semana ---
@callback(
    Output("weekday-chart", "figure"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_weekday_chart(selected_cats, tab_value, selected_currencies):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    filtered = filter_by_event_tab(
        filter_by_currency(filter_by_categories(hist_df, selected_cats, product_cat_map), selected_currencies),
        tab_value,
    ).assign(weekday=lambda d: d["order_date"].dt.dayofweek)
    wd = filtered.groupby("weekday")["quantity_sold"].sum().reset_index()
    wd["weekday_name"] = wd["weekday"].map(lambda x: weekday_names[x])

    colors = [COLORS["accent3"] if x >= 5 else COLORS["accent"] for x in wd["weekday"]]

    fig.add_trace(go.Bar(
        x=wd["weekday_name"], y=wd["quantity_sold"],
        marker_color=colors, marker_line_width=0,
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(xaxis_title="", yaxis_title="Quantity", showlegend=False)
    return fig


# --- Vendas por hora do dia ---
@callback(
    Output("hourly-chart", "figure"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_hourly_chart(selected_cats, tab_value, selected_currencies):
    fig = go.Figure()
    _hourly_df = get_hourly_df()
    if not selected_cats or _hourly_df.empty:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_currency(
        filter_by_categories(_hourly_df, selected_cats, product_cat_map),
        selected_currencies,
    )
    filtered = filter_by_event_tab(filtered, tab_value)

    if filtered.empty:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    hr = filtered.groupby("hour")["quantity_sold"].sum().reset_index()
    hr = hr.sort_values("hour")

    # Identify top 3 hours for highlighting
    top3 = set(hr.nlargest(3, "quantity_sold")["hour"].tolist())
    colors = [COLORS["accent3"] if h in top3 else COLORS["accent"] for h in hr["hour"]]

    hr["label"] = hr["hour"].apply(lambda h: f"{h:02d}:00")

    fig.add_trace(go.Bar(
        x=hr["label"], y=hr["quantity_sold"],
        marker_color=colors, marker_line_width=0,
        hovertemplate="<b>%{x}</b><br>Qty: %{y}<extra></extra>",
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="Hour of Day",
        yaxis_title="Quantity",
        showlegend=False,
        xaxis=dict(dtick=1),
    )
    return fig


# --- Tabela de metricas ---
@callback(
    Output("metrics-table", "children"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
)
def update_metrics_table(selected_cats, tab_value, selected_currencies):
    if not selected_cats:
        return html.P("Select at least one category.", style={"color": COLORS["text_muted"]})

    # Filtrar metricas por categorias (multi-categoria)
    filtered_metrics = filter_by_categories(metrics_df, selected_cats, product_cat_map)
    filtered_metrics = filter_by_event_tab(filtered_metrics, tab_value)
    # Filter by currency: keep only products that have sales in selected currencies
    if selected_currencies:
        fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
        valid_pids = set(fh["product_id"].unique()) if not fh.empty else set()
        filtered_metrics = filtered_metrics[filtered_metrics["product_id"].isin(valid_pids)]

    if filtered_metrics.empty:
        return html.P("No products found in selected categories.", style={"color": COLORS["text_muted"]})

    # Resumo de metricas (agrupar por product_id para evitar duplicatas)
    agg_dict = {
        "product_name": ("product_name", "first"),
        "category": ("category", "first"),
        "mae": ("mae", "mean"),
        "rmse": ("rmse", "mean"),
        "r2_score": ("r2_score", "max"),
    }
    if "method" in filtered_metrics.columns:
        agg_dict["method"] = ("method", "first")

    ms = (
        filtered_metrics.groupby("product_id")
        .agg(**agg_dict)
        .reset_index()
    )

    # Juntar com previsao total
    filtered_pred = filter_by_categories(pred_df, selected_cats, product_cat_map)
    filtered_pred = filter_by_event_tab(filtered_pred, tab_value)
    pred_summary = (
        filtered_pred
        .groupby("product_id")
        .agg(total_prev=("predicted_quantity", "sum"), media_dia=("predicted_quantity", "mean"))
        .reset_index()
    )

    table_df = ms.merge(pred_summary, on="product_id", how="left").fillna(0)
    table_df = table_df.sort_values("total_prev", ascending=False).head(40)

    has_method = "method" in table_df.columns

    header_style = {
        "padding": "10px 14px", "textAlign": "left", "fontSize": "11px",
        "color": COLORS["text_muted"], "textTransform": "uppercase",
        "letterSpacing": "0.5px", "fontWeight": "600",
        "borderBottom": f"2px solid {COLORS['card_border']}",
        "position": "sticky", "top": "0", "backgroundColor": COLORS["card"],
    }
    cell_style = {
        "padding": "8px 14px", "fontSize": "13px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
    }

    def r2_color(val):
        if val >= 0.5:
            return COLORS["accent3"]
        if val >= 0:
            return COLORS["accent"]
        return COLORS["red"]

    def method_label(val):
        labels = {"gradient_boosting": "ML", "weighted_average": "Avg"}
        return labels.get(str(val), str(val)[:10])

    header_cells = [
        html.Th("Product", style=header_style),
        html.Th("Categories", style=header_style),
        html.Th("MAE", style={**header_style, "textAlign": "right"}),
        html.Th("RMSE", style={**header_style, "textAlign": "right"}),
        html.Th("R2", style={**header_style, "textAlign": "right"}),
        html.Th("Fcst. 30d", style={**header_style, "textAlign": "right"}),
        html.Th("Avg/Day", style={**header_style, "textAlign": "right"}),
    ]
    if has_method:
        header_cells.append(html.Th("Method", style={**header_style, "textAlign": "center"}))
    header = html.Tr(header_cells)

    rows = []
    for _, row in table_df.iterrows():
        name = row["product_name"]
        if len(name) > 50:
            name = name[:47] + "..."
        # Mostrar categorias de forma legivel (pipe -> virgula)
        cat_display = str(row["category"]).replace("|", ", ")
        if len(cat_display) > 40:
            cat_display = cat_display[:37] + "..."

        row_cells = [
            html.Td(name, style=cell_style),
            html.Td(cat_display, style={**cell_style, "color": COLORS["accent3"], "fontSize": "12px"}),
            html.Td(f"{row['mae']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(f"{row['rmse']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(f"{row['r2_score']:.3f}", style={**cell_style, "textAlign": "right", "color": r2_color(row["r2_score"]), "fontWeight": "600"}),
            html.Td(f"{row['total_prev']:.1f}", style={**cell_style, "textAlign": "right", "color": COLORS["accent4"], "fontWeight": "600"}),
            html.Td(f"{row['media_dia']:.2f}", style={**cell_style, "textAlign": "right"}),
        ]
        if has_method:
            m = method_label(row.get("method", ""))
            m_color = COLORS["accent3"] if m == "ML" else COLORS["accent"]
            row_cells.append(html.Td(m, style={**cell_style, "textAlign": "center", "color": m_color, "fontWeight": "600", "fontSize": "11px"}))

        rows.append(html.Tr(row_cells))

    return html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )


# ============================================================
# AI CHAT CALLBACKS
# ============================================================

def _make_message_bubble(role, content):
    """Create a styled chat message bubble."""
    is_user = role == "user"
    return html.Div(
        style={
            "display": "flex", "gap": "10px", "alignItems": "flex-start",
            "marginBottom": "16px",
            "flexDirection": "row-reverse" if is_user else "row",
        },
        children=[
            # Avatar
            html.Div(
                "You" if is_user else "AI",
                style={
                    "backgroundColor": COLORS["accent3"] if is_user else COLORS["accent"],
                    "color": COLORS["bg"], "borderRadius": "50%",
                    "width": "30px", "height": "30px",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "fontSize": "10px", "fontWeight": "700", "flexShrink": "0",
                },
            ),
            # Message content
            html.Div(
                style={
                    "backgroundColor": "rgba(90,170,136,0.08)" if is_user else "transparent",
                    "borderRadius": "10px", "padding": "10px 14px" if is_user else "0",
                    "maxWidth": "85%",
                },
                children=[
                    dcc.Markdown(
                        content,
                        style={
                            "color": COLORS["text"], "fontSize": "13px",
                            "margin": "0", "lineHeight": "1.7",
                        },
                    ),
                ],
            ),
        ],
    )


@callback(
    Output("chat-display", "children"),
    Output("chat-history", "data"),
    Output("chat-input", "value"),
    Input("chat-send", "n_clicks"),
    Input("chat-input", "n_submit"),
    Input("quick-daily", "n_clicks"),
    Input("quick-weekly", "n_clicks"),
    Input("quick-top", "n_clicks"),
    Input("quick-forecast", "n_clicks"),
    Input("chat-clear", "n_clicks"),
    State("chat-input", "value"),
    State("chat-history", "data"),
    prevent_initial_call=True,
)
def handle_chat(send_clicks, n_submit, daily_clicks, weekly_clicks,
                top_clicks, forecast_clicks, clear_clicks,
                input_value, chat_history):
    from dash import ctx

    # Determine which input triggered
    triggered_id = ctx.triggered_id

    # --- Clear chat ---
    if triggered_id == "chat-clear":
        welcome = html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "flex-start"},
            children=[
                html.Div("AI", style={
                    "backgroundColor": COLORS["accent"], "color": COLORS["bg"],
                    "borderRadius": "50%", "width": "30px", "height": "30px",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "fontSize": "11px", "fontWeight": "700", "flexShrink": "0",
                }),
                dcc.Markdown(
                    "Chat cleared. How can I help you?",
                    style={"color": COLORS["text"], "fontSize": "13px",
                           "margin": "0", "lineHeight": "1.6", "flex": "1"},
                ),
            ],
        )
        return [welcome], [], ""

    # --- Determine question ---
    question = None
    quick_label = None

    if triggered_id in ("chat-send", "chat-input"):
        question = (input_value or "").strip()
        if not question:
            return no_update, no_update, no_update
    elif triggered_id == "quick-daily":
        question = ai_agent.QUICK_ACTIONS["daily_report"]
        quick_label = "Daily Report"
    elif triggered_id == "quick-weekly":
        question = ai_agent.QUICK_ACTIONS["weekly_summary"]
        quick_label = "Weekly Summary"
    elif triggered_id == "quick-top":
        question = ai_agent.QUICK_ACTIONS["top_products"]
        quick_label = "Top Products"
    elif triggered_id == "quick-forecast":
        question = ai_agent.QUICK_ACTIONS["forecast_analysis"]
        quick_label = "Forecast Analysis"

    if not question:
        return no_update, no_update, no_update

    # Display text for the user message bubble
    display_question = quick_label if quick_label else question

    # --- Call AI agent ---
    try:
        response = ai_agent.chat(question, hist_df, pred_df, metrics_df, chat_history)
    except Exception as e:
        response = f"**Error:** {str(e)}"

    # Update history
    new_history = list(chat_history or [])
    new_history.append({"role": "user", "content": question})
    new_history.append({"role": "assistant", "content": response})

    # Build all message bubbles
    bubbles = []
    for msg in new_history:
        display_text = msg["content"]
        # For quick actions in history, show short label if it matches
        if msg["role"] == "user":
            for key, val in ai_agent.QUICK_ACTIONS.items():
                if msg["content"] == val:
                    labels = {"daily_report": "Daily Report", "weekly_summary": "Weekly Summary",
                              "top_products": "Top Products", "forecast_analysis": "Forecast Analysis"}
                    display_text = f"Generate: **{labels.get(key, key)}**"
                    break
        bubbles.append(_make_message_bubble(msg["role"], display_text))

    return bubbles, new_history, ""


# ============================================================
# SALES SOURCES CHART
# ============================================================

@callback(
    Output("source-chart", "figure"),
    Input("event-tabs", "value"),
)
def update_source_chart(_tab):
    """Render horizontal bar chart of sales by acquisition source."""
    fig = go.Figure()
    _source_df = get_source_df()
    if _source_df.empty:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Normalize & map labels
    _SOURCE_LABELS = {
        "typein": "Direct", "direct": "Direct",
        "organic": "Organic Search", "referral": "Referral",
        "utm": "Paid / UTM", "admin": "Admin / Manual",
        "facebook": "Facebook", "fb": "Facebook",
        "instagram": "Instagram", "ig": "Instagram",
        "google": "Google Ads", "adwords": "Google Ads",
        "ppc,adwords": "Google Ads",
        "hs_email": "HubSpot Email", "hs_automation": "HubSpot Auto",
        "telegram": "Telegram", "twitter": "Twitter / X",
        "linkedin": "LinkedIn", "youtube": "YouTube",
        "tiktok": "TikTok", "affiliation": "Affiliate",
        "speaker_website": "Speaker Website",
    }
    df = _source_df.assign(
        label=_source_df["source"].apply(
            lambda s: _SOURCE_LABELS.get(str(s).lower().strip(), str(s).strip().title())
        )
    )

    # Merge rows with same label (e.g. "fb" + "facebook" → "Facebook")
    df = df.groupby("label").agg(
        quantity_sold=("quantity_sold", "sum"),
        revenue=("revenue", "sum"),
        order_count=("order_count", "sum"),
    ).reset_index().sort_values("quantity_sold", ascending=False)

    # Keep top 10, group the rest into "Other"
    if len(df) > 10:
        top = df.head(10)
        rest = df.iloc[10:]
        other = pd.DataFrame([{
            "label": "Other",
            "quantity_sold": rest["quantity_sold"].sum(),
            "revenue": rest["revenue"].sum(),
            "order_count": rest["order_count"].sum(),
        }])
        df = pd.concat([top, other], ignore_index=True)

    # Calculate percentages
    total = df["quantity_sold"].sum()
    df["pct"] = (df["quantity_sold"] / total * 100).round(1)

    # Sort ascending for horizontal bar (top source at top)
    df = df.sort_values("quantity_sold", ascending=True)

    # Color palette
    _SRC_COLORS = {
        "Direct": COLORS["accent"], "Facebook": "#4267B2", "Instagram": "#E1306C",
        "Google Ads": "#34A853", "Organic Search": COLORS["accent3"],
        "HubSpot Email": "#FF7A59", "HubSpot Auto": "#FF5C35",
        "Referral": "#6ea8d9", "Telegram": "#0088cc",
        "Speaker Website": COLORS["accent4"], "Affiliate": "#a67ed6",
    }
    colors = [_SRC_COLORS.get(lbl, COLORS["text_muted"]) for lbl in df["label"]]

    fig.add_trace(go.Bar(
        y=df["label"],
        x=df["quantity_sold"],
        orientation="h",
        marker_color=colors,
        marker_line_width=0,
        text=df.apply(lambda r: f"{int(r['quantity_sold']):,}  ({r['pct']}%)", axis=1),
        textposition="auto",
        textfont=dict(size=11, color="#fff"),
        hovertemplate="<b>%{y}</b><br>Units: %{x:,}<br>Orders: %{customdata[0]:,}<br>Revenue: $%{customdata[1]:,.0f}<extra></extra>",
        customdata=df[["order_count", "revenue"]].values,
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        showlegend=False,
        margin=dict(l=120, r=20, t=10, b=10),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showline=False),
        bargap=0.25,
    )
    return fig


# ============================================================
# SALES MAP
# ============================================================

@callback(
    Output("map-section", "style"),
    Input("event-tabs", "value"),
)
def toggle_map_section(tab_value):
    """Show map section only when the map tab is active."""
    if tab_value == "map":
        return {"display": "block", "marginTop": "24px"}
    return {"display": "none"}


@callback(
    Output("map-category-filter", "options"),
    Input("event-tabs", "value"),
)
def update_map_cat_options(tab_value):
    """Populate category filter for the map."""
    _geo_df = get_geo_sales_df()
    if tab_value != "map" or _geo_df.empty:
        return []
    all_cats = sorted(set(
        cat
        for cats_str in _geo_df["category"].dropna().unique()
        for cat in parse_categories(cats_str)
        if cat not in GENERIC_CATS
    ))
    return [{"label": c, "value": c} for c in all_cats]


@callback(
    Output("map-product-filter", "options"),
    Output("map-product-filter", "value"),
    Input("event-tabs", "value"),
    Input("map-category-filter", "value"),
)
def update_map_product_options(tab_value, selected_map_cats):
    """Populate product filter, filtered by selected categories."""
    _geo_df = get_geo_sales_df()
    if tab_value != "map" or _geo_df.empty:
        return [], []

    df = _geo_df
    if selected_map_cats:
        geo_cat_map = build_product_cat_map(_geo_df)
        df = filter_by_categories(_geo_df, selected_map_cats, geo_cat_map)

    products = (
        df.groupby(["product_id", "product_name"])["quantity_sold"]
        .sum().reset_index()
        .sort_values("quantity_sold", ascending=False)
    )
    prod_opts = [
        {"label": f"{r['product_name']} ({int(r['quantity_sold'])} sold)", "value": int(r["product_id"])}
        for _, r in products.iterrows()
    ]
    return prod_opts, []


@callback(
    Output("sales-map", "figure"),
    Input("event-tabs", "value"),
    Input("map-category-filter", "value"),
    Input("map-product-filter", "value"),
)
def update_sales_map(tab_value, selected_map_cats, selected_products):
    """Render interactive Mapbox map with sales locations."""
    fig = go.Figure()

    # Mapbox-compatible layout (no xaxis/yaxis/hovermode)
    _map_base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=30, lon=0),
            zoom=1,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
        showlegend=False,
    )

    _geo_df = get_geo_sales_df()
    if tab_value != "map" or _geo_df.empty:
        fig.update_layout(**_map_base)
        return fig

    df = _geo_df

    # Filter by selected categories (multi-category aware)
    if selected_map_cats:
        geo_cat_map = build_product_cat_map(_geo_df)
        df = filter_by_categories(df, selected_map_cats, geo_cat_map)

    # Filter by selected products if any
    if selected_products:
        df = df[df["product_id"].isin(selected_products)]

    if df.empty:
        fig.update_layout(**_map_base)
        return fig

    # Expand rows: one row per (location, individual category) for proper coloring
    rows_expanded = []
    for _, row in df.iterrows():
        cats = parse_categories(row.get("category", ""))
        # If filtering by cats, only keep matching ones for the label
        if selected_map_cats:
            cats = [c for c in cats if c in selected_map_cats] or cats[:1]
        for cat in cats:
            rows_expanded.append({**row.to_dict(), "_cat": cat})
    exp_df = pd.DataFrame(rows_expanded)

    # Aggregate by location + individual category
    agg = (
        exp_df.groupby(["country", "state", "city", "lat", "lng", "_cat"])
        .agg(quantity_sold=("quantity_sold", "sum"), revenue=("revenue", "sum"),
             category=("category", "first"))
        .reset_index()
    )
    agg["cat_label"] = agg["_cat"]

    # Get unique categories for consistent coloring
    unique_cats = sorted(agg["cat_label"].unique())
    _PALETTE = [
        "#c8a44e", "#5aaa88", "#b87348", "#6ea8d9", "#e05555",
        "#a67ed6", "#e0a030", "#4ecdc4", "#ff6b6b", "#95e667",
        "#ff9ff3", "#54a0ff", "#feca57", "#ff9f43", "#00d2d3",
    ]
    cat_colors = {cat: _PALETTE[i % len(_PALETTE)] for i, cat in enumerate(unique_cats)}

    for cat in unique_cats:
        cat_data = agg[agg["cat_label"] == cat]
        fig.add_trace(go.Scattermapbox(
            lat=cat_data["lat"],
            lon=cat_data["lng"],
            text=cat_data.apply(
                lambda r: (
                    f"<b>{r['city']}, {r['state']}</b> ({r['country']})<br>"
                    f"Category: {r['category']}<br>"
                    f"Qty: {int(r['quantity_sold'])}<br>"
                    f"Revenue: {r['revenue']:,.2f}"
                ), axis=1,
            ),
            hoverinfo="text",
            marker=dict(
                size=cat_data["quantity_sold"].clip(lower=5).apply(lambda x: min(x * 0.7 + 5, 35)),
                color=cat_colors[cat],
                opacity=0.75,
                sizemode="diameter",
            ),
            name=cat[:30],
        ))

    # Auto-center on the data
    center_lat = agg["lat"].mean()
    center_lon = agg["lng"].mean()

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        showlegend=True,
        legend=dict(
            font=dict(size=11, color=COLORS["text"]),
            bgcolor="rgba(20,18,30,0.8)",
            bordercolor=COLORS["card_border"],
            borderwidth=1,
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="left", x=0,
        ),
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=1.5,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
    )

    return fig


# ============================================================
# ALL ORDERS TABLE
# ============================================================

@callback(
    Output("orders-page", "data"),
    Input("orders-search", "value"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
    Input("orders-page-size", "value"),
)
def reset_orders_page(*_):
    """Reset to page 1 when any filter changes."""
    return 1


@callback(
    Output("orders-table", "children"),
    Output("orders-count", "children"),
    Output("orders-pagination", "children"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
    Input("currency-filter", "value"),
    Input("orders-search", "value"),
    Input("orders-page-size", "value"),
    Input("orders-page", "data"),
)
def update_orders_table(selected_cats, tab_value, selected_currencies, search_text, page_size, current_page):
    """Render the orders table with filters and search."""
    if all_orders_df.empty:
        return (
            html.P("No orders loaded.", style={"color": COLORS["text_muted"], "fontSize": "13px"}),
            "0 orders",
            [],
        )

    df = all_orders_df

    # Filter by event tab
    if tab_value not in ("map",):
        pids = {pid for pid, st in event_status_map.items() if st == tab_value}
        df = df[df["product_id"].isin(pids)]

    # Filter by categories
    if selected_cats:
        df = filter_by_categories(df, selected_cats, orders_cat_map)

    # Filter by currency
    df = filter_by_currency(df, selected_currencies)

    # Search filter
    if search_text and search_text.strip():
        q = search_text.strip().lower()
        mask = (
            df["order_id"].astype(str).str.contains(q, case=False, na=False)
            | df["product_name"].astype(str).str.lower().str.contains(q, na=False)
            | df["billing_country"].astype(str).str.lower().str.contains(q, na=False)
            | df["billing_city"].astype(str).str.lower().str.contains(q, na=False)
            | df["order_status"].astype(str).str.lower().str.contains(q, na=False)
            | df["order_source"].astype(str).str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    total_rows = len(df)
    page_size = page_size or 50
    total_pages = max(1, -(-total_rows // page_size))  # ceil division
    current_page = min(max(1, current_page or 1), total_pages)

    # Paginate
    start = (current_page - 1) * page_size
    end = start + page_size
    page_df = df.iloc[start:end]

    # Count text
    count_text = f"Showing {start + 1}-{min(end, total_rows)} of {total_rows:,} orders"

    # Build table
    columns = [
        ("Order #", "order_id"),
        ("Date", "order_date"),
        ("Product", "product_name"),
        ("Qty", "quantity"),
        ("Total", "total"),
        ("Currency", "currency"),
        ("Status", "order_status"),
        ("Country", "billing_country"),
        ("City", "billing_city"),
        ("Source", "order_source"),
    ]

    th_style = {
        "textAlign": "left", "padding": "10px 12px",
        "borderBottom": f"2px solid {COLORS['card_border']}",
        "color": COLORS["text_muted"], "fontWeight": "600",
        "fontSize": "11px", "textTransform": "uppercase",
        "letterSpacing": "0.5px", "position": "sticky", "top": "0",
        "backgroundColor": COLORS["card"], "whiteSpace": "nowrap",
    }

    td_style = {
        "padding": "8px 12px", "fontSize": "13px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
        "whiteSpace": "nowrap",
    }

    status_colors = {
        "completed": COLORS["accent3"],
        "processing": COLORS["accent"],
        "on-hold": COLORS["accent4"],
        "cancelled": COLORS["red"],
        "refunded": COLORS["red"],
        "failed": COLORS["red"],
    }

    rows = []
    for _, r in page_df.iterrows():
        st_color = status_colors.get(str(r.get("order_status", "")).lower(), COLORS["text_muted"])
        date_str = r["order_date"].strftime("%Y-%m-%d") if pd.notna(r["order_date"]) else ""
        total_val = f"{currency_symbol(r['currency'])}{float(r['total']):,.2f}" if pd.notna(r["total"]) else ""

        rows.append(html.Tr(children=[
            html.Td(f"#{int(r['order_id'])}", style={**td_style, "color": COLORS["accent"], "fontWeight": "600"}),
            html.Td(date_str, style=td_style),
            html.Td(str(r.get("product_name", "")), style={
                **td_style, "maxWidth": "280px", "overflow": "hidden",
                "textOverflow": "ellipsis",
            }),
            html.Td(str(int(r["quantity"])) if pd.notna(r["quantity"]) else "", style={**td_style, "textAlign": "center"}),
            html.Td(total_val, style={**td_style, "fontWeight": "500"}),
            html.Td(str(r.get("currency", "")), style={**td_style, "textAlign": "center"}),
            html.Td(
                str(r.get("order_status", "")),
                style={**td_style, "color": st_color, "fontWeight": "500"},
            ),
            html.Td(str(r.get("billing_country", "") or ""), style=td_style),
            html.Td(str(r.get("billing_city", "") or ""), style=td_style),
            html.Td(str(r.get("order_source", "") or ""), style={**td_style, "color": COLORS["text_muted"]}),
        ]))

    table = html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[
            html.Thead(children=[
                html.Tr([html.Th(col_label, style=th_style) for col_label, _ in columns])
            ]),
            html.Tbody(children=rows),
        ],
    )

    # Pagination buttons
    pagination = []
    if total_pages > 1:
        btn_style_base = {
            "backgroundColor": COLORS["bg"],
            "color": COLORS["text_muted"],
            "border": f"1px solid {COLORS['card_border']}",
            "borderRadius": "6px", "padding": "6px 12px",
            "fontSize": "12px", "cursor": "pointer",
            "fontFamily": FONT,
        }
        btn_style_active = {
            **btn_style_base,
            "backgroundColor": COLORS["accent"],
            "color": COLORS["bg"],
            "fontWeight": "700",
            "border": f"1px solid {COLORS['accent']}",
        }

        # Previous
        if current_page > 1:
            pagination.append(
                html.Button("Prev", id={"type": "orders-page-btn", "page": current_page - 1},
                            n_clicks=0, style=btn_style_base)
            )

        # Page numbers (show max 7 pages around current)
        start_p = max(1, current_page - 3)
        end_p = min(total_pages, current_page + 3)
        if start_p > 1:
            pagination.append(
                html.Button("1", id={"type": "orders-page-btn", "page": 1},
                            n_clicks=0, style=btn_style_base)
            )
            if start_p > 2:
                pagination.append(html.Span("...", style={"color": COLORS["text_muted"], "padding": "0 4px"}))

        for p in range(start_p, end_p + 1):
            style = btn_style_active if p == current_page else btn_style_base
            pagination.append(
                html.Button(str(p), id={"type": "orders-page-btn", "page": p},
                            n_clicks=0, style=style)
            )

        if end_p < total_pages:
            if end_p < total_pages - 1:
                pagination.append(html.Span("...", style={"color": COLORS["text_muted"], "padding": "0 4px"}))
            pagination.append(
                html.Button(str(total_pages), id={"type": "orders-page-btn", "page": total_pages},
                            n_clicks=0, style=btn_style_base)
            )

        # Next
        if current_page < total_pages:
            pagination.append(
                html.Button("Next", id={"type": "orders-page-btn", "page": current_page + 1},
                            n_clicks=0, style=btn_style_base)
            )

    return table, count_text, pagination


@callback(
    Output("orders-page", "data", allow_duplicate=True),
    Input({"type": "orders-page-btn", "page": dash.ALL}, "n_clicks"),
    State({"type": "orders-page-btn", "page": dash.ALL}, "id"),
    prevent_initial_call=True,
)
def handle_orders_pagination(n_clicks_list, ids):
    """Navigate to the clicked page."""
    if not n_clicks_list or not any(n_clicks_list):
        return no_update
    # Find which button was clicked
    ctx = dash.callback_context
    if not ctx.triggered:
        return no_update
    triggered_id = ctx.triggered[0]["prop_id"]
    # Extract page number from the pattern-matching id
    try:
        id_dict = json.loads(triggered_id.split(".")[0])
        return id_dict["page"]
    except Exception:
        return no_update


# ============================================================
# SYNC & RETRAIN (background thread + real-time log)
# ============================================================

import threading
import tempfile

_SYNC_LOG_FILE = os.path.join(tempfile.gettempdir(), "tcche_sync.log")
_sync_lock = threading.Lock()
_sync_state = {"running": False, "exit_code": None}


def _run_sync_thread():
    """Run main.py in a background thread, streaming output to a log file."""
    main_py = str(DATA_DIR / "main.py")
    _sync_state["running"] = True
    _sync_state["exit_code"] = None

    try:
        with open(_SYNC_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("[Starting sync...]\n")
            f.flush()

            env = os.environ.copy()
            env.setdefault("MPLCONFIGDIR", os.path.join(env.get("TMPDIR", "/tmp"), "matplotlib"))
            env.setdefault("MPLBACKEND", "Agg")

            proc = subprocess.Popen(
                [sys.executable, "-u", main_py],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(DATA_DIR),
                bufsize=1,
                env=env,
            )

            for line in proc.stdout:
                f.write(line)
                f.flush()

            proc.wait(timeout=600)
            _sync_state["exit_code"] = proc.returncode

            if proc.returncode == 0:
                f.write("\n[Sync completed successfully!]\n")
            else:
                f.write(f"\n[Sync failed with exit code {proc.returncode}]\n")
            f.flush()

    except subprocess.TimeoutExpired:
        proc.kill()
        _sync_state["exit_code"] = -1
        with open(_SYNC_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n[ERROR: Sync timed out after 10 minutes]\n")
    except Exception as e:
        _sync_state["exit_code"] = -1
        with open(_SYNC_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n[ERROR: {e}]\n")
    finally:
        _sync_state["running"] = False


@callback(
    Output("sync-btn", "disabled"),
    Output("sync-status", "children"),
    Output("sync-running", "data"),
    Output("sync-poll", "disabled"),
    Output("sync-log-panel", "style"),
    Input("sync-btn", "n_clicks"),
    prevent_initial_call=True,
)
def start_sync(n_clicks):
    """Start background sync when button is clicked."""
    if not n_clicks:
        return no_update, no_update, no_update, no_update, no_update

    if _sync_state["running"]:
        return True, "Sync already running...", True, False, {"display": "block"}

    with open(_SYNC_LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")

    thread = threading.Thread(target=_run_sync_thread, daemon=True)
    thread.start()

    return (
        True,
        "Syncing...",
        True,
        False,
        {"display": "block"},
    )


@callback(
    Output("sync-log", "children"),
    Output("sync-step", "children"),
    Output("sync-btn", "disabled", allow_duplicate=True),
    Output("sync-status", "children", allow_duplicate=True),
    Output("sync-poll", "disabled", allow_duplicate=True),
    Output("sync-trigger", "data"),
    Output("sync-log-panel", "style", allow_duplicate=True),
    Input("sync-poll", "n_intervals"),
    State("sync-running", "data"),
    prevent_initial_call=True,
)
def poll_sync_progress(n_intervals, is_running):
    """Poll sync log file and update the UI."""
    log_text = ""
    try:
        with open(_SYNC_LOG_FILE, "r", encoding="utf-8") as f:
            log_text = f.read()
    except FileNotFoundError:
        pass

    last_lines = log_text.strip().split("\n")
    last_line = last_lines[-1] if last_lines else ""

    step = ""
    if "[*]" in last_line:
        step = last_line.strip().lstrip("[*]").strip()
    elif "Pagina" in last_line or "pagina" in last_line:
        step = "Fetching data from WooCommerce..."
    elif "Prophet" in last_line or "Media" in last_line:
        step = "Training models..."
    elif "[OK]" in last_line:
        step = last_line.strip()

    if not _sync_state["running"] and _sync_state["exit_code"] is not None:
        exit_code = _sync_state["exit_code"]
        if exit_code == 0:
            reload_all_data()
            return (
                log_text, "Done!", False,
                "Sync complete! Reloading...",
                True, "reload",
                {"display": "block"},
            )
        else:
            err_lines = [l for l in last_lines if "Error" in l or "ERRO" in l or "ERROR" in l]
            err_msg = err_lines[-1].strip() if err_lines else f"Failed (exit code {exit_code})"
            return (
                log_text, "Failed", False,
                f"Error: {err_msg}",
                True, no_update,
                {"display": "block"},
            )

    return (
        log_text, step, no_update,
        no_update, no_update, no_update,
        no_update,
    )


@callback(
    Output("page-reload", "href"),
    Input("sync-trigger", "data"),
    prevent_initial_call=True,
)
def reload_after_sync(trigger):
    """Reload the page after successful sync to pick up fresh data."""
    if trigger == "reload":
        return "/"
    return no_update


# ============================================================
# REPORT CALLBACKS
# ============================================================

def _build_report_charts(selected_cats, tab_value, selected_currencies, product_id):
    """Build all report chart figures and statistical summary."""
    from datetime import datetime

    charts = []
    analysis_lines = []
    sym = currency_symbol(DISPLAY_CURRENCY)

    # --- Filter base data ---
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    fp = filter_by_event_tab(pred_df, tab_value)
    fm = filter_by_event_tab(metrics_df, tab_value)

    if selected_cats:
        fh = filter_by_categories(fh, selected_cats, product_cat_map)
        fp = filter_by_categories(fp, selected_cats, product_cat_map)
        fm = filter_by_categories(fm, selected_cats, product_cat_map)

    if selected_currencies and "currency" not in fh.columns:
        pass
    elif selected_currencies:
        valid_pids = set(fh["product_id"].unique()) if not fh.empty else set()
        fp = fp[fp["product_id"].isin(valid_pids)]
        fm = fm[fm["product_id"].isin(valid_pids)]

    # --- KPI summary ---
    n_products = fh["product_id"].nunique() if not fh.empty else 0
    total_qty = int(fh["quantity_sold"].sum()) if not fh.empty else 0
    rev_col = "revenue_converted" if "revenue_converted" in fh.columns else "revenue"
    total_rev = fh[rev_col].sum() if not fh.empty else 0
    forecast_qty = fp["predicted_quantity"].sum() if not fp.empty else 0

    cats_label = ", ".join(selected_cats) if selected_cats else "All"
    tab_labels = {"active": "Active Events", "past": "Past Events", "course": "Online Courses", "map": "All"}
    tab_label = tab_labels.get(tab_value, "All")

    analysis_lines.append(f"Report generated on {datetime.now().strftime('%B %d, %Y at %H:%M')}")
    analysis_lines.append(f"Scope: {tab_label} | Categories: {cats_label}")
    analysis_lines.append(f"Currency filter: {', '.join(selected_currencies) if selected_currencies else 'All'}")
    analysis_lines.append("")
    analysis_lines.append(f"Products: {n_products}")
    analysis_lines.append(f"Total historical sales: {total_qty:,} units")
    analysis_lines.append(f"Total revenue: {sym}{total_rev:,.2f}")
    analysis_lines.append(f"30-day forecast: {forecast_qty:,.0f} units")

    if not fh.empty:
        date_range_start = fh["order_date"].min().strftime("%d/%m/%Y")
        date_range_end = fh["order_date"].max().strftime("%d/%m/%Y")
        analysis_lines.append(f"Data range: {date_range_start} to {date_range_end}")

    # Pre-compute exploded DataFrames (used by timeline + forecast sections)
    hist_exp = explode_categories(fh) if selected_cats and not fh.empty else pd.DataFrame()
    pred_exp = explode_categories(fp) if selected_cats and not fp.empty else pd.DataFrame()

    # --- 1. Category Timeline ---
    if selected_cats and not fh.empty:
        fig_timeline = go.Figure()
        exploded = hist_exp[hist_exp["cat_single"].isin(selected_cats)]
        for i, cat in enumerate(selected_cats):
            cat_data = exploded[exploded["cat_single"] == cat]
            if cat_data.empty:
                continue
            agg = cat_data.groupby("order_date")["quantity_sold"].sum().reset_index()
            color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]
            fig_timeline.add_trace(go.Scatter(
                x=agg["order_date"], y=agg["quantity_sold"],
                mode="lines", name=cat, line=dict(color=color, width=2),
            ))
        fig_timeline.update_layout(**PLOT_LAYOUT, title="Sales by Category Over Time",
                                   xaxis_title="Date", yaxis_title="Quantity Sold",
                                   legend=H_LEGEND, height=400)
        charts.append(("Sales by Category Over Time", fig_timeline))

    # --- 2. Category Forecast ---
    if selected_cats and not fh.empty:
        fig_fcst = go.Figure()
        for i, cat in enumerate(selected_cats):
            h = hist_exp[hist_exp["cat_single"] == cat]
            h_daily = h.groupby("order_date")["quantity_sold"].sum().reset_index()
            color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]
            if not h_daily.empty:
                cutoff = h_daily["order_date"].max() - pd.Timedelta(days=60)
                h_recent = h_daily[h_daily["order_date"] >= cutoff]
                fig_fcst.add_trace(go.Scatter(
                    x=h_recent["order_date"], y=h_recent["quantity_sold"],
                    mode="lines", name=f"{cat} (historical)",
                    line=dict(color=color, width=2), legendgroup=cat,
                ))
            if not pred_exp.empty:
                p = pred_exp[pred_exp["cat_single"] == cat]
                p_daily = p.groupby("order_date")["predicted_quantity"].sum().reset_index()
                if not p_daily.empty:
                    fig_fcst.add_trace(go.Scatter(
                        x=p_daily["order_date"], y=p_daily["predicted_quantity"],
                        mode="lines+markers", name=f"{cat} (forecast)",
                        line=dict(color=color, width=2.5, dash="dash"),
                        marker=dict(size=4), legendgroup=cat,
                    ))
        fig_fcst.update_layout(**PLOT_LAYOUT, title="Category Forecast (Next 30 Days)",
                               xaxis_title="Date", yaxis_title="Quantity",
                               legend=H_LEGEND, height=400)
        charts.append(("Daily Forecast by Category", fig_fcst))

        # Analysis: category breakdown
        analysis_lines.append("")
        analysis_lines.append("--- Category Breakdown ---")
        for cat in selected_cats:
            cat_h = hist_exp[hist_exp["cat_single"] == cat]
            cat_qty = int(cat_h["quantity_sold"].sum()) if not cat_h.empty else 0
            cat_rev_col = "revenue_converted" if "revenue_converted" in cat_h.columns else "revenue"
            cat_rev = cat_h[cat_rev_col].sum() if not cat_h.empty else 0
            n_prod = cat_h["product_id"].nunique() if not cat_h.empty else 0
            analysis_lines.append(f"  {cat}: {n_prod} products | {cat_qty:,} sold | {sym}{cat_rev:,.2f} revenue")

    # --- 3. Top Products ---
    if selected_cats and not fh.empty:
        fig_top = go.Figure()
        filtered_ps = filter_by_categories(product_sales, selected_cats, product_cat_map)
        filtered_ps = filter_by_event_tab(filtered_ps, tab_value)
        if selected_currencies:
            valid_pids_ps = set(fh["product_id"].unique())
            filtered_ps = filtered_ps[filtered_ps["product_id"].isin(valid_pids_ps)]
        top15 = filtered_ps.head(15).iloc[::-1]
        if not top15.empty:
            fig_top.add_trace(go.Bar(
                x=top15["quantity_sold"], y=top15["product_name"],
                orientation="h", marker_color=COLORS["accent"],
                marker_line_width=0, texttemplate="%{x:.0f}",
                textposition="outside", textfont_size=11,
            ))
            fig_top.update_layout(**PLOT_LAYOUT)
            fig_top.update_layout(title="Top 15 Products",
                                  margin=dict(l=10, r=40, t=40, b=30),
                                  showlegend=False, yaxis_title="",
                                  xaxis_title="Quantity Sold", height=500)
            charts.append(("Top 15 Products", fig_top))

            analysis_lines.append("")
            analysis_lines.append("--- Top 5 Products ---")
            for _, row in filtered_ps.head(5).iterrows():
                analysis_lines.append(f"  {row['product_name'][:60]}: {int(row['quantity_sold']):,} units")

    # --- 4. Monthly Revenue ---
    if selected_cats and not fh.empty:
        fig_rev = go.Figure()
        rev_data = fh.assign(month=lambda d: d["order_date"].dt.to_period("M").apply(lambda r: r.start_time))
        currencies = sorted(rev_data["currency"].dropna().unique()) if "currency" in rev_data.columns else [DISPLAY_CURRENCY]
        bar_colors = [COLORS["accent3"], COLORS["accent"], COLORS["accent4"],
                      COLORS["accent2"], "#7b8de0", "#e06070"]
        for i, cur in enumerate(currencies):
            cur_data = rev_data[rev_data["currency"] == cur] if "currency" in rev_data.columns else rev_data
            monthly = cur_data.groupby("month")[rev_col].sum().reset_index()
            if not monthly.empty:
                fig_rev.add_trace(go.Bar(
                    x=monthly["month"], y=monthly[rev_col],
                    name=f"from {currency_symbol(cur)} ({cur})" if len(currencies) > 1 else "Revenue",
                    marker_color=bar_colors[i % len(bar_colors)],
                    marker_line_width=0, opacity=0.85,
                ))
        fig_rev.update_layout(**PLOT_LAYOUT, title="Monthly Revenue",
                              xaxis_title="Month", yaxis_title=f"Revenue ({sym})",
                              barmode="stack", showlegend=len(currencies) > 1,
                              height=400)
        charts.append(("Monthly Revenue", fig_rev))

        if not rev_data.empty:
            analysis_lines.append("")
            analysis_lines.append("--- Revenue Trend ---")
            monthly_total = rev_data.groupby("month")[rev_col].sum().reset_index()
            if len(monthly_total) >= 2:
                last_month_rev = monthly_total[rev_col].iloc[-1]
                prev_month_rev = monthly_total[rev_col].iloc[-2]
                if prev_month_rev > 0:
                    change_pct = ((last_month_rev - prev_month_rev) / prev_month_rev) * 100
                    direction = "up" if change_pct > 0 else "down"
                    analysis_lines.append(
                        f"  Last month vs previous: {direction} {abs(change_pct):.1f}% "
                        f"({sym}{prev_month_rev:,.2f} -> {sym}{last_month_rev:,.2f})")
                avg_monthly = monthly_total[rev_col].mean()
                analysis_lines.append(f"  Average monthly revenue: {sym}{avg_monthly:,.2f}")

    # --- 5. Day of Week ---
    if selected_cats and not fh.empty:
        fig_wd = go.Figure()
        weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        wd_data = fh.assign(weekday=lambda d: d["order_date"].dt.dayofweek)
        wd = wd_data.groupby("weekday")["quantity_sold"].sum().reset_index()
        wd["weekday_name"] = wd["weekday"].map(lambda x: weekday_names[x])
        colors_wd = [COLORS["accent3"] if x >= 5 else COLORS["accent"] for x in wd["weekday"]]
        fig_wd.add_trace(go.Bar(
            x=wd["weekday_name"], y=wd["quantity_sold"],
            marker_color=colors_wd, marker_line_width=0,
        ))
        fig_wd.update_layout(**PLOT_LAYOUT, title="Sales by Day of Week",
                             xaxis_title="", yaxis_title="Quantity",
                             showlegend=False, height=350)
        charts.append(("Sales by Day of Week", fig_wd))

        if not wd.empty:
            best_day = wd.loc[wd["quantity_sold"].idxmax()]
            worst_day = wd.loc[wd["quantity_sold"].idxmin()]
            analysis_lines.append("")
            analysis_lines.append("--- Day of Week Patterns ---")
            analysis_lines.append(f"  Best day: {best_day['weekday_name']} ({int(best_day['quantity_sold']):,} units)")
            analysis_lines.append(f"  Slowest day: {worst_day['weekday_name']} ({int(worst_day['quantity_sold']):,} units)")

    # --- 6. Product Detail (if selected) ---
    if product_id is not None:
        pid = int(product_id)
        h = hist_df[hist_df["product_id"] == pid].sort_values("order_date")
        p = pred_df[pred_df["product_id"] == pid].sort_values("order_date")

        if not h.empty or not p.empty:
            fig_prod = go.Figure()
            pname = h["product_name"].iloc[0] if not h.empty else p["product_name"].iloc[0] if not p.empty else f"Product {pid}"

            if not h.empty:
                h_agg = h.groupby("order_date")["quantity_sold"].sum().reset_index()
                fig_prod.add_trace(go.Scatter(
                    x=h_agg["order_date"], y=h_agg["quantity_sold"],
                    mode="lines", name="Actual",
                    line=dict(color=COLORS["accent"], width=1.5),
                ))

            if not p.empty:
                has_ci = "yhat_lower" in p.columns and "yhat_upper" in p.columns
                if has_ci:
                    fig_prod.add_trace(go.Scatter(
                        x=pd.concat([p["order_date"], p["order_date"][::-1]]),
                        y=pd.concat([p["yhat_upper"], p["yhat_lower"][::-1]]),
                        fill="toself", fillcolor="rgba(184, 115, 72, 0.15)",
                        line=dict(color="rgba(0,0,0,0)"), name="80% interval",
                        showlegend=True, hoverinfo="skip",
                    ))
                fig_prod.add_trace(go.Scatter(
                    x=p["order_date"], y=p["predicted_quantity"],
                    mode="lines", name="Forecast",
                    line=dict(color=COLORS["accent4"], width=2),
                ))

            fig_prod.update_layout(**PLOT_LAYOUT, title=f"Product Detail: {pname[:70]}",
                                   height=400, legend=H_LEGEND)
            charts.append((f"Product Detail: {pname[:50]}", fig_prod))

            pm = metrics_df[metrics_df["product_id"] == pid]
            if not pm.empty:
                row = pm.iloc[0]
                analysis_lines.append("")
                analysis_lines.append(f"--- Product Detail: {pname[:60]} ---")
                analysis_lines.append(f"  MAE: {row.get('mae', 0):.2f}")
                analysis_lines.append(f"  RMSE: {row.get('rmse', 0):.2f}")
                analysis_lines.append(f"  R2 Score: {row.get('r2_score', 0):.3f}")
                if not p.empty:
                    analysis_lines.append(f"  30-day forecast: {p['predicted_quantity'].sum():.1f} units")
                    analysis_lines.append(f"  Daily average forecast: {p['predicted_quantity'].mean():.2f} units/day")

    # --- 7. Model Metrics Summary ---
    if not fm.empty:
        analysis_lines.append("")
        analysis_lines.append("--- Model Performance Summary ---")
        avg_mae = fm["mae"].mean()
        avg_r2 = fm["r2_score"].mean()
        analysis_lines.append(f"  Average MAE: {avg_mae:.2f}")
        analysis_lines.append(f"  Average R2 Score: {avg_r2:.3f}")
        if "method" in fm.columns:
            method_counts = fm["method"].value_counts()
            for method, count in method_counts.items():
                analysis_lines.append(f"  {method}: {count} products")

    return charts, "\n".join(analysis_lines), fh, fp, fm


def _get_ai_report_analysis(selected_cats, tab_value, selected_currencies, product_id,
                            fh, fp, fm):
    """Ask the AI agent to generate a comprehensive report analysis."""
    try:
        cats_label = ", ".join(selected_cats) if selected_cats else "All"
        tab_labels = {"active": "Active Events", "past": "Past Events",
                      "course": "Online Courses", "map": "All"}
        tab_label = tab_labels.get(tab_value, "All")

        prompt_parts = [
            "Generate a comprehensive, professional sales report for the following scope:",
            f"- Event type: {tab_label}",
            f"- Categories: {cats_label}",
            f"- Currencies: {', '.join(selected_currencies) if selected_currencies else 'All'}",
        ]

        if product_id is not None:
            pid = int(product_id)
            prows = hist_df[hist_df["product_id"] == pid]
            pname = prows["product_name"].iloc[0] if not prows.empty else f"Product {pid}"
            prompt_parts.append(f"- Focus product: {pname} (ID #{pid})")

        prompt_parts.extend([
            "",
            "Structure the report with these sections:",
            "## Executive Summary",
            "A brief 2-3 sentence overview of the current state.",
            "",
            "## Key Performance Metrics",
            "Show the most important KPIs in a table.",
            "",
            "## Sales Trends & Analysis",
            "Analyze recent trends, compare periods, highlight growth or decline.",
            "",
            "## Top Products Performance",
            "Rank and analyze the best-performing products.",
            "",
            "## Revenue Analysis",
            "Break down revenue by category and trends over time.",
            "",
            "## Forecast & Outlook",
            "Summarize the 30-day forecast and what it means.",
            "",
            "## Recommendations",
            "Provide 3-5 actionable recommendations based on the data.",
            "",
            "Use markdown formatting with headers, tables, bullet points.",
            "Be specific with numbers - use exact values from the data.",
        ])

        prompt = "\n".join(prompt_parts)
        response = ai_agent.chat(prompt, fh, fp, fm)
        return response
    except Exception as e:
        print(f"  [WARNING] AI report generation failed: {e}")
        return None


def _safe_text(text):
    """Convert text to latin-1 safe string for PDF."""
    if not text:
        return ""
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _generate_pdf_report(charts, stats_text, ai_text, selected_cats,
                         tab_value, selected_currencies, product_id):
    """Generate a PDF report with AI analysis, stats, and chart images."""
    from fpdf import FPDF
    import plotly.io as pio
    from datetime import datetime
    import tempfile
    import io

    cats_label = ", ".join(selected_cats) if selected_cats else "All"
    tab_labels = {"active": "Active Events", "past": "Past Events",
                  "course": "Online Courses", "map": "All"}
    tab_label = tab_labels.get(tab_value, "All")
    now_str = datetime.now().strftime("%B %d, %Y at %H:%M")

    class ReportPDF(FPDF):
        def header(self):
            if self.page_no() > 1:
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(160, 140, 80)
                self.cell(0, 6, "TCCHE - Sales Report", align="L")
                self.cell(0, 6, now_str, align="R", new_x="LMARGIN", new_y="NEXT")
                self.set_draw_color(200, 164, 78)
                self.line(10, self.get_y(), 200, self.get_y())
                self.ln(4)

        def footer(self):
            self.set_y(-15)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(140)
            self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ---- Title Page ----
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 8, "TCCHE", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 16, "Sales Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_draw_color(200, 164, 78)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(100)
    pdf.cell(0, 8, _safe_text(now_str), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(f"Scope: {tab_label}"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(f"Categories: {cats_label}"), align="C", new_x="LMARGIN", new_y="NEXT")
    if selected_currencies:
        pdf.cell(0, 8, _safe_text(f"Currencies: {', '.join(selected_currencies)}"),
                 align="C", new_x="LMARGIN", new_y="NEXT")

    # ---- AI Analysis ----
    content_w = pdf.w - pdf.l_margin - pdf.r_margin

    def _pdf_write_line(text, font_style="", font_size=10,
                        color=(60, 60, 60), spacing_before=0):
        """Write a line of text to the PDF, resetting X to left margin."""
        if spacing_before:
            pdf.ln(spacing_before)
        pdf.set_x(pdf.l_margin)
        pdf.set_font("Helvetica", font_style, font_size)
        pdf.set_text_color(*color)
        pdf.multi_cell(w=content_w, h=6, text=_safe_text(text),
                        new_x="LMARGIN", new_y="NEXT")

    if ai_text:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(200, 164, 78)
        pdf.cell(0, 6, "AI ANALYSIS", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        for line in ai_text.split("\n"):
            line = line.rstrip()
            if line.startswith("## "):
                _pdf_write_line(line[3:], "B", 14, spacing_before=4)
                pdf.set_draw_color(200, 164, 78)
                pdf.line(10, pdf.get_y(), 80, pdf.get_y())
                pdf.ln(2)
            elif line.startswith("### "):
                _pdf_write_line(line[4:], "B", 11, color=(80, 80, 80),
                                spacing_before=2)
            elif line.startswith("# "):
                _pdf_write_line(line[2:], "B", 16, color=(50, 50, 50),
                                spacing_before=4)
            elif line.startswith("|") and "---" in line:
                continue
            elif line.startswith("|"):
                pdf.set_x(pdf.l_margin)
                pdf.set_font("Courier", "", 8)
                pdf.set_text_color(60, 60, 60)
                pdf.multi_cell(w=content_w, h=5, text=_safe_text(line),
                               new_x="LMARGIN", new_y="NEXT")
            elif line.startswith("- ") or line.startswith("* "):
                _pdf_write_line("  - " + line[2:])
            elif line.startswith("**") and line.endswith("**"):
                _pdf_write_line(line.strip("*"), "B")
            elif line.strip() == "":
                pdf.ln(3)
            else:
                _pdf_write_line(line)

    # ---- Statistical Summary ----
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 6, "STATISTICAL SUMMARY", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(60, 60, 60)
    for line in stats_text.split("\n"):
        if line.startswith("---"):
            _pdf_write_line(line.strip("- "), "B", 11, color=(80, 80, 80),
                            spacing_before=2)
        elif line.strip() == "":
            pdf.ln(3)
        else:
            _pdf_write_line(line)

    # ---- Charts ----
    for title, fig in charts:
        try:
            fig_export = go.Figure(fig)
            fig_export.update_layout(
                paper_bgcolor="white", plot_bgcolor="white",
                font=dict(color="#333", size=12),
                xaxis=dict(gridcolor="#eee", showline=True, linecolor="#ccc"),
                yaxis=dict(gridcolor="#eee", showline=True, linecolor="#ccc",
                           rangemode="tozero"),
            )
            img_bytes = pio.to_image(fig_export, format="png", width=1100, height=480,
                                     scale=2)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(img_bytes)
                tmp_path = tmp.name

            pdf.add_page()
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(50, 50, 50)
            pdf.cell(0, 10, _safe_text(title), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
            pdf.image(tmp_path, x=5, w=200)

            import os as _os
            _os.unlink(tmp_path)
        except Exception as e:
            pdf.set_font("Helvetica", "I", 10)
            pdf.set_text_color(180, 80, 80)
            pdf.cell(0, 8, f"[Chart could not be rendered: {e}]",
                     new_x="LMARGIN", new_y="NEXT")

    # ---- Footer page ----
    pdf.add_page()
    pdf.ln(60)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 8, "TCCHE", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(140)
    pdf.cell(0, 8, "Sales Forecast Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(now_str), align="C", new_x="LMARGIN", new_y="NEXT")

    return bytes(pdf.output())


# Step 1a: Open modal instantly on Generate Report click
app.clientside_callback(
    "function(n) { return [{display: 'block'}, Date.now()]; }",
    Output("report-modal", "style"),
    Output("report-trigger", "data"),
    Input("report-btn", "n_clicks"),
    prevent_initial_call=True,
)

# Step 1b: Close modal on Close / overlay click
app.clientside_callback(
    "function(a, b) { return {display: 'none'}; }",
    Output("report-modal", "style", allow_duplicate=True),
    Input("report-close-btn", "n_clicks"),
    Input("report-overlay", "n_clicks"),
    prevent_initial_call=True,
)


# Step 2: Populate report content (server-side, triggered by Store change)
@callback(
    Output("report-content", "children"),
    Output("report-cache", "data"),
    Input("report-trigger", "data"),
    State("category-filter", "value"),
    State("event-tabs", "value"),
    State("currency-filter", "value"),
    State("product-selector", "value"),
    prevent_initial_call=True,
)
def generate_report_content(trigger, selected_cats, tab_value,
                            selected_currencies, product_id):
    """Generate report content and cache for PDF reuse."""
    if not selected_cats:
        return html.Div(style={"textAlign": "center", "padding": "60px"}, children=[
            html.P("No categories selected.", style={
                "color": COLORS["text_muted"], "fontSize": "18px",
            }),
            html.P("Select categories in the filter above, then click Generate Report.",
                   style={"color": COLORS["text_muted"], "fontSize": "14px"}),
        ]), None

    charts, stats_text, fh, fp, fm = _build_report_charts(
        selected_cats, tab_value, selected_currencies, product_id
    )

    ai_text = _get_ai_report_analysis(
        selected_cats, tab_value, selected_currencies, product_id, fh, fp, fm
    )

    # Cache charts as JSON + text for PDF reuse (avoids 2nd AI call)
    import plotly.io as pio
    cache_data = {
        "charts_json": [(title, pio.to_json(fig)) for title, fig in charts],
        "stats_text": stats_text,
        "ai_text": ai_text or "",
        "selected_cats": selected_cats,
        "tab_value": tab_value,
        "selected_currencies": selected_currencies,
        "product_id": product_id,
    }

    report_children = []

    if ai_text:
        report_children.append(
            html.Div(style=card_style({"marginBottom": "24px",
                                       "borderTop": f"3px solid {COLORS['accent']}"}), children=[
                section_label("AI ANALYSIS"),
                dcc.Markdown(
                    ai_text,
                    style={
                        "fontSize": "14px",
                        "color": COLORS["text"],
                        "lineHeight": "1.8",
                    },
                    className="report-ai-markdown",
                ),
            ])
        )

    report_children.append(
        html.Div(style=card_style({"marginBottom": "24px"}), children=[
            section_label("STATISTICAL SUMMARY"),
            html.Pre(stats_text, style={
                "fontFamily": "'Outfit', sans-serif",
                "fontSize": "13px",
                "color": COLORS["text"],
                "backgroundColor": "transparent",
                "margin": "0",
                "whiteSpace": "pre-wrap",
                "lineHeight": "1.8",
            }),
        ])
    )

    for title, fig in charts:
        report_children.append(
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                dcc.Graph(figure=fig, config={"displayModeBar": True, "toImageButtonOptions": {
                    "format": "png", "width": 1200, "height": 500,
                }}),
            ])
        )

    return report_children, cache_data


# Instant spinner on Download PDF click
app.clientside_callback(
    """
    function(n) {
        if (!n) return [window.dash_clientside.no_update,
                        window.dash_clientside.no_update,
                        window.dash_clientside.no_update];
        return [{display: 'inline-block'}, 'Preparing...', true];
    }
    """,
    Output("pdf-spinner", "style"),
    Output("pdf-btn-text", "children"),
    Output("report-download-btn", "disabled"),
    Input("report-download-btn", "n_clicks"),
    prevent_initial_call=True,
)


@callback(
    Output("report-download", "data"),
    Output("pdf-spinner", "style", allow_duplicate=True),
    Output("pdf-btn-text", "children", allow_duplicate=True),
    Output("report-download-btn", "disabled", allow_duplicate=True),
    Input("report-download-btn", "n_clicks"),
    State("report-cache", "data"),
    State("category-filter", "value"),
    State("event-tabs", "value"),
    State("currency-filter", "value"),
    State("product-selector", "value"),
    prevent_initial_call=True,
)
def download_report_pdf(n_clicks, cache, selected_cats, tab_value,
                        selected_currencies, product_id):
    """Generate PDF from cached report data (no duplicate AI call)."""
    from datetime import datetime
    import plotly.io as pio

    if not n_clicks or not selected_cats:
        return no_update, no_update, no_update, no_update

    # Reuse cached data when available (same filters)
    if (cache
            and cache.get("selected_cats") == selected_cats
            and cache.get("tab_value") == tab_value
            and cache.get("selected_currencies") == selected_currencies
            and cache.get("product_id") == product_id):
        charts = [(t, pio.from_json(j)) for t, j in cache["charts_json"]]
        stats_text = cache["stats_text"]
        ai_text = cache["ai_text"] or None
    else:
        charts, stats_text, fh, fp, fm = _build_report_charts(
            selected_cats, tab_value, selected_currencies, product_id
        )
        ai_text = _get_ai_report_analysis(
            selected_cats, tab_value, selected_currencies, product_id, fh, fp, fm
        )

    pdf_bytes = _generate_pdf_report(
        charts, stats_text, ai_text,
        selected_cats, tab_value, selected_currencies, product_id
    )

    filename = f"tcche_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"

    return (
        dcc.send_bytes(pdf_bytes, filename, mime_type="application/pdf"),
        {"display": "none"},
        "Download PDF",
        False,
    )


# ============================================================
# RUN
# ============================================================

# Expose the Flask server for gunicorn (production)
server = app.server

# Setup authentication (JWT + cookies)
import auth as _auth
_auth.setup_auth(server)
print("  [OK] Authentication enabled")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    debug = os.environ.get("RENDER") is None  # disable debug in production
    print(f"\n  Dashboard available at: http://localhost:{port}\n")
    app.run(debug=debug, host="0.0.0.0", port=port)
