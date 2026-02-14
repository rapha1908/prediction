import os
import pandas as pd
import numpy as np
from dash import Dash, html, dcc, callback, Output, Input, State, no_update
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
# EXCHANGE RATES & REVENUE CONVERSION
# ============================================================

# Fetch rates once at startup and add converted revenue column
_currencies_in_data = list(hist_df["currency"].dropna().unique()) if "currency" in hist_df.columns else []
exchange_rates = ai_agent.fetch_exchange_rates(_currencies_in_data)
hist_df = convert_revenue(hist_df, exchange_rates)

print(f"  Display currency: {DISPLAY_CURRENCY}")
if len(_currencies_in_data) > 1:
    print(f"  Currencies found: {', '.join(sorted(_currencies_in_data))}")


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
    df = df.copy()
    df["category_list"] = df["category"].apply(parse_categories)
    return df.explode("category_list").rename(columns={"category_list": "cat_single"})


# ============================================================
# PREPROCESSING
# ============================================================

# Category map by product
product_cat_map = build_product_cat_map(hist_df)

# Product -> active or past event map (based on ticket_end_date)
TODAY = pd.Timestamp.now().normalize()


ONLINE_COURSE_CATS = {"ONLINE COURSE"}


def _is_online_course(pid):
    """Check if a product belongs to the ONLINE COURSE category."""
    rows = hist_df[hist_df["product_id"] == pid]
    if rows.empty:
        return False
    cats = set(parse_categories(rows["category"].iloc[0]))
    return bool(cats & ONLINE_COURSE_CATS)


def build_event_status_map():
    """
    Create map product_id -> 'active', 'past', or 'course' based on
    ticket_end_date and category.
    Products in the ONLINE COURSE category are always classified as 'course'.
    """
    # First, collect the most reliable ticket_end_date for each product_id
    # Priority: hist > pred > metrics (hist has more products)
    date_by_pid = {}
    for df in [metrics_df, pred_df, hist_df]:  # hist por ultimo = maior prioridade
        if "ticket_end_date" not in df.columns:
            continue
        for pid, grp in df.groupby("product_id"):
            end_vals = grp["ticket_end_date"].dropna()
            if not end_vals.empty:
                date_by_pid[pid] = end_vals.iloc[0]

    # Classify each product
    status_map = {}
    no_date_pids = set()
    all_pids = set(hist_df["product_id"].unique())
    all_pids |= set(pred_df["product_id"].unique()) if "product_id" in pred_df.columns else set()

    # --- Pass 0: Online Courses go to their own tab ---
    course_pids = set()
    for pid in all_pids:
        if _is_online_course(pid):
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
    # Infer status by category: if none of the specific categories
    # of this product have ACTIVE products (from pass 1), classify as "past".
    GENERIC_CATS = {"Uncategorized", "Sem categoria", "EVENTS", "LIVESTREAM", "ONLINE COURSE",
                    "THE BREATHWORK REVOLUTION"}

    # Category map -> has active product? (using pass 1 only)
    cat_has_active = {}
    for pid_val, st in status_map.items():
        if st == "course":
            continue
        rows = hist_df[hist_df["product_id"] == pid_val]
        if rows.empty:
            continue
        for cat in parse_categories(rows["category"].iloc[0]):
            if cat not in GENERIC_CATS:
                if st == "active":
                    cat_has_active[cat] = True
                elif cat not in cat_has_active:
                    cat_has_active[cat] = False

    for pid in no_date_pids:
        rows = hist_df[hist_df["product_id"] == pid]
        if rows.empty:
            status_map[pid] = "past"
            continue

        # Specific categories of this product
        product_cats = set(parse_categories(rows["category"].iloc[0])) - GENERIC_CATS

        # If any specific category has an active product -> active
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

# Unique categories (expanded from pipe-separated)
all_categories = sorted(set(
    cat
    for cats_str in hist_df["category"].dropna().unique()
    for cat in parse_categories(cats_str)
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
)
app.title = "TCCHE – Sales Forecast Dashboard"

app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"], "minHeight": "100vh",
        "fontFamily": FONT, "color": COLORS["text"], "padding": "0",
    },
    children=[

        # --- HEADER ---
        html.Div(
            style={
                "background": "linear-gradient(135deg, #13121e 0%, #1a1528 40%, #1e1610 100%)",
                "padding": "36px 48px 32px", "borderBottom": f"1px solid {COLORS['card_border']}",
            },
            children=[
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
            ],
        ),

        # --- CONTEUDO ---
        html.Div(style={"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"}, children=[

            # KPIs (dinamicos com a tab)
            html.Div(id="kpi-container",
                style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px"},
            ),

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
                ],
            ),

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

            # ============ FILTRO DE CATEGORIAS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                section_label("FILTERS"),
                html.H3("Filter by Category", style={
                    "margin": "0 0 14px", "fontSize": "18px", "fontWeight": "700",
                }),
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

def filter_by_event_tab(df, tab_value):
    """Filter DataFrame by event status (active/past/course) based on the tab."""
    if "product_id" not in df.columns:
        return df
    pids = {pid for pid, st in event_status_map.items() if st == tab_value}
    return df[df["product_id"].isin(pids)]


# --- Update categories based on tab ---
@callback(
    Output("category-filter", "options"),
    Output("category-filter", "value"),
    Input("event-tabs", "value"),
)
def update_category_filter(tab_value):
    filtered = filter_by_event_tab(hist_df, tab_value)
    if filtered.empty:
        return [], []
    cats = sorted(set(
        cat
        for cats_str in filtered["category"].dropna().unique()
        for cat in parse_categories(cats_str)
    ))
    return [{"label": c, "value": c} for c in cats], cats


# --- Dynamic KPIs ---
@callback(
    Output("kpi-container", "children"),
    Input("event-tabs", "value"),
)
def update_kpis(tab_value):
    fh = filter_by_event_tab(hist_df, tab_value)
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

    tab_labels = {"active": "Active", "past": "Past", "course": "Online Courses"}
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
)
def update_daily_report(tab_value):
    fh = filter_by_event_tab(hist_df, tab_value)
    fp = filter_by_event_tab(pred_df, tab_value)

    if fh.empty and fp.empty:
        return html.P("No products found.", style={"color": COLORS["text_muted"]})

    today = pd.Timestamp.now().normalize()

    # Products with forecast (sort by 7d total forecast)
    pred_pids = set(fp["product_id"].unique()) if not fp.empty else set()
    hist_pids = set(fh["product_id"].unique()) if not fh.empty else set()
    all_pids = pred_pids | hist_pids

    # Build data per product
    rows_data = []
    for pid in all_pids:
        ph = fh[fh["product_id"] == pid]
        pp = fp[fp["product_id"] == pid]

        pname = ph["product_name"].iloc[-1] if not ph.empty else (pp["product_name"].iloc[0] if not pp.empty else f"#{pid}")

        # Sales from last 7 days
        recent_sales = {}
        if not ph.empty:
            for i in range(7):
                d = today - pd.Timedelta(days=7 - i)
                day_data = ph[ph["order_date"] == d]
                recent_sales[d] = int(day_data["quantity_sold"].sum()) if not day_data.empty else 0

        # Forecast for next 7 days
        forecast = {}
        if not pp.empty:
            pp_sorted = pp.sort_values("order_date")
            for _, row in pp_sorted.head(7).iterrows():
                forecast[row["order_date"]] = round(row["predicted_quantity"], 1)

        # Total 7d forecast
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


# --- Update product dropdown based on categories and tab ---
@callback(
    Output("product-selector", "options"),
    Output("product-selector", "value"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
)
def update_product_options(selected_cats, tab_value):
    if not selected_cats:
        return [], None
    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
    filtered = filter_by_event_tab(filtered, tab_value)
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
)
def update_category_timeline(selected_cats, granularity, tab_value):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Filtrar por tab
    filtered_hist = filter_by_event_tab(hist_df, tab_value)

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
)
def update_category_forecast(selected_cats, tab_value):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Filtrar por tab
    filtered_hist = filter_by_event_tab(hist_df, tab_value)
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
)
def update_top_products(selected_cats, tab_value):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
    filtered = filter_by_event_tab(filtered, tab_value)
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
)
def update_monthly_revenue(selected_cats, tab_value):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_categories(hist_df, selected_cats, product_cat_map).copy()
    filtered = filter_by_event_tab(filtered, tab_value)
    filtered["month"] = filtered["order_date"].dt.to_period("M").apply(lambda r: r.start_time)

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
)
def update_weekday_chart(selected_cats, tab_value):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    filtered = filter_by_categories(hist_df, selected_cats, product_cat_map).copy()
    filtered = filter_by_event_tab(filtered, tab_value)
    filtered["weekday"] = filtered["order_date"].dt.dayofweek
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


# --- Tabela de metricas ---
@callback(
    Output("metrics-table", "children"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
)
def update_metrics_table(selected_cats, tab_value):
    if not selected_cats:
        return html.P("Select at least one category.", style={"color": COLORS["text_muted"]})

    # Filtrar metricas por categorias (multi-categoria)
    filtered_metrics = filter_by_categories(metrics_df, selected_cats, product_cat_map)
    filtered_metrics = filter_by_event_tab(filtered_metrics, tab_value)

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
# RUN
# ============================================================

if __name__ == "__main__":
    print("\n  Dashboard available at: http://localhost:8050\n")
    app.run(debug=True, port=8050)
