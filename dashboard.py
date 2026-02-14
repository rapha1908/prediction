import pandas as pd
import numpy as np
from dash import Dash, html, dcc, callback, Output, Input
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import sys

# ============================================================
# CARREGAR DADOS
# ============================================================

DATA_DIR = Path(__file__).parent


def load_data():
    """Carrega os CSVs gerados pelo main.py."""
    files = {
        "historico": DATA_DIR / "vendas_historicas.csv",
        "previsoes": DATA_DIR / "previsoes_vendas.csv",
        "metricas": DATA_DIR / "metricas_modelos.csv",
    }

    for name, path in files.items():
        if not path.exists():
            print(f"Arquivo nao encontrado: {path}")
            print("Execute primeiro: py main.py")
            sys.exit(1)

    hist = pd.read_csv(files["historico"], parse_dates=["order_date"])
    pred = pd.read_csv(files["previsoes"], parse_dates=["order_date"])
    metrics = pd.read_csv(files["metricas"])

    # Garantir coluna category
    for df in [hist, pred, metrics]:
        if "category" not in df.columns:
            df["category"] = "Sem categoria"

    # Garantir que product_id nao tem duplicatas de nome nas metricas
    if not metrics.empty and "product_id" in metrics.columns:
        metrics = metrics.drop_duplicates(subset=["product_id"], keep="first")

    # Parsear ticket_end_date como datetime em todos os DataFrames
    for df in [hist, pred, metrics]:
        if "ticket_end_date" in df.columns:
            df["ticket_end_date"] = pd.to_datetime(df["ticket_end_date"], errors="coerce")

    return hist, pred, metrics


hist_df, pred_df, metrics_df = load_data()

# ============================================================
# HELPERS MULTI-CATEGORIA
# ============================================================

def parse_categories(cat_str):
    """Extrai lista de categorias de uma string pipe-separada."""
    if pd.isna(cat_str) or str(cat_str).strip() == "":
        return ["Sem categoria"]
    return [c.strip() for c in str(cat_str).split("|") if c.strip()]


def build_product_cat_map(df):
    """Cria mapa product_id -> set de categorias."""
    mapping = {}
    for _, row in df.drop_duplicates("product_id").iterrows():
        mapping[row["product_id"]] = set(parse_categories(row["category"]))
    return mapping


def product_matches_cats(product_id, selected_cats, cat_map):
    """Verifica se um produto pertence a alguma das categorias selecionadas."""
    return bool(cat_map.get(product_id, set()) & set(selected_cats))


def filter_by_categories(df, selected_cats, cat_map):
    """Filtra DataFrame para produtos que pertencem a alguma das categorias."""
    matching_pids = {
        pid for pid, cats in cat_map.items()
        if cats & set(selected_cats)
    }
    return df[df["product_id"].isin(matching_pids)]


def explode_categories(df):
    """Expande linhas para que cada categoria tenha sua propria linha."""
    df = df.copy()
    df["category_list"] = df["category"].apply(parse_categories)
    return df.explode("category_list").rename(columns={"category_list": "cat_single"})


# ============================================================
# PRE-PROCESSAR
# ============================================================

# Mapa de categorias por produto
product_cat_map = build_product_cat_map(hist_df)

# Mapa de produto -> evento ativo ou passado (baseado em ticket_end_date)
TODAY = pd.Timestamp.now().normalize()


def build_event_status_map():
    """
    Cria mapa product_id -> 'active' ou 'past' baseado em ticket_end_date.
    Usa todos os DataFrames disponiveis para encontrar a data de cada produto.
    """
    # Primeiro, coletar a ticket_end_date mais confiavel para cada product_id
    # Prioridade: hist > pred > metrics (hist tem mais produtos)
    date_by_pid = {}
    for df in [metrics_df, pred_df, hist_df]:  # hist por ultimo = maior prioridade
        if "ticket_end_date" not in df.columns:
            continue
        for pid, grp in df.groupby("product_id"):
            end_vals = grp["ticket_end_date"].dropna()
            if not end_vals.empty:
                date_by_pid[pid] = end_vals.iloc[0]

    # Classificar cada produto em 2 passes
    status_map = {}
    no_date_pids = set()
    all_pids = set(hist_df["product_id"].unique())
    all_pids |= set(pred_df["product_id"].unique()) if "product_id" in pred_df.columns else set()

    # --- Passo 1: produtos COM ticket_end_date ---
    for pid in all_pids:
        if pid in date_by_pid and pd.notna(date_by_pid[pid]):
            status_map[pid] = "active" if date_by_pid[pid] >= TODAY else "past"
        else:
            no_date_pids.add(pid)

    # --- Passo 2: produtos SEM ticket_end_date ---
    # Inferir status pela categoria: se nenhuma das categorias especificas
    # deste produto tem produtos ATIVOS (do passo 1), classificar como "past".
    GENERIC_CATS = {"Sem categoria", "EVENTS", "LIVESTREAM", "ONLINE COURSE",
                    "THE BREATHWORK REVOLUTION"}

    # Mapa de categoria -> tem produto ativo? (usando apenas passo 1)
    cat_has_active = {}
    for pid_val, st in status_map.items():
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

        # Categorias especificas deste produto
        product_cats = set(parse_categories(rows["category"].iloc[0])) - GENERIC_CATS

        # Se alguma categoria especifica tem produto ativo -> active
        if product_cats and any(cat_has_active.get(c, False) for c in product_cats):
            status_map[pid] = "active"
        else:
            status_map[pid] = "past"

    return status_map


event_status_map = build_event_status_map()
n_active = sum(1 for v in event_status_map.values() if v == "active")
n_past = sum(1 for v in event_status_map.values() if v == "past")
print(f"  Eventos: {n_active} ativos, {n_past} passados")

# Categorias unicas (expandidas de pipe-separadas)
all_categories = sorted(set(
    cat
    for cats_str in hist_df["category"].dropna().unique()
    for cat in parse_categories(cats_str)
))

# Lista de produtos (ordenar por total vendido, agrupar por product_id para evitar duplicatas)
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

# KPIs gerais
total_products = hist_df["product_id"].nunique()
total_sales_qty = int(hist_df["quantity_sold"].sum())
total_revenue = hist_df["revenue"].sum()
total_orders_days = hist_df["order_date"].nunique()
date_min = hist_df["order_date"].min().strftime("%d/%m/%Y")
date_max = hist_df["order_date"].max().strftime("%d/%m/%Y")
pred_total_qty = pred_df["predicted_quantity"].sum()

# ============================================================
# ESTILO E CORES
# ============================================================

COLORS = {
    "bg": "#0f1117",
    "card": "#1a1d23",
    "card_border": "#2d3139",
    "text": "#e6e6e6",
    "text_muted": "#8b949e",
    "accent": "#58a6ff",
    "accent2": "#f97316",
    "accent3": "#22c55e",
    "accent4": "#a855f7",
    "red": "#ef4444",
    "grid": "#21262d",
}

FONT = "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif"

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family=FONT, color=COLORS["text"], size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor=COLORS["grid"], showline=False),
    yaxis=dict(gridcolor=COLORS["grid"], showline=False, rangemode="tozero"),
    hovermode="x unified",
)

CATEGORY_COLORS = px.colors.qualitative.Plotly


def card_style(extra=None):
    base = {
        "backgroundColor": COLORS["card"],
        "border": f"1px solid {COLORS['card_border']}",
        "borderRadius": "12px",
        "padding": "24px",
    }
    if extra:
        base.update(extra)
    return base


def kpi_card(title, value, subtitle="", color=COLORS["accent"]):
    return html.Div(
        style=card_style({"textAlign": "center", "flex": "1", "minWidth": "170px"}),
        children=[
            html.P(title, style={
                "color": COLORS["text_muted"], "fontSize": "12px",
                "marginBottom": "4px", "textTransform": "uppercase",
                "letterSpacing": "0.5px", "fontWeight": "500",
            }),
            html.H2(value, style={
                "color": color, "margin": "8px 0 4px",
                "fontSize": "26px", "fontWeight": "700",
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

app = Dash(__name__)
app.title = "Dashboard de Previsao de Vendas"

app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"], "minHeight": "100vh",
        "fontFamily": FONT, "color": COLORS["text"], "padding": "0",
    },
    children=[

        # --- HEADER ---
        html.Div(
            style={
                "background": "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)",
                "padding": "28px 48px", "borderBottom": f"1px solid {COLORS['card_border']}",
            },
            children=[
                html.H1("Previsao de Vendas", style={
                    "margin": "0 0 4px", "fontSize": "28px", "fontWeight": "700",
                    "background": "linear-gradient(90deg, #58a6ff, #a855f7)",
                    "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                }),
                html.P(f"Dados de {date_min} a {date_max}", style={
                    "color": COLORS["text_muted"], "margin": "0", "fontSize": "14px",
                }),
            ],
        ),

        # --- CONTEUDO ---
        html.Div(style={"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"}, children=[

            # KPIs (dinamicos com a tab)
            html.Div(id="kpi-container",
                style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px"},
            ),

            # ============ TABS: EVENTOS ATIVOS / PASSADOS ============
            dcc.Tabs(
                id="event-tabs",
                value="active",
                style={"marginBottom": "24px"},
                children=[
                    dcc.Tab(
                        label=f"Eventos Ativos ({n_active})",
                        value="active",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 24px", "fontFamily": FONT, "fontSize": "14px", "fontWeight": "500"},
                        selected_style={"backgroundColor": COLORS["card"], "color": COLORS["accent"],
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 24px",
                                        "fontFamily": FONT, "fontSize": "14px", "fontWeight": "700"},
                    ),
                    dcc.Tab(
                        label=f"Eventos Passados ({n_past})",
                        value="past",
                        style={"backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                               "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                               "padding": "12px 24px", "fontFamily": FONT, "fontSize": "14px", "fontWeight": "500"},
                        selected_style={"backgroundColor": COLORS["card"], "color": COLORS["accent2"],
                                        "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                                        "borderRadius": "8px 8px 0 0", "padding": "12px 24px",
                                        "fontFamily": FONT, "fontSize": "14px", "fontWeight": "700"},
                    ),
                ],
            ),

            # ============ REPORTE DIARIO ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Reporte Diario - Vendas e Previsao 7 Dias", style={
                    "margin": "0 0 4px", "fontSize": "16px", "fontWeight": "600",
                }),
                html.P("Vendas recentes por produto e previsao diaria para os proximos 7 dias", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px",
                }),
                html.Div(id="daily-report", style={"overflowX": "auto", "maxHeight": "600px", "overflowY": "auto"}),
            ]),

            # ============ FILTRO DE CATEGORIAS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Filtrar por Categoria", style={
                    "margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600",
                }),
                html.Div(style={"display": "flex", "gap": "16px", "alignItems": "center", "flexWrap": "wrap"}, children=[
                    html.Div(style={"flex": "1", "minWidth": "300px"}, children=[
                        html.Label("Categorias:", style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block"}),
                        dcc.Dropdown(
                            id="category-filter",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Selecione categorias...",
                            style=dropdown_style,
                        ),
                    ]),
                    html.Div(style={"minWidth": "180px"}, children=[
                        html.Label("Granularidade:", style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block"}),
                        dcc.RadioItems(
                            id="time-granularity",
                            options=[
                                {"label": " Diario", "value": "daily"},
                                {"label": " Semanal", "value": "weekly"},
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
                html.H3("Vendas por Categoria ao Longo do Tempo", style={
                    "margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600",
                }),
                dcc.Graph(id="category-timeline", config={"displayModeBar": False}),
            ]),

            # ============ PREVISAO POR CATEGORIA (DIARIA) ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Previsao Diaria por Categoria (Proximos 30 dias)", style={
                    "margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600",
                }),
                dcc.Graph(id="category-forecast", config={"displayModeBar": False}),
            ]),

            # ============ PREVISAO INDIVIDUAL POR PRODUTO (largura total) ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Real vs Previsao por Produto", style={
                    "margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600",
                }),
                dcc.Dropdown(
                    id="product-selector",
                    placeholder="Selecione um produto...",
                    style={**dropdown_style, "marginBottom": "16px"},
                ),
                dcc.Graph(id="product-forecast", style={"height": "420px"}, config={"displayModeBar": False}),
            ]),

            # ============ TOP PRODUTOS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Top 15 Produtos (Categorias Selecionadas)", style={
                    "margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600",
                }),
                dcc.Graph(id="top-products-chart", config={"displayModeBar": False}),
            ]),

            # ============ GRID: RECEITA + DIA DA SEMANA ============
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "28px"}, children=[

                html.Div(style=card_style(), children=[
                    html.H3("Receita Mensal (Categorias Selecionadas)", style={
                        "margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600",
                    }),
                    dcc.Graph(id="monthly-revenue", config={"displayModeBar": False}),
                ]),

                html.Div(style=card_style(), children=[
                    html.H3("Vendas por Dia da Semana", style={
                        "margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600",
                    }),
                    dcc.Graph(id="weekday-chart", config={"displayModeBar": False}),
                ]),
            ]),

            # ============ TABELA DE METRICAS ============
            html.Div(style=card_style({"marginBottom": "28px"}), children=[
                html.H3("Metricas dos Modelos de Previsao", style={
                    "margin": "0 0 8px", "fontSize": "16px", "fontWeight": "600",
                }),
                html.P("Ordenado por previsao total (30 dias)", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px",
                }),
                html.Div(id="metrics-table", style={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"}),
            ]),

            # FOOTER
            html.Div(style={"textAlign": "center", "padding": "20px 0", "borderTop": f"1px solid {COLORS['card_border']}"}, children=[
                html.P("Dashboard de Previsao de Vendas - Powered by Plotly Dash", style={
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
    """Filtra DataFrame por status do evento (active/past) baseado na tab."""
    if "product_id" not in df.columns:
        return df
    if tab_value == "active":
        pids = {pid for pid, st in event_status_map.items() if st == "active"}
    else:
        pids = {pid for pid, st in event_status_map.items() if st == "past"}
    return df[df["product_id"].isin(pids)]


# --- Atualizar categorias baseado na tab ---
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


# --- KPIs dinamicos ---
@callback(
    Output("kpi-container", "children"),
    Input("event-tabs", "value"),
)
def update_kpis(tab_value):
    fh = filter_by_event_tab(hist_df, tab_value)
    fp = filter_by_event_tab(pred_df, tab_value)

    n_products = fh["product_id"].nunique() if not fh.empty else 0
    n_sales = int(fh["quantity_sold"].sum()) if not fh.empty else 0
    rev = fh["revenue"].sum() if not fh.empty else 0
    n_cats = len(set(
        cat for cats_str in fh["category"].dropna().unique()
        for cat in parse_categories(cats_str)
    )) if not fh.empty else 0
    pred_total = fp["predicted_quantity"].sum() if not fp.empty else 0

    tab_label = "Ativos" if tab_value == "active" else "Passados"

    return [
        kpi_card("Produtos", str(n_products), color=COLORS["accent"], subtitle=tab_label),
        kpi_card("Vendas Totais", f"{n_sales:,}".replace(",", "."), color=COLORS["accent3"]),
        kpi_card("Receita Total", f"$ {rev:,.2f}", color=COLORS["accent2"]),
        kpi_card("Categorias", str(n_cats), color=COLORS["accent4"]),
        kpi_card("Previsao 30d", f"{pred_total:,.0f} un.", color=COLORS["accent4"]),
    ]


# --- Reporte diario ---
@callback(
    Output("daily-report", "children"),
    Input("event-tabs", "value"),
)
def update_daily_report(tab_value):
    fh = filter_by_event_tab(hist_df, tab_value)
    fp = filter_by_event_tab(pred_df, tab_value)

    if fh.empty and fp.empty:
        return html.P("Nenhum produto encontrado.", style={"color": COLORS["text_muted"]})

    today = pd.Timestamp.now().normalize()

    # Produtos com previsao (ordenar por previsao total 7d)
    pred_pids = set(fp["product_id"].unique()) if not fp.empty else set()
    hist_pids = set(fh["product_id"].unique()) if not fh.empty else set()
    all_pids = pred_pids | hist_pids

    # Construir dados por produto
    rows_data = []
    for pid in all_pids:
        ph = fh[fh["product_id"] == pid]
        pp = fp[fp["product_id"] == pid]

        pname = ph["product_name"].iloc[-1] if not ph.empty else (pp["product_name"].iloc[0] if not pp.empty else f"#{pid}")

        # Vendas dos ultimos 7 dias
        recent_sales = {}
        if not ph.empty:
            for i in range(7):
                d = today - pd.Timedelta(days=7 - i)
                day_data = ph[ph["order_date"] == d]
                recent_sales[d] = int(day_data["quantity_sold"].sum()) if not day_data.empty else 0

        # Previsao dos proximos 7 dias
        forecast = {}
        if not pp.empty:
            pp_sorted = pp.sort_values("order_date")
            for _, row in pp_sorted.head(7).iterrows():
                forecast[row["order_date"]] = round(row["predicted_quantity"], 1)

        # Total previsao 7d
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

    # Ordenar por previsao 7d desc
    rows_data.sort(key=lambda x: x["total_prev_7d"], reverse=True)
    rows_data = rows_data[:50]  # Limitar a 50 produtos

    if not rows_data:
        return html.P("Nenhum dado disponivel.", style={"color": COLORS["text_muted"]})

    # Coletar datas para colunas
    recent_dates = sorted(set(d for r in rows_data for d in r["recent_sales"]))
    forecast_dates = sorted(set(d for r in rows_data for d in r["forecast"]))

    # Estilo da tabela
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
        html.Th("Produto", style={**th_style, "textAlign": "left", "minWidth": "200px"}),
    ]
    # Colunas de vendas recentes (ultimos 7 dias)
    for d in recent_dates:
        day_label = d.strftime("%d/%m")
        header_cells.append(html.Th(day_label, style={**th_style, "backgroundColor": "#1a2332"}))
    header_cells.append(html.Th("Total 7d", style={**th_style, "backgroundColor": "#1a2332"}))

    # Separador visual
    header_cells.append(html.Th("", style={**th_style, "width": "4px", "padding": "0",
                                            "backgroundColor": COLORS["accent"], "minWidth": "4px"}))

    # Colunas de previsao (proximos 7 dias)
    for d in forecast_dates:
        day_label = d.strftime("%d/%m")
        header_cells.append(html.Th(day_label, style={**th_style, "backgroundColor": "#2a1f14"}))
    header_cells.append(html.Th("Total 7d", style={**th_style, "backgroundColor": "#2a1f14"}))

    # Sub-header (label das secoes)
    sub_cells = [html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}"})]
    for _ in recent_dates:
        sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                             "backgroundColor": "#1a2332"}))
    sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                         "backgroundColor": "#1a2332"}))
    sub_cells.append(html.Th("", style={**th_style, "width": "4px", "padding": "0",
                                         "backgroundColor": COLORS["accent"], "minWidth": "4px"}))
    for _ in forecast_dates:
        sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                             "backgroundColor": "#2a1f14"}))
    sub_cells.append(html.Th("", style={**th_style, "borderBottom": f"1px solid {COLORS['card_border']}",
                                         "backgroundColor": "#2a1f14"}))

    # Linha de titulo de grupo
    n_recent = len(recent_dates) + 1  # +1 para total
    n_forecast = len(forecast_dates) + 1
    group_header = html.Tr([
        html.Th("", style={**th_style, "borderBottom": "none"}),
        html.Th("VENDAS RECENTES", colSpan=n_recent,
                style={**th_style, "borderBottom": "none", "color": COLORS["accent"],
                       "fontSize": "11px", "backgroundColor": "#1a2332"}),
        html.Th("", style={**th_style, "width": "4px", "padding": "0", "borderBottom": "none",
                            "backgroundColor": COLORS["accent"], "minWidth": "4px"}),
        html.Th("PREVISAO", colSpan=n_forecast,
                style={**th_style, "borderBottom": "none", "color": COLORS["accent2"],
                       "fontSize": "11px", "backgroundColor": "#2a1f14"}),
    ])

    # Linhas
    body_rows = []
    for r in rows_data:
        name = r["name"]
        if len(name) > 45:
            name = name[:42] + "..."

        cells = [html.Td(name, style={**td_style, "textAlign": "left", "fontWeight": "500"})]

        # Vendas recentes
        for d in recent_dates:
            val = r["recent_sales"].get(d, 0)
            bg = "#1a2332"
            if val > 0:
                intensity = min(val / 5, 1)
                bg = f"rgba(88, 166, 255, {0.08 + intensity * 0.2})"
            cells.append(html.Td(
                str(val) if val > 0 else "-",
                style={**td_style, "backgroundColor": bg,
                       "color": COLORS["accent"] if val > 0 else COLORS["text_muted"],
                       "fontWeight": "600" if val > 0 else "400"},
            ))

        # Total recente
        tr = r["total_recent_7d"]
        cells.append(html.Td(
            str(tr) if tr > 0 else "-",
            style={**td_style, "fontWeight": "700",
                   "color": COLORS["accent"] if tr > 0 else COLORS["text_muted"],
                   "backgroundColor": "#1a2332"},
        ))

        # Separador
        cells.append(html.Td("", style={**td_style, "width": "4px", "padding": "0",
                                         "backgroundColor": COLORS["accent"], "minWidth": "4px"}))

        # Previsao
        for d in forecast_dates:
            val = r["forecast"].get(d, 0)
            bg = "#2a1f14"
            if val > 0.1:
                intensity = min(val / 5, 1)
                bg = f"rgba(249, 115, 22, {0.08 + intensity * 0.2})"
            cells.append(html.Td(
                f"{val:.1f}" if val > 0.05 else "-",
                style={**td_style, "backgroundColor": bg,
                       "color": COLORS["accent2"] if val > 0.05 else COLORS["text_muted"],
                       "fontWeight": "600" if val > 0.05 else "400"},
            ))

        # Total previsao
        tp = r["total_prev_7d"]
        cells.append(html.Td(
            f"{tp:.1f}" if tp > 0.05 else "-",
            style={**td_style, "fontWeight": "700",
                   "color": COLORS["accent2"] if tp > 0.05 else COLORS["text_muted"],
                   "backgroundColor": "#2a1f14"},
        ))

        body_rows.append(html.Tr(cells))

    return html.Table(
        [html.Thead([group_header, html.Tr(header_cells)]), html.Tbody(body_rows)],
        style={"width": "100%", "borderCollapse": "collapse", "tableLayout": "auto"},
    )


# --- Atualizar dropdown de produtos baseado nas categorias e tab ---
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
        {"label": f"{r['product_name']}  ({int(r['quantity_sold'])} vendidos)",
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
        xaxis_title="Data", yaxis_title="Quantidade Vendida",
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
                mode="lines", name=f"{cat} (historico)",
                line=dict(color=color, width=2),
                legendgroup=cat,
            ))

        if not p_daily.empty:
            fig.add_trace(go.Scatter(
                x=p_daily["order_date"], y=p_daily["predicted_quantity"],
                mode="lines+markers", name=f"{cat} (previsao)",
                line=dict(color=color, width=2.5, dash="dash"),
                marker=dict(size=4),
                legendgroup=cat,
            ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        xaxis_title="Data", yaxis_title="Quantidade",
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
        texttemplate="%{x:.0f}", textposition="outside", textfont_size=11,
    ))

    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(
        margin=dict(l=10, r=40, t=10, b=30),
        showlegend=False,
        yaxis_title="", xaxis_title="Quantidade Vendida",
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

    # --- Linha REAL (azul) - todo o historico diario ---
    if not h.empty:
        h_agg = h.groupby("order_date")["quantity_sold"].sum().reset_index()

        fig.add_trace(go.Scatter(
            x=h_agg["order_date"], y=h_agg["quantity_sold"],
            mode="lines", name="real",
            line=dict(color="#4A90D9", width=1.5),
        ))

    # --- Linha PREDICT (laranja) + intervalo de confianca ---
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
                fillcolor="rgba(245, 166, 35, 0.15)",
                line=dict(color="rgba(0,0,0,0)"),
                name="intervalo 80%",
                showlegend=True,
                hoverinfo="skip",
            ))

        fig.add_trace(go.Scatter(
            x=p_plot["order_date"], y=p_plot["predicted_quantity"],
            mode="lines", name="predict",
            line=dict(color="#F5A623", width=2),
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
    monthly = filtered.groupby("month")["revenue"].sum().reset_index()

    fig.add_trace(go.Bar(
        x=monthly["month"], y=monthly["revenue"],
        marker_color=COLORS["accent3"], marker_line_width=0, opacity=0.85,
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(xaxis_title="Mes", yaxis_title="Receita ($)", showlegend=False)
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

    weekday_names = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    filtered = filter_by_categories(hist_df, selected_cats, product_cat_map).copy()
    filtered = filter_by_event_tab(filtered, tab_value)
    filtered["weekday"] = filtered["order_date"].dt.dayofweek
    wd = filtered.groupby("weekday")["quantity_sold"].sum().reset_index()
    wd["weekday_name"] = wd["weekday"].map(lambda x: weekday_names[x])

    colors = [COLORS["accent4"] if x >= 5 else COLORS["accent"] for x in wd["weekday"]]

    fig.add_trace(go.Bar(
        x=wd["weekday_name"], y=wd["quantity_sold"],
        marker_color=colors, marker_line_width=0,
    ))
    fig.update_layout(**PLOT_LAYOUT)
    fig.update_layout(xaxis_title="", yaxis_title="Quantidade", showlegend=False)
    return fig


# --- Tabela de metricas ---
@callback(
    Output("metrics-table", "children"),
    Input("category-filter", "value"),
    Input("event-tabs", "value"),
)
def update_metrics_table(selected_cats, tab_value):
    if not selected_cats:
        return html.P("Selecione ao menos uma categoria.", style={"color": COLORS["text_muted"]})

    # Filtrar metricas por categorias (multi-categoria)
    filtered_metrics = filter_by_categories(metrics_df, selected_cats, product_cat_map)
    filtered_metrics = filter_by_event_tab(filtered_metrics, tab_value)

    if filtered_metrics.empty:
        return html.P("Nenhum produto encontrado nas categorias selecionadas.", style={"color": COLORS["text_muted"]})

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
        labels = {"gradient_boosting": "ML", "weighted_average": "Media"}
        return labels.get(str(val), str(val)[:10])

    header_cells = [
        html.Th("Produto", style=header_style),
        html.Th("Categorias", style=header_style),
        html.Th("MAE", style={**header_style, "textAlign": "right"}),
        html.Th("RMSE", style={**header_style, "textAlign": "right"}),
        html.Th("R2", style={**header_style, "textAlign": "right"}),
        html.Th("Prev. 30d", style={**header_style, "textAlign": "right"}),
        html.Th("Media/Dia", style={**header_style, "textAlign": "right"}),
    ]
    if has_method:
        header_cells.append(html.Th("Metodo", style={**header_style, "textAlign": "center"}))
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
            html.Td(cat_display, style={**cell_style, "color": COLORS["accent4"], "fontSize": "12px"}),
            html.Td(f"{row['mae']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(f"{row['rmse']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(f"{row['r2_score']:.3f}", style={**cell_style, "textAlign": "right", "color": r2_color(row["r2_score"]), "fontWeight": "600"}),
            html.Td(f"{row['total_prev']:.1f}", style={**cell_style, "textAlign": "right", "color": COLORS["accent2"], "fontWeight": "600"}),
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
# RUN
# ============================================================

if __name__ == "__main__":
    print("\n  Dashboard disponivel em: http://localhost:8050\n")
    app.run(debug=True, port=8050)
