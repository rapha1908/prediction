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

            # KPIs
            html.Div(
                style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px"},
                children=[
                    kpi_card("Produtos", str(total_products), color=COLORS["accent"]),
                    kpi_card("Vendas Totais", f"{total_sales_qty:,}".replace(",", "."), color=COLORS["accent3"]),
                    kpi_card("Receita Total", f"$ {total_revenue:,.2f}", color=COLORS["accent2"]),
                    kpi_card("Categorias", str(len(all_categories)), color=COLORS["accent4"]),
                    kpi_card("Previsao 30d", f"{pred_total_qty:,.0f} un.", color=COLORS["accent4"]),
                ],
            ),

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
                            options=[{"label": c, "value": c} for c in all_categories],
                            value=all_categories,
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

# --- Atualizar dropdown de produtos baseado nas categorias ---
@callback(
    Output("product-selector", "options"),
    Output("product-selector", "value"),
    Input("category-filter", "value"),
)
def update_product_options(selected_cats):
    if not selected_cats:
        return [], None
    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
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
)
def update_category_timeline(selected_cats, granularity):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Explodir categorias para agrupar corretamente por categoria individual
    exploded = explode_categories(hist_df)
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
)
def update_category_forecast(selected_cats):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    # Explodir historico e previsao
    hist_exp = explode_categories(hist_df)
    pred_exp = explode_categories(pred_df)

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
)
def update_top_products(selected_cats):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_categories(product_sales, selected_cats, product_cat_map)
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

    # --- Linha PREDICT (laranja) - previsao futura ---
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
)
def update_monthly_revenue(selected_cats):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    filtered = filter_by_categories(hist_df, selected_cats, product_cat_map).copy()
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
)
def update_weekday_chart(selected_cats):
    fig = go.Figure()
    if not selected_cats:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    weekday_names = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    filtered = filter_by_categories(hist_df, selected_cats, product_cat_map).copy()
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
)
def update_metrics_table(selected_cats):
    if not selected_cats:
        return html.P("Selecione ao menos uma categoria.", style={"color": COLORS["text_muted"]})

    # Filtrar metricas por categorias (multi-categoria)
    filtered_metrics = filter_by_categories(metrics_df, selected_cats, product_cat_map)

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
