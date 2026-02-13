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
            print(f"Arquivo não encontrado: {path}")
            print("Execute primeiro: py main.py")
            sys.exit(1)

    hist = pd.read_csv(files["historico"], parse_dates=["order_date"])
    pred = pd.read_csv(files["previsoes"], parse_dates=["order_date"])
    metrics = pd.read_csv(files["metricas"])

    return hist, pred, metrics


hist_df, pred_df, metrics_df = load_data()

# ============================================================
# PRÉ-PROCESSAR
# ============================================================

# Lista de produtos únicos (ordenar por total vendido)
product_sales = (
    hist_df.groupby(["product_id", "product_name"])["quantity_sold"]
    .sum().reset_index()
    .sort_values("quantity_sold", ascending=False)
)
product_options = [
    {"label": f"{row['product_name']}  ({int(row['quantity_sold'])} vendidos)",
     "value": str(row["product_id"])}
    for _, row in product_sales.iterrows()
]

# KPIs gerais
total_products = hist_df["product_id"].nunique()
total_sales_qty = int(hist_df["quantity_sold"].sum())
total_revenue = hist_df["revenue"].sum()
total_orders_days = hist_df["order_date"].nunique()
date_range_str = f"{hist_df['order_date'].min().strftime('%d/%m/%Y')} — {hist_df['order_date'].max().strftime('%d/%m/%Y')}"

# Previsão totais
pred_total_qty = pred_df["predicted_quantity"].sum()

# Top 10 produtos
top10 = product_sales.head(10)

# Vendas diárias totais
daily_total = hist_df.groupby("order_date").agg(
    quantity=("quantity_sold", "sum"),
    revenue=("revenue", "sum"),
).reset_index()

# Vendas semanais totais
weekly_total = daily_total.copy()
weekly_total["week"] = weekly_total["order_date"].dt.to_period("W").apply(lambda r: r.start_time)
weekly_total = weekly_total.groupby("week").agg(
    quantity=("quantity", "sum"),
    revenue=("revenue", "sum"),
).reset_index()

# Métricas resumidas (agregar por product_id, pegar melhor nome)
metrics_summary = (
    metrics_df.groupby("product_id")
    .agg(
        product_name=("product_name", "first"),
        mae=("mae", "mean"),
        rmse=("rmse", "mean"),
        r2_score=("r2_score", "max"),
    )
    .reset_index()
    .sort_values("mae", ascending=True)
)

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
    yaxis=dict(gridcolor=COLORS["grid"], showline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

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
        style=card_style({"textAlign": "center", "flex": "1", "minWidth": "180px"}),
        children=[
            html.P(title, style={
                "color": COLORS["text_muted"], "fontSize": "13px",
                "marginBottom": "4px", "textTransform": "uppercase",
                "letterSpacing": "0.5px", "fontWeight": "500",
            }),
            html.H2(value, style={
                "color": color, "margin": "8px 0 4px",
                "fontSize": "28px", "fontWeight": "700",
            }),
            html.P(subtitle, style={
                "color": COLORS["text_muted"], "fontSize": "12px", "margin": "0",
            }) if subtitle else None,
        ],
    )


# ============================================================
# APP DASH
# ============================================================

app = Dash(__name__)
app.title = "Dashboard de Previsão de Vendas"

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
                "padding": "32px 48px", "borderBottom": f"1px solid {COLORS['card_border']}",
            },
            children=[
                html.H1("Previsão de Vendas", style={
                    "margin": "0 0 4px", "fontSize": "28px", "fontWeight": "700",
                    "background": "linear-gradient(90deg, #58a6ff, #a855f7)",
                    "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                }),
                html.P(f"Dados de {date_range_str}", style={
                    "color": COLORS["text_muted"], "margin": "0", "fontSize": "14px",
                }),
            ],
        ),

        # --- CONTEÚDO ---
        html.Div(style={"padding": "32px 48px", "maxWidth": "1400px", "margin": "0 auto"}, children=[

            # KPI CARDS
            html.Div(
                style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "32px"},
                children=[
                    kpi_card("Produtos", str(total_products), color=COLORS["accent"]),
                    kpi_card("Vendas Totais", f"{total_sales_qty:,}".replace(",", "."), color=COLORS["accent3"]),
                    kpi_card("Receita Total", f"$ {total_revenue:,.2f}", color=COLORS["accent2"]),
                    kpi_card("Previsão 30d", f"{pred_total_qty:,.0f} un.", "Quantidade prevista", color=COLORS["accent4"]),
                    kpi_card("Dias com Dados", str(total_orders_days), color=COLORS["accent"]),
                ],
            ),

            # LINHA 1: Vendas ao longo do tempo + Top 10
            html.Div(style={"display": "grid", "gridTemplateColumns": "2fr 1fr", "gap": "24px", "marginBottom": "32px"}, children=[

                # Gráfico de vendas ao longo do tempo
                html.Div(style=card_style(), children=[
                    html.H3("Vendas ao Longo do Tempo", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                    dcc.RadioItems(
                        id="time-granularity",
                        options=[
                            {"label": " Diário", "value": "daily"},
                            {"label": " Semanal", "value": "weekly"},
                        ],
                        value="weekly",
                        inline=True,
                        style={"marginBottom": "12px", "fontSize": "13px"},
                        inputStyle={"marginRight": "4px"},
                        labelStyle={"marginRight": "20px", "cursor": "pointer"},
                    ),
                    dcc.Graph(id="sales-timeline", config={"displayModeBar": False}),
                ]),

                # Top 10 Produtos
                html.Div(style=card_style(), children=[
                    html.H3("Top 10 Produtos", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                    dcc.Graph(
                        id="top10-chart",
                        figure=px.bar(
                            top10.iloc[::-1],
                            x="quantity_sold", y="product_name",
                            orientation="h",
                            color="quantity_sold",
                            color_continuous_scale=["#1e3a5f", "#58a6ff"],
                        ).update_layout(
                            **{k: v for k, v in PLOT_LAYOUT.items() if k != "margin"},
                            showlegend=False, coloraxis_showscale=False,
                            yaxis_title="", xaxis_title="Quantidade Vendida",
                            margin=dict(l=10, r=20, t=10, b=40),
                        ).update_traces(
                            texttemplate="%{x:.0f}", textposition="outside",
                            textfont_size=11,
                        ),
                        config={"displayModeBar": False},
                    ),
                ]),
            ]),

            # LINHA 2: Previsão por Produto
            html.Div(style=card_style({"marginBottom": "32px"}), children=[
                html.H3("Histórico + Previsão por Produto", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                html.Div(style={"display": "flex", "gap": "16px", "alignItems": "center", "marginBottom": "16px", "flexWrap": "wrap"}, children=[
                    html.Label("Produto:", style={"fontSize": "13px", "color": COLORS["text_muted"]}),
                    dcc.Dropdown(
                        id="product-selector",
                        options=product_options,
                        value=str(product_sales.iloc[0]["product_id"]) if len(product_sales) > 0 else None,
                        style={
                            "width": "500px", "backgroundColor": COLORS["bg"],
                            "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                            "borderRadius": "8px",
                        },
                        className="dash-dropdown",
                    ),
                ]),
                dcc.Graph(id="product-forecast", config={"displayModeBar": False}),
            ]),

            # LINHA 3: Grid de previsões top 6 + Tabela de métricas
            html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "32px"}, children=[

                # Receita mensal
                html.Div(style=card_style(), children=[
                    html.H3("Receita Mensal", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                    dcc.Graph(id="monthly-revenue", config={"displayModeBar": False}),
                ]),

                # Distribuição dia da semana
                html.Div(style=card_style(), children=[
                    html.H3("Vendas por Dia da Semana", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                    dcc.Graph(id="weekday-chart", config={"displayModeBar": False}),
                ]),
            ]),

            # LINHA 4: Tabela de métricas dos modelos
            html.Div(style=card_style({"marginBottom": "32px"}), children=[
                html.H3("Métricas dos Modelos de Previsão", style={"margin": "0 0 16px", "fontSize": "16px", "fontWeight": "600"}),
                html.P("Mostrando produtos com maior volume de previsão", style={"color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px"}),
                html.Div(id="metrics-table", style={"overflowX": "auto"}),
            ]),

            # FOOTER
            html.Div(style={"textAlign": "center", "padding": "24px 0", "borderTop": f"1px solid {COLORS['card_border']}"}, children=[
                html.P("Dashboard de Previsão de Vendas — Powered by Plotly Dash", style={
                    "color": COLORS["text_muted"], "fontSize": "12px", "margin": "0",
                }),
            ]),
        ]),
    ],
)


# ============================================================
# CALLBACKS
# ============================================================

@callback(
    Output("sales-timeline", "figure"),
    Input("time-granularity", "value"),
)
def update_timeline(granularity):
    if granularity == "weekly":
        df = weekly_total
        x_col, label = "week", "Semana"
    else:
        df = daily_total
        x_col, label = "order_date", "Data"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x_col], y=df["quantity"],
        mode="lines", name="Quantidade",
        line=dict(color=COLORS["accent"], width=2),
        fill="tozeroy", fillcolor="rgba(88,166,255,0.08)",
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        xaxis_title=label, yaxis_title="Quantidade Vendida",
        showlegend=False,
        hovermode="x unified",
    )
    return fig


@callback(
    Output("product-forecast", "figure"),
    Input("product-selector", "value"),
)
def update_product_forecast(product_id):
    if product_id is None:
        return go.Figure().update_layout(**PLOT_LAYOUT)

    pid = int(product_id)

    # Histórico
    h = hist_df[hist_df["product_id"] == pid].sort_values("order_date")
    # Previsão
    p = pred_df[pred_df["product_id"] == pid].sort_values("order_date")

    fig = go.Figure()

    if not h.empty:
        # Média móvel 7 dias
        h_agg = h.groupby("order_date")["quantity_sold"].sum().reset_index()
        h_agg["rolling_7d"] = h_agg["quantity_sold"].rolling(7, min_periods=1).mean()

        fig.add_trace(go.Scatter(
            x=h_agg["order_date"], y=h_agg["quantity_sold"],
            mode="lines", name="Vendas Diárias",
            line=dict(color=COLORS["accent"], width=1.2),
            opacity=0.4,
        ))
        fig.add_trace(go.Scatter(
            x=h_agg["order_date"], y=h_agg["rolling_7d"],
            mode="lines", name="Média Móvel 7d",
            line=dict(color=COLORS["accent"], width=2.5),
        ))

        # Linha vertical separando histórico de previsão
        last_date = h_agg["order_date"].max()
        fig.add_trace(go.Scatter(
            x=[last_date, last_date],
            y=[0, max(h_agg["quantity_sold"].max(), 1)],
            mode="lines", name="Início Previsão",
            line=dict(color=COLORS["text_muted"], width=1.5, dash="dot"),
            showlegend=True,
        ))

    if not p.empty:
        fig.add_trace(go.Scatter(
            x=p["order_date"], y=p["predicted_quantity"],
            mode="lines+markers", name="Previsão",
            line=dict(color=COLORS["accent2"], width=2.5, dash="dash"),
            marker=dict(size=4),
            fill="tozeroy", fillcolor="rgba(249,115,22,0.06)",
        ))

    product_name = h["product_name"].iloc[0] if not h.empty else (p["product_name"].iloc[0] if not p.empty else "")

    fig.update_layout(
        **PLOT_LAYOUT,
        xaxis_title="Data",
        yaxis_title="Quantidade",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


@callback(
    Output("monthly-revenue", "figure"),
    Input("time-granularity", "value"),  # dummy trigger
)
def update_monthly_revenue(_):
    monthly = hist_df.copy()
    monthly["month"] = monthly["order_date"].dt.to_period("M").apply(lambda r: r.start_time)
    monthly = monthly.groupby("month")["revenue"].sum().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly["month"], y=monthly["revenue"],
        marker_color=COLORS["accent3"],
        marker_line_width=0,
        opacity=0.85,
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        xaxis_title="Mês", yaxis_title="Receita ($)",
        showlegend=False,
    )
    return fig


@callback(
    Output("weekday-chart", "figure"),
    Input("time-granularity", "value"),  # dummy trigger
)
def update_weekday_chart(_):
    weekday_names = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    wd = hist_df.copy()
    wd["weekday"] = wd["order_date"].dt.dayofweek
    wd_agg = wd.groupby("weekday")["quantity_sold"].sum().reset_index()
    wd_agg["weekday_name"] = wd_agg["weekday"].map(lambda x: weekday_names[x])

    colors = [COLORS["accent4"] if x >= 5 else COLORS["accent"] for x in wd_agg["weekday"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=wd_agg["weekday_name"], y=wd_agg["quantity_sold"],
        marker_color=colors,
        marker_line_width=0,
    ))
    fig.update_layout(
        **PLOT_LAYOUT,
        xaxis_title="", yaxis_title="Quantidade",
        showlegend=False,
    )
    return fig


@callback(
    Output("metrics-table", "children"),
    Input("time-granularity", "value"),  # dummy trigger
)
def update_metrics_table(_):
    # Juntar com previsão total por produto
    pred_summary = (
        pred_df.groupby("product_id")
        .agg(total_previsto=("predicted_quantity", "sum"), media_diaria=("predicted_quantity", "mean"))
        .reset_index()
    )

    table_df = metrics_summary.merge(pred_summary, on="product_id", how="left").fillna(0)
    table_df = table_df.sort_values("total_previsto", ascending=False).head(30)

    header_style = {
        "padding": "12px 16px", "textAlign": "left", "fontSize": "12px",
        "color": COLORS["text_muted"], "textTransform": "uppercase",
        "letterSpacing": "0.5px", "fontWeight": "600",
        "borderBottom": f"2px solid {COLORS['card_border']}",
        "position": "sticky", "top": "0", "backgroundColor": COLORS["card"],
    }
    cell_style = {
        "padding": "10px 16px", "fontSize": "13px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
    }

    def r2_color(val):
        if val >= 0.5:
            return COLORS["accent3"]
        elif val >= 0:
            return COLORS["accent"]
        return COLORS["red"]

    header = html.Tr([
        html.Th("Produto", style=header_style),
        html.Th("MAE", style={**header_style, "textAlign": "right"}),
        html.Th("RMSE", style={**header_style, "textAlign": "right"}),
        html.Th("R²", style={**header_style, "textAlign": "right"}),
        html.Th("Prev. 30d (total)", style={**header_style, "textAlign": "right"}),
        html.Th("Prev. Média/Dia", style={**header_style, "textAlign": "right"}),
    ])

    rows = []
    for _, row in table_df.iterrows():
        name = row["product_name"]
        if len(name) > 55:
            name = name[:52] + "..."
        rows.append(html.Tr([
            html.Td(name, style=cell_style),
            html.Td(f"{row['mae']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(f"{row['rmse']:.2f}", style={**cell_style, "textAlign": "right"}),
            html.Td(
                f"{row['r2_score']:.3f}",
                style={**cell_style, "textAlign": "right", "color": r2_color(row["r2_score"]), "fontWeight": "600"},
            ),
            html.Td(f"{row['total_previsto']:.1f}", style={**cell_style, "textAlign": "right", "color": COLORS["accent2"], "fontWeight": "600"}),
            html.Td(f"{row['media_diaria']:.2f}", style={**cell_style, "textAlign": "right"}),
        ]))

    return html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    print("\n  Dashboard disponível em: http://localhost:8050\n")
    app.run(debug=True, port=8050)
