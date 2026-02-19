"""Google Analytics & Google Ads page – layout and callbacks."""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import html, dcc, callback, Output, Input, State, no_update, ctx

import ga4_loader
import ga4_trainer
from config import (
    COLORS, FONT, PLOT_LAYOUT, H_LEGEND,
    card_style, section_label, kpi_card, _th_style, _td_style,
)


# ── Color maps ──

_CH_COLORS = {
    "Paid Search": "#34A853", "Organic Search": "#5aaa88",
    "Direct": "#c8a44e", "Referral": "#6ea8d9",
    "Organic Social": "#E1306C", "Paid Social": "#4267B2",
    "Email": "#FF7A59", "Display": "#FBBC04",
    "Affiliates": "#a67ed6", "Cross-network": "#EA4335",
    "Paid Other": "#b87348", "Unassigned": "#8a847a",
    "Organic Video": "#FF0000",
}

GOOGLE_BLUE = "#4285F4"
GOOGLE_GREEN = "#34A853"
GOOGLE_YELLOW = "#FBBC04"
GOOGLE_RED = "#EA4335"


def _empty_fig(height=280):
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        xaxis=dict(showgrid=False, showline=False, zeroline=False),
        yaxis=dict(showgrid=False, showline=False, zeroline=False),
        height=height, margin=dict(l=40, r=20, t=20, b=40),
    )
    return fig


def _no_data(msg="No data available"):
    return html.P(msg, style={
        "color": COLORS["text_muted"], "fontSize": "13px",
        "textAlign": "center", "padding": "60px 0",
    })


# ============================================================
# LAYOUT
# ============================================================

def layout():
    """Return the Google Analytics page layout."""
    if not ga4_loader.is_configured():
        return [
            dcc.Link("< Back to Dashboard", href="/", style={
                "color": COLORS["text_muted"], "fontSize": "13px",
                "textDecoration": "none", "marginBottom": "8px", "display": "block",
            }),
            html.Div(style=card_style({"textAlign": "center", "padding": "60px"}), children=[
                html.H3("Google Analytics not configured", style={"color": COLORS["text_muted"]}),
                html.P("Add GA4_PROPERTY_ID and ga4-credentials.json to your .env file.",
                       style={"color": COLORS["text_muted"], "fontSize": "14px"}),
            ]),
        ]

    return [
        # ── Header ──
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                        "marginBottom": "28px", "flexWrap": "wrap", "gap": "16px"}, children=[
            html.Div(children=[
                dcc.Link("< Back to Dashboard", href="/", style={
                    "color": COLORS["text_muted"], "fontSize": "13px",
                    "textDecoration": "none", "marginBottom": "8px", "display": "block",
                }),
                section_label("GOOGLE ANALYTICS & ADS"),
                html.H2("Marketing Performance", style={
                    "margin": "0 0 4px", "fontSize": "24px", "fontWeight": "700",
                    "background": f"linear-gradient(90deg, {GOOGLE_BLUE}, {GOOGLE_GREEN}, {GOOGLE_YELLOW}, {GOOGLE_RED})",
                    "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                }),
                html.P("Traffic, channels, campaigns, and ad spend analytics",
                       style={"color": COLORS["text_muted"], "fontSize": "14px", "margin": "0"}),
            ]),
            html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center"}, children=[
                dcc.Dropdown(
                    id="ga-period",
                    options=[
                        {"label": "Last 7 days", "value": "7daysAgo"},
                        {"label": "Last 30 days", "value": "30daysAgo"},
                        {"label": "Last 90 days", "value": "90daysAgo"},
                        {"label": "Last 6 months", "value": "180daysAgo"},
                        {"label": "Last 12 months", "value": "365daysAgo"},
                    ],
                    value="30daysAgo",
                    clearable=False,
                    style={"width": "180px", "fontSize": "12px", "backgroundColor": COLORS["bg"]},
                ),
                html.Button("Refresh", id="ga-refresh-btn", n_clicks=0, style={
                    "backgroundColor": GOOGLE_GREEN, "color": "#fff",
                    "border": "none", "borderRadius": "8px",
                    "padding": "10px 20px", "fontSize": "13px",
                    "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                }),
            ]),
        ]),

        # ── Traffic KPIs ──
        html.Div(id="ga-traffic-kpis", style={
            "display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "24px",
        }),

        # ── Google Ads KPIs ──
        html.Div(id="ga-ads-kpis", style={
            "display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px",
        }),

        # ── Row 1: Traffic trend + Channel breakdown ──
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_BLUE}"}), children=[
                section_label("WEBSITE TRAFFIC"),
                html.H3("Daily Sessions & Users", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                dcc.Graph(id="ga-traffic-chart", config={"displayModeBar": False}, style={"height": "300px"}),
            ]),
            html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_GREEN}"}), children=[
                section_label("CHANNELS"),
                html.H3("Traffic by Channel Group", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                dcc.Graph(id="ga-channel-chart", config={"displayModeBar": False}, style={"height": "300px"}),
            ]),
        ]),

        # ── Row 2: Google Ads campaigns table (full width) ──
        html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_GREEN}", "marginBottom": "24px"}), children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                            "marginBottom": "12px"}, children=[
                html.Div(children=[
                    section_label("GOOGLE ADS"),
                    html.H3("Campaign Performance", style={"margin": "0", "fontSize": "16px", "fontWeight": "600"}),
                ]),
                html.Button("Export CSV", id="ga-campaigns-export-btn", n_clicks=0, style={
                    "backgroundColor": "transparent", "color": GOOGLE_GREEN,
                    "border": f"1px solid {GOOGLE_GREEN}", "borderRadius": "6px",
                    "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                    "cursor": "pointer", "fontFamily": FONT,
                }),
            ]),
            dcc.Download(id="ga-campaigns-download"),
            html.Div(id="ga-campaigns-table", style={"maxHeight": "420px", "overflowY": "auto", "overflowX": "auto"}),
        ]),

        # ── Row 2b: Daily ad spend chart (full width) ──
        html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_BLUE}", "marginBottom": "24px"}), children=[
            section_label("AD SPEND"),
            html.H3("Daily Cost & Conversions", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
            dcc.Graph(id="ga-ads-daily-chart", config={"displayModeBar": False}, style={"height": "340px"}),
        ]),

        # ── Row 3: Source/Medium + Landing pages ──
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "24px"}, children=[
            html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_YELLOW}"}), children=[
                section_label("SOURCE / MEDIUM"),
                html.H3("Traffic Sources Detail", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                html.Div(id="ga-source-medium-table", style={"maxHeight": "380px", "overflowY": "auto"}),
            ]),
            html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_RED}"}), children=[
                section_label("LANDING PAGES"),
                html.H3("Top Entry Pages", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                html.Div(id="ga-landing-table", style={"maxHeight": "380px", "overflowY": "auto"}),
            ]),
        ]),

        # ── Row 4: Channel revenue pie + ROAS comparison ──
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px", "marginBottom": "28px"}, children=[
            html.Div(style=card_style({"borderTop": f"3px solid {COLORS['accent']}"}), children=[
                section_label("REVENUE"),
                html.H3("Revenue by Channel", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                dcc.Graph(id="ga-revenue-pie", config={"displayModeBar": False}, style={"height": "340px"}),
            ]),
            html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_GREEN}"}), children=[
                section_label("CAMPAIGN ROAS"),
                html.H3("Return on Ad Spend", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
                dcc.Graph(id="ga-roas-chart", config={"displayModeBar": False}, style={"height": "340px"}),
            ]),
        ]),

        # ════════════════════════════════════════════════════════════
        # TRAINING & FORECASTING SECTION
        # ════════════════════════════════════════════════════════════

        html.Hr(style={"border": "none", "borderTop": f"2px solid {COLORS['card_border']}", "margin": "10px 0 28px"}),

        # ── Training controls ──
        html.Div(style=card_style({"borderTop": f"3px solid {COLORS['accent']}", "marginBottom": "24px"}), children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                            "flexWrap": "wrap", "gap": "16px"}, children=[
                html.Div(children=[
                    section_label("FORECASTING"),
                    html.H3("GA4-Enhanced Sales Prediction", style={
                        "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                    }),
                    html.P("Train Prophet models with GA4 traffic and Google Ads data as regressors, then compare against the base model.",
                           style={"color": COLORS["text_muted"], "fontSize": "13px", "margin": "0", "maxWidth": "600px"}),
                ]),
                html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center"}, children=[
                    dcc.Dropdown(
                        id="ga-train-period",
                        options=[
                            {"label": "Last 90 days", "value": 90},
                            {"label": "Last 180 days", "value": 180},
                            {"label": "Last 365 days", "value": 365},
                        ],
                        value=90,
                        clearable=False,
                        style={"width": "170px", "fontSize": "12px", "backgroundColor": COLORS["bg"]},
                    ),
                    html.Button("Train Models", id="ga-train-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent"], "color": COLORS["bg"],
                        "border": "none", "borderRadius": "8px",
                        "padding": "12px 28px", "fontSize": "13px",
                        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                    }),
                ]),
            ]),
            html.Div(id="ga-train-status", style={"marginTop": "16px"}),
            dcc.Interval(id="ga-train-poll", interval=1500, disabled=True),
        ]),

        # ── Comparison KPIs ──
        html.Div(id="ga-compare-kpis", style={
            "display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "24px",
        }),

        # ── Comparison table (full width) ──
        html.Div(id="ga-compare-table-container", style={"marginBottom": "24px"}),

        # ── Comparison charts: R2 bars + MAE scatter ──
        html.Div(id="ga-compare-charts-container", style={"marginBottom": "24px"}),

        # ── Forecast Viewer ──
        html.Div(id="ga-forecast-viewer-container"),
    ]


# ============================================================
# CALLBACKS
# ============================================================

@callback(
    Output("ga-traffic-kpis", "children"),
    Output("ga-ads-kpis", "children"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_kpis(period, _refresh):
    if _refresh and _refresh > 0:
        ga4_loader.invalidate_cache()

    period = period or "30daysAgo"
    traffic_kpis = []
    ads_kpis = []

    try:
        df = ga4_loader.get_traffic_overview(period)
        if not df.empty:
            traffic_kpis = [
                kpi_card("Sessions", f"{int(df['sessions'].sum()):,}", color=GOOGLE_BLUE),
                kpi_card("Users", f"{int(df['totalUsers'].sum()):,}", color=GOOGLE_GREEN),
                kpi_card("Page Views", f"{int(df['screenPageViews'].sum()):,}", color=GOOGLE_YELLOW),
                kpi_card("Bounce Rate", f"{df['bounceRate'].mean():.1f}%", color=GOOGLE_RED),
                kpi_card("Avg Duration", f"{df['averageSessionDuration'].mean():.0f}s", color=COLORS["accent3"]),
            ]
    except Exception:
        pass

    try:
        ads_df = ga4_loader.get_google_ads_daily(period)
        if not ads_df.empty:
            total_cost = ads_df["cost"].sum()
            total_clicks = int(ads_df["clicks"].sum())
            total_impr = int(ads_df["impressions"].sum())
            total_conv = ads_df["conversions"].sum()
            total_rev = ads_df["purchaseRevenue"].sum()
            roas = (total_rev / total_cost) if total_cost > 0 else 0
            cpc = (total_cost / total_clicks) if total_clicks > 0 else 0
            ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0
            ads_kpis = [
                kpi_card("Ad Spend", f"${total_cost:,.0f}", color=GOOGLE_GREEN),
                kpi_card("Clicks", f"{total_clicks:,}", color=GOOGLE_BLUE),
                kpi_card("Impressions", f"{total_impr:,}", color=GOOGLE_YELLOW),
                kpi_card("CTR", f"{ctr:.2f}%", color=COLORS["accent"]),
                kpi_card("CPC", f"${cpc:.2f}", color=COLORS["accent4"]),
                kpi_card("Conversions", f"{total_conv:,.0f}", color=GOOGLE_RED),
                kpi_card("Ad Revenue", f"${total_rev:,.0f}", color=COLORS["accent3"]),
                kpi_card("ROAS", f"{roas:.1f}x",
                         color=GOOGLE_GREEN if roas >= 1 else COLORS["red"]),
            ]
    except Exception:
        pass

    return traffic_kpis, ads_kpis


@callback(
    Output("ga-traffic-chart", "figure"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_traffic_chart(period, _):
    period = period or "30daysAgo"
    fig = _empty_fig(300)
    try:
        df = ga4_loader.get_traffic_overview(period)
        if not df.empty:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["sessions"],
                mode="lines", name="Sessions",
                line=dict(color=GOOGLE_BLUE, width=2),
                fill="tozeroy", fillcolor="rgba(66,133,244,0.08)",
                hovertemplate="%{x|%b %d}<br>Sessions: %{y:,}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["totalUsers"],
                mode="lines", name="Users",
                line=dict(color=GOOGLE_GREEN, width=2, dash="dot"),
                hovertemplate="%{x|%b %d}<br>Users: %{y:,}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["screenPageViews"],
                mode="lines", name="Page Views",
                line=dict(color=GOOGLE_YELLOW, width=1.5, dash="dash"),
                visible="legendonly",
                hovertemplate="%{x|%b %d}<br>Page Views: %{y:,}<extra></extra>",
            ))
    except Exception:
        pass
    fig.update_layout(
        showlegend=True, legend=H_LEGEND,
        xaxis=dict(gridcolor=COLORS["grid"], showline=False),
        yaxis=dict(gridcolor=COLORS["grid"], showline=False, rangemode="tozero"),
        hovermode="x unified",
    )
    return fig


@callback(
    Output("ga-channel-chart", "figure"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_channel_chart(period, _):
    period = period or "30daysAgo"
    fig = _empty_fig(300)
    try:
        df = ga4_loader.get_channel_breakdown(period)
        if not df.empty:
            total_sess = df["sessions"].sum()
            df["pct"] = (df["sessions"] / total_sess * 100).round(1) if total_sess > 0 else 0
            df = df.sort_values("sessions", ascending=True)
            colors = [_CH_COLORS.get(lbl, COLORS["text_muted"]) for lbl in df["sessionDefaultChannelGroup"]]

            fig.add_trace(go.Bar(
                x=df["sessions"],
                y=df["sessionDefaultChannelGroup"],
                orientation="h",
                marker_color=colors,
                marker_line_width=0,
                text=df.apply(lambda r: f"{int(r['sessions']):,}  ({r['pct']}%)", axis=1),
                textposition="auto",
                textfont=dict(size=11, color="#fff"),
                hovertemplate="<b>%{y}</b><br>Sessions: %{x:,}<br>Users: %{customdata[0]:,}<br>Conversions: %{customdata[1]:,.0f}<br>Revenue: $%{customdata[2]:,.0f}<extra></extra>",
                customdata=df[["totalUsers", "conversions", "purchaseRevenue"]].values,
            ))
    except Exception:
        pass
    fig.update_layout(
        showlegend=False,
        margin=dict(l=140, r=20, t=10, b=10),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showline=False),
        bargap=0.25,
    )
    return fig


@callback(
    Output("ga-campaigns-table", "children"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_campaigns_table(period, _):
    period = period or "30daysAgo"
    try:
        df = ga4_loader.get_google_ads_campaigns(period)
        if df.empty:
            return _no_data("No Google Ads campaigns found")

        rows = []
        for _, r in df.iterrows():
            roas_color = GOOGLE_GREEN if r["roas"] >= 2 else (GOOGLE_YELLOW if r["roas"] >= 1 else COLORS["red"])
            rows.append(html.Tr([
                html.Td(r["campaign"], style=_td_style({"fontWeight": "600", "maxWidth": "220px",
                         "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"})),
                html.Td(f"{int(r['sessions']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{int(r['clicks']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{int(r['impressions']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"${r['cost']:,.0f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['conversions']:.0f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"${r['purchaseRevenue']:,.0f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"${r['cpc']:.2f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['ctr']:.1f}%", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['roas']:.1f}x", style=_td_style({"textAlign": "right", "color": roas_color, "fontWeight": "700"})),
            ]))

        return html.Table(style={"width": "100%", "borderCollapse": "collapse"}, children=[
            html.Thead(html.Tr([
                html.Th("Campaign", style=_th_style()),
                html.Th("Sessions", style=_th_style({"textAlign": "right"})),
                html.Th("Clicks", style=_th_style({"textAlign": "right"})),
                html.Th("Impr.", style=_th_style({"textAlign": "right"})),
                html.Th("Cost", style=_th_style({"textAlign": "right"})),
                html.Th("Conv.", style=_th_style({"textAlign": "right"})),
                html.Th("Revenue", style=_th_style({"textAlign": "right"})),
                html.Th("CPC", style=_th_style({"textAlign": "right"})),
                html.Th("CTR", style=_th_style({"textAlign": "right"})),
                html.Th("ROAS", style=_th_style({"textAlign": "right"})),
            ])),
            html.Tbody(rows),
        ])
    except Exception:
        return _no_data("Could not load Google Ads data")


@callback(
    Output("ga-campaigns-download", "data"),
    Input("ga-campaigns-export-btn", "n_clicks"),
    State("ga-period", "value"),
    prevent_initial_call=True,
)
def export_campaigns(n_clicks, period):
    if not n_clicks:
        return no_update
    try:
        df = ga4_loader.get_google_ads_campaigns(period or "30daysAgo")
        if df.empty:
            return no_update
        export = df[["campaign", "sessions", "clicks", "impressions", "cost",
                      "conversions", "purchaseRevenue", "cpc", "ctr", "roas"]].copy()
        export.columns = ["Campaign", "Sessions", "Clicks", "Impressions", "Cost",
                          "Conversions", "Revenue", "CPC", "CTR", "ROAS"]
        return dcc.send_data_frame(export.to_csv, "google_ads_campaigns.csv", index=False)
    except Exception:
        return no_update


@callback(
    Output("ga-ads-daily-chart", "figure"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_ads_daily(period, _):
    period = period or "30daysAgo"
    fig = _empty_fig(340)
    try:
        df = ga4_loader.get_google_ads_daily(period)
        if not df.empty:
            fig.add_trace(go.Bar(
                x=df["date"], y=df["cost"],
                name="Ad Spend ($)", marker_color="rgba(66,133,244,0.5)",
                hovertemplate="%{x|%b %d}<br>Spend: $%{y:,.0f}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["conversions"],
                name="Conversions", yaxis="y2",
                mode="lines+markers",
                line=dict(color=GOOGLE_GREEN, width=2),
                marker=dict(size=4),
                hovertemplate="%{x|%b %d}<br>Conversions: %{y:.0f}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["clicks"],
                name="Clicks", yaxis="y2",
                mode="lines",
                line=dict(color=GOOGLE_YELLOW, width=1.5, dash="dot"),
                visible="legendonly",
                hovertemplate="%{x|%b %d}<br>Clicks: %{y:,}<extra></extra>",
            ))
            fig.update_layout(
                yaxis2=dict(
                    overlaying="y", side="right",
                    showgrid=False, zeroline=False,
                    color=GOOGLE_GREEN,
                ),
            )
    except Exception:
        pass
    fig.update_layout(
        showlegend=True, legend=H_LEGEND,
        xaxis=dict(gridcolor=COLORS["grid"], showline=False),
        yaxis=dict(gridcolor=COLORS["grid"], showline=False, rangemode="tozero",
                   title=dict(text="Spend ($)", font=dict(size=11, color=COLORS["text_muted"]))),
        hovermode="x unified", barmode="overlay",
        margin=dict(l=50, r=50, t=30, b=40),
    )
    return fig


@callback(
    Output("ga-source-medium-table", "children"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_source_medium(period, _):
    period = period or "30daysAgo"
    try:
        df = ga4_loader.get_source_medium(period)
        if df.empty:
            return _no_data()

        df = df.head(20)
        rows = []
        for _, r in df.iterrows():
            rows.append(html.Tr([
                html.Td(r["sessionSource"], style=_td_style({"fontWeight": "600"})),
                html.Td(r["sessionMedium"], style=_td_style({"color": COLORS["text_muted"]})),
                html.Td(f"{int(r['sessions']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{int(r['totalUsers']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['conversions']:.0f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"${r['purchaseRevenue']:,.0f}", style=_td_style({"textAlign": "right"})),
            ]))

        return html.Table(style={"width": "100%", "borderCollapse": "collapse"}, children=[
            html.Thead(html.Tr([
                html.Th("Source", style=_th_style()),
                html.Th("Medium", style=_th_style()),
                html.Th("Sessions", style=_th_style({"textAlign": "right"})),
                html.Th("Users", style=_th_style({"textAlign": "right"})),
                html.Th("Conv.", style=_th_style({"textAlign": "right"})),
                html.Th("Revenue", style=_th_style({"textAlign": "right"})),
            ])),
            html.Tbody(rows),
        ])
    except Exception:
        return _no_data("Could not load source/medium data")


@callback(
    Output("ga-landing-table", "children"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_landing_pages(period, _):
    period = period or "30daysAgo"
    try:
        df = ga4_loader.get_landing_pages(period)
        if df.empty:
            return _no_data()

        rows = []
        for _, r in df.iterrows():
            page = r["landing_page"]
            if len(page) > 50:
                page = page[:47] + "..."
            rows.append(html.Tr([
                html.Td(page, style=_td_style({"fontWeight": "500", "fontSize": "12px",
                         "maxWidth": "250px", "overflow": "hidden", "textOverflow": "ellipsis",
                         "whiteSpace": "nowrap"})),
                html.Td(f"{int(r['sessions']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{int(r['totalUsers']):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['conversions']:.0f}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{r['bounceRate']:.1f}%", style=_td_style({"textAlign": "right"})),
            ]))

        return html.Table(style={"width": "100%", "borderCollapse": "collapse"}, children=[
            html.Thead(html.Tr([
                html.Th("Landing Page", style=_th_style()),
                html.Th("Sessions", style=_th_style({"textAlign": "right"})),
                html.Th("Users", style=_th_style({"textAlign": "right"})),
                html.Th("Conv.", style=_th_style({"textAlign": "right"})),
                html.Th("Bounce", style=_th_style({"textAlign": "right"})),
            ])),
            html.Tbody(rows),
        ])
    except Exception:
        return _no_data("Could not load landing page data")


@callback(
    Output("ga-revenue-pie", "figure"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_revenue_pie(period, _):
    period = period or "30daysAgo"
    fig = _empty_fig(340)
    try:
        df = ga4_loader.get_channel_breakdown(period)
        if not df.empty:
            df = df[df["purchaseRevenue"] > 0].copy()
            if not df.empty:
                colors = [_CH_COLORS.get(lbl, COLORS["text_muted"]) for lbl in df["sessionDefaultChannelGroup"]]
                fig.add_trace(go.Pie(
                    labels=df["sessionDefaultChannelGroup"],
                    values=df["purchaseRevenue"],
                    marker=dict(colors=colors),
                    textinfo="label+percent",
                    textfont=dict(size=11),
                    hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.0f}<br>Share: %{percent}<extra></extra>",
                    hole=0.45,
                ))
                fig.update_layout(
                    showlegend=False,
                    margin=dict(l=20, r=20, t=20, b=20),
                    annotations=[dict(
                        text=f"${df['purchaseRevenue'].sum():,.0f}",
                        x=0.5, y=0.5, font_size=18, font_color=COLORS["accent"],
                        showarrow=False, font_family=FONT,
                    )],
                )
    except Exception:
        pass
    return fig


@callback(
    Output("ga-roas-chart", "figure"),
    Input("ga-period", "value"),
    Input("ga-refresh-btn", "n_clicks"),
)
def update_roas_chart(period, _):
    period = period or "30daysAgo"
    fig = _empty_fig(340)
    try:
        df = ga4_loader.get_google_ads_campaigns(period)
        if not df.empty:
            df = df[df["cost"] > 0].copy()
            if not df.empty:
                df = df.sort_values("roas", ascending=True).tail(15)
                colors = [GOOGLE_GREEN if r >= 2 else (GOOGLE_YELLOW if r >= 1 else COLORS["red"]) for r in df["roas"]]

                short_names = []
                for name in df["campaign"]:
                    short = name if len(name) <= 35 else name[:32] + "..."
                    short_names.append(short)

                fig.add_trace(go.Bar(
                    y=short_names,
                    x=df["roas"],
                    orientation="h",
                    marker_color=colors,
                    marker_line_width=0,
                    text=df["roas"].apply(lambda r: f"{r:.1f}x"),
                    textposition="auto",
                    textfont=dict(size=11, color="#fff"),
                    hovertemplate="<b>%{customdata[0]}</b><br>ROAS: %{x:.1f}x<br>Cost: $%{customdata[1]:,.0f}<br>Revenue: $%{customdata[2]:,.0f}<extra></extra>",
                    customdata=df[["campaign", "cost", "purchaseRevenue"]].values,
                ))
                fig.add_vline(x=1, line_dash="dash", line_color=COLORS["text_muted"], line_width=1, opacity=0.5)
    except Exception:
        pass
    fig.update_layout(
        showlegend=False,
        margin=dict(l=180, r=20, t=10, b=30),
        xaxis=dict(showgrid=True, gridcolor=COLORS["grid"], zeroline=False,
                   title=dict(text="ROAS", font=dict(size=11, color=COLORS["text_muted"]))),
        yaxis=dict(showgrid=False, showline=False),
        bargap=0.3,
    )
    return fig


# ============================================================
# TRAINING CALLBACKS
# ============================================================

@callback(
    Output("ga-train-status", "children"),
    Output("ga-train-poll", "disabled"),
    Output("ga-train-btn", "disabled"),
    Input("ga-train-btn", "n_clicks"),
    State("ga-train-period", "value"),
    prevent_initial_call=True,
)
def start_training(n_clicks, period):
    if not n_clicks:
        return no_update, no_update, no_update
    started = ga4_trainer.start_training(days_back=period or 90)
    if not started:
        return html.P("Training already running...", style={"color": GOOGLE_YELLOW, "fontSize": "13px"}), True, True
    return (
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
            html.Div(style={
                "width": "8px", "height": "8px", "borderRadius": "50%",
                "backgroundColor": COLORS["accent3"],
                "animation": "pulse 1.5s ease-in-out infinite",
            }),
            html.Span("Starting training...", style={"fontSize": "13px", "color": COLORS["accent3"]}),
        ]),
        False,
        True,
    )


@callback(
    Output("ga-train-status", "children", allow_duplicate=True),
    Output("ga-train-poll", "disabled", allow_duplicate=True),
    Output("ga-train-btn", "disabled", allow_duplicate=True),
    Output("ga-compare-kpis", "children"),
    Output("ga-compare-table-container", "children"),
    Output("ga-compare-charts-container", "children"),
    Output("ga-forecast-viewer-container", "children"),
    Input("ga-train-poll", "n_intervals"),
    prevent_initial_call=True,
)
def poll_training(_):
    state = ga4_trainer.get_state()

    if state["running"]:
        pct = ""
        if state["total"] > 0:
            pct = f" ({state['current']}/{state['total']})"
        status = html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px"}, children=[
            html.Div(style={
                "width": "8px", "height": "8px", "borderRadius": "50%",
                "backgroundColor": COLORS["accent3"],
                "animation": "pulse 1.5s ease-in-out infinite",
            }),
            html.Span(f"{state['progress']}{pct}", style={"fontSize": "13px", "color": COLORS["accent3"]}),
        ])
        if state["total"] > 0:
            progress_bar = html.Div(style={"marginTop": "8px"}, children=[
                html.Div(style={
                    "height": "4px", "backgroundColor": COLORS["card_border"], "borderRadius": "2px",
                    "overflow": "hidden",
                }, children=[
                    html.Div(style={
                        "height": "100%", "borderRadius": "2px",
                        "backgroundColor": COLORS["accent3"],
                        "width": f"{state['current'] / state['total'] * 100:.0f}%",
                        "transition": "width 0.3s ease",
                    }),
                ]),
            ])
            status = html.Div([status, progress_bar])
        return status, False, True, no_update, no_update, no_update, no_update

    # Training finished
    results = ga4_trainer.get_results()
    if results is None:
        return (
            html.P(state["progress"], style={"color": COLORS["red"] if "ERROR" in state["progress"] else COLORS["text_muted"], "fontSize": "13px"}),
            True, False, [], html.Div(), html.Div(), html.Div(),
        )

    done_msg = html.P(
        f"Training complete at {results['trained_at']} | {len(results['comparison'])} products | "
        f"GA4 data: {'Yes' if results['ga4_available'] else 'No'} | Period: {results['days_back']}d",
        style={"color": COLORS["accent3"], "fontSize": "13px"},
    )

    comp = results["comparison"]
    kpis = _build_comparison_kpis(comp)
    table = _build_comparison_table(comp)
    charts = _build_comparison_charts(comp)
    forecast = _build_forecast_viewer(results)

    return done_msg, True, False, kpis, table, charts, forecast


def _build_comparison_kpis(comp):
    if comp.empty:
        return []
    kpis = []
    avg_mae_base = comp["mae_base"].mean()
    avg_r2_base = comp["r2_base"].mean()
    kpis.append(kpi_card("Avg MAE (Base)", f"{avg_mae_base:.2f}", color=GOOGLE_BLUE))
    kpis.append(kpi_card("Avg R2 (Base)", f"{avg_r2_base:.3f}", color=GOOGLE_BLUE))

    has_ga4 = comp["mae_ga4"].notna().any()
    if has_ga4:
        ga4_rows = comp[comp["mae_ga4"].notna()]
        avg_mae_ga4 = ga4_rows["mae_ga4"].mean()
        avg_r2_ga4 = ga4_rows["r2_ga4"].mean()
        ga4_wins = (ga4_rows["best_model"] == "ga4").sum()
        win_pct = ga4_wins / len(ga4_rows) * 100 if len(ga4_rows) > 0 else 0
        avg_improvement = ga4_rows["improvement_pct"].mean()

        kpis.append(kpi_card("Avg MAE (GA4)", f"{avg_mae_ga4:.2f}", color=GOOGLE_GREEN))
        kpis.append(kpi_card("Avg R2 (GA4)", f"{avg_r2_ga4:.3f}", color=GOOGLE_GREEN))
        kpis.append(kpi_card("GA4 Wins", f"{ga4_wins}/{len(ga4_rows)} ({win_pct:.0f}%)",
                             color=GOOGLE_GREEN if win_pct > 50 else GOOGLE_YELLOW))
        imp_color = GOOGLE_GREEN if avg_improvement > 0 else COLORS["red"]
        kpis.append(kpi_card("Avg MAE Improvement", f"{avg_improvement:+.1f}%", color=imp_color))
    else:
        kpis.append(kpi_card("GA4 Model", "No GA4 data", color=COLORS["text_muted"]))

    kpis.append(kpi_card("Products Trained", f"{len(comp)}", color=COLORS["accent"]))
    return kpis


def _build_comparison_table(comp):
    if comp.empty:
        return _no_data("No training results yet. Click 'Train Models' above.")

    has_ga4 = comp["mae_ga4"].notna().any()
    sorted_comp = comp.sort_values("mae_base", ascending=True)

    rows = []
    for _, r in sorted_comp.iterrows():
        name = r["product_name"]
        if len(name) > 45:
            name = name[:42] + "..."

        best = r.get("best_model", "base")
        best_badge = html.Span(
            "GA4" if best == "ga4" else "Base",
            style={
                "backgroundColor": GOOGLE_GREEN if best == "ga4" else GOOGLE_BLUE,
                "color": "#fff", "padding": "2px 8px", "borderRadius": "4px",
                "fontSize": "10px", "fontWeight": "700",
            },
        )

        imp = r.get("improvement_pct", 0)
        imp_color = GOOGLE_GREEN if imp > 0 else (COLORS["red"] if imp < 0 else COLORS["text_muted"])

        cells = [
            html.Td(name, style=_td_style({"fontWeight": "600", "maxWidth": "250px",
                     "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"})),
            html.Td(f"{r['mae_base']:.2f}", style=_td_style({"textAlign": "right"})),
            html.Td(f"{r['rmse_base']:.2f}", style=_td_style({"textAlign": "right"})),
            html.Td(f"{r['r2_base']:.3f}", style=_td_style({"textAlign": "right"})),
        ]

        if has_ga4:
            if pd.notna(r.get("mae_ga4")):
                cells.extend([
                    html.Td(f"{r['mae_ga4']:.2f}", style=_td_style({"textAlign": "right"})),
                    html.Td(f"{r['rmse_ga4']:.2f}", style=_td_style({"textAlign": "right"})),
                    html.Td(f"{r['r2_ga4']:.3f}", style=_td_style({"textAlign": "right"})),
                    html.Td(best_badge, style=_td_style({"textAlign": "center"})),
                    html.Td(f"{imp:+.1f}%", style=_td_style({"textAlign": "right", "color": imp_color, "fontWeight": "600"})),
                ])
            else:
                cells.extend([html.Td("--", style=_td_style({"textAlign": "right", "color": COLORS["text_muted"]}))] * 5)

        rows.append(html.Tr(cells))

    headers = [
        html.Th("Product", style=_th_style()),
        html.Th("MAE Base", style=_th_style({"textAlign": "right"})),
        html.Th("RMSE Base", style=_th_style({"textAlign": "right"})),
        html.Th("R2 Base", style=_th_style({"textAlign": "right"})),
    ]
    if has_ga4:
        headers.extend([
            html.Th("MAE GA4", style=_th_style({"textAlign": "right"})),
            html.Th("RMSE GA4", style=_th_style({"textAlign": "right"})),
            html.Th("R2 GA4", style=_th_style({"textAlign": "right"})),
            html.Th("Best", style=_th_style({"textAlign": "center"})),
            html.Th("Improv.", style=_th_style({"textAlign": "right"})),
        ])

    return html.Div(style=card_style({"borderTop": f"3px solid {COLORS['accent']}"}), children=[
        section_label("MODEL COMPARISON"),
        html.H3("Base Prophet vs GA4-Enhanced Prophet", style={"margin": "0 0 12px", "fontSize": "16px", "fontWeight": "600"}),
        html.Div(style={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"}, children=[
            html.Table(style={"width": "100%", "borderCollapse": "collapse"}, children=[
                html.Thead(html.Tr(headers)),
                html.Tbody(rows),
            ]),
        ]),
    ])


def _build_comparison_charts(comp):
    if comp.empty or not comp["mae_ga4"].notna().any():
        return html.Div()

    ga4_rows = comp[comp["mae_ga4"].notna()].copy()
    if ga4_rows.empty:
        return html.Div()

    ga4_rows = ga4_rows.sort_values("r2_base", ascending=True).tail(15)

    short_names = []
    for name in ga4_rows["product_name"]:
        short_names.append(name if len(name) <= 30 else name[:27] + "...")

    # R2 comparison chart
    r2_fig = go.Figure()
    r2_fig.add_trace(go.Bar(
        y=short_names, x=ga4_rows["r2_base"],
        name="Base", orientation="h",
        marker_color=GOOGLE_BLUE, opacity=0.8,
    ))
    r2_fig.add_trace(go.Bar(
        y=short_names, x=ga4_rows["r2_ga4"],
        name="GA4", orientation="h",
        marker_color=GOOGLE_GREEN, opacity=0.8,
    ))
    r2_fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        barmode="group", bargap=0.3, bargroupgap=0.1,
        showlegend=True, legend=H_LEGEND,
        margin=dict(l=170, r=20, t=30, b=40), height=400,
        xaxis=dict(gridcolor=COLORS["grid"], showline=False, zeroline=False,
                   title=dict(text="R2 Score", font=dict(size=11, color=COLORS["text_muted"]))),
        yaxis=dict(showgrid=False, showline=False),
    )

    # MAE scatter (base vs GA4)
    mae_fig = go.Figure()
    max_mae = max(ga4_rows["mae_base"].max(), ga4_rows["mae_ga4"].max()) * 1.1
    mae_fig.add_trace(go.Scatter(
        x=[0, max_mae], y=[0, max_mae],
        mode="lines", name="Equal",
        line=dict(color=COLORS["text_muted"], width=1, dash="dash"),
        showlegend=False,
    ))
    colors = [GOOGLE_GREEN if r["best_model"] == "ga4" else GOOGLE_BLUE for _, r in ga4_rows.iterrows()]
    mae_fig.add_trace(go.Scatter(
        x=ga4_rows["mae_base"], y=ga4_rows["mae_ga4"],
        mode="markers", name="Products",
        marker=dict(size=10, color=colors, line=dict(width=1, color="#fff")),
        text=ga4_rows["product_name"].apply(lambda n: n[:30]),
        hovertemplate="<b>%{text}</b><br>MAE Base: %{x:.2f}<br>MAE GA4: %{y:.2f}<extra></extra>",
    ))
    mae_fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=12),
        showlegend=False, height=400,
        margin=dict(l=50, r=20, t=30, b=50),
        xaxis=dict(gridcolor=COLORS["grid"], showline=False, zeroline=False,
                   title=dict(text="MAE (Base)", font=dict(size=11, color=COLORS["text_muted"]))),
        yaxis=dict(gridcolor=COLORS["grid"], showline=False, zeroline=False,
                   title=dict(text="MAE (GA4)", font=dict(size=11, color=COLORS["text_muted"]))),
        annotations=[dict(
            x=max_mae * 0.7, y=max_mae * 0.4,
            text="GA4 better", showarrow=False,
            font=dict(size=11, color=GOOGLE_GREEN),
        ), dict(
            x=max_mae * 0.3, y=max_mae * 0.7,
            text="Base better", showarrow=False,
            font=dict(size=11, color=GOOGLE_BLUE),
        )],
    )

    return html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px"}, children=[
        html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_BLUE}"}), children=[
            section_label("R2 COMPARISON"),
            html.H3("R2 Score: Base vs GA4", style={"margin": "0 0 8px", "fontSize": "16px", "fontWeight": "600"}),
            dcc.Graph(figure=r2_fig, config={"displayModeBar": False}),
        ]),
        html.Div(style=card_style({"borderTop": f"3px solid {GOOGLE_GREEN}"}), children=[
            section_label("MAE COMPARISON"),
            html.H3("MAE: Base vs GA4 (lower is better)", style={"margin": "0 0 8px", "fontSize": "16px", "fontWeight": "600"}),
            html.P("Points below the diagonal = GA4 model is better", style={
                "color": COLORS["text_muted"], "fontSize": "11px", "margin": "0 0 4px",
            }),
            dcc.Graph(figure=mae_fig, config={"displayModeBar": False}),
        ]),
    ])


def _build_forecast_viewer(results):
    if not results or results["comparison"].empty:
        return html.Div()

    comp = results["comparison"]
    product_options = [
        {"label": r["product_name"][:50], "value": r["product_id"]}
        for _, r in comp.iterrows()
    ]

    return html.Div(style=card_style({"borderTop": f"3px solid {COLORS['accent']}", "marginTop": "0"}), children=[
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                        "marginBottom": "12px", "flexWrap": "wrap", "gap": "12px"}, children=[
            html.Div(children=[
                section_label("FORECAST VIEWER"),
                html.H3("Historical + Predictions Comparison", style={
                    "margin": "0", "fontSize": "16px", "fontWeight": "600",
                }),
            ]),
            dcc.Dropdown(
                id="ga-forecast-product",
                options=product_options,
                value=product_options[0]["value"] if product_options else None,
                clearable=False,
                style={"width": "350px", "fontSize": "12px", "backgroundColor": COLORS["bg"]},
            ),
        ]),
        dcc.Graph(id="ga-forecast-chart", config={"displayModeBar": False}, style={"height": "400px"}),
    ])


@callback(
    Output("ga-forecast-chart", "figure"),
    Input("ga-forecast-product", "value"),
    prevent_initial_call=True,
)
def update_forecast_chart(product_id):
    fig = _empty_fig(400)
    if not product_id:
        return fig

    results = ga4_trainer.get_results()
    if not results:
        return fig

    hist_data = results["historical"].get(product_id)
    base_preds = results["base_predictions"].get(product_id)
    ga4_preds = results["ga4_predictions"].get(product_id)

    if hist_data is None:
        return fig

    df_hist = hist_data["data"]
    pname = hist_data["product_name"]

    # Historical
    fig.add_trace(go.Scatter(
        x=df_hist["ds"], y=df_hist["y"],
        mode="lines", name="Actual Sales",
        line=dict(color=COLORS["text"], width=1.5),
        hovertemplate="%{x|%b %d}<br>Sales: %{y:.0f}<extra></extra>",
    ))

    # Base predictions
    if base_preds is not None:
        fig.add_trace(go.Scatter(
            x=base_preds["ds"], y=base_preds["yhat"],
            mode="lines", name="Forecast (Base)",
            line=dict(color=GOOGLE_BLUE, width=2),
            hovertemplate="%{x|%b %d}<br>Base: %{y:.1f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=pd.concat([base_preds["ds"], base_preds["ds"].iloc[::-1]]),
            y=pd.concat([base_preds["yhat_upper"], base_preds["yhat_lower"].iloc[::-1]]),
            fill="toself", fillcolor="rgba(66,133,244,0.1)",
            line=dict(width=0), name="Base 80% CI",
            showlegend=False, hoverinfo="skip",
        ))

    # GA4 predictions
    if ga4_preds is not None:
        fig.add_trace(go.Scatter(
            x=ga4_preds["ds"], y=ga4_preds["yhat"],
            mode="lines", name="Forecast (GA4)",
            line=dict(color=GOOGLE_GREEN, width=2),
            hovertemplate="%{x|%b %d}<br>GA4: %{y:.1f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=pd.concat([ga4_preds["ds"], ga4_preds["ds"].iloc[::-1]]),
            y=pd.concat([ga4_preds["yhat_upper"], ga4_preds["yhat_lower"].iloc[::-1]]),
            fill="toself", fillcolor="rgba(52,168,83,0.1)",
            line=dict(width=0), name="GA4 80% CI",
            showlegend=False, hoverinfo="skip",
        ))

    fig.update_layout(
        showlegend=True, legend=H_LEGEND,
        xaxis=dict(gridcolor=COLORS["grid"], showline=False),
        yaxis=dict(gridcolor=COLORS["grid"], showline=False, rangemode="tozero",
                   title=dict(text="Quantity", font=dict(size=11, color=COLORS["text_muted"]))),
        hovermode="x unified",
        margin=dict(l=50, r=20, t=30, b=40),
        title=dict(text=pname[:60], font=dict(size=14, color=COLORS["text"]), x=0.5),
    )
    return fig
