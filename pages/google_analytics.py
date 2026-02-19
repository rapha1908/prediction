"""Google Analytics & Google Ads page – layout and callbacks."""

import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc, callback, Output, Input, State, no_update

import ga4_loader
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
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px"}, children=[
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
