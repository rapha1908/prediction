import os
from dotenv import load_dotenv

load_dotenv()

from dash import Dash, html, dcc, callback, Output, Input, no_update
from config import COLORS, FONT
from data_loader import date_min, date_max
from pages import stock_manager, forms_manager, settings as settings_page
from pages import cross_sell, reports, main_dashboard, google_analytics  # noqa: F401 – registers callbacks

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
            .settings-dropdown .Select-control {
                background-color: #0b0b14 !important;
                border-color: #1f1f32 !important;
            }
            .settings-dropdown .Select-menu-outer {
                background-color: #131320 !important;
                border-color: #1f1f32 !important;
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
        dcc.Store(id="current-user-perms", data=[]),

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
                    html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"}, children=[
                        html.Div(id="sync-status", style={"fontSize": "13px", "color": COLORS["text_muted"]}),
                        dcc.Checklist(
                            id="sync-full-check",
                            options=[{"label": " Sync completo (todos os pedidos)", "value": "full"}],
                            value=[],
                            style={"display": "flex", "alignItems": "center", "fontSize": "12px", "color": COLORS["text_muted"]},
                            inputStyle={"marginRight": "6px"},
                            labelStyle={"cursor": "pointer"},
                        ),
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
                        html.Button(
                            "Update Google Sheet",
                            id="sheets-update-btn",
                            n_clicks=0,
                            style={
                                "backgroundColor": "#34A853",
                                "color": "#fff",
                                "border": "none", "borderRadius": "8px",
                                "padding": "10px 20px", "fontSize": "13px",
                                "fontWeight": "700", "cursor": "pointer",
                                "fontFamily": FONT, "letterSpacing": "0.5px",
                                "whiteSpace": "nowrap",
                            },
                        ),
                        html.Span(id="sheets-update-status", style={"fontSize": "12px", "color": COLORS["text_muted"]}),
                        dcc.Link(
                            "Stock Manager",
                            id="header-stock-link",
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
                        dcc.Link(
                            "Forms Manager",
                            id="header-forms-link",
                            href="/forms",
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
                        dcc.Link(
                            "Cross-Sell",
                            id="header-crosssell-link",
                            href="/cross-sell",
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
                        dcc.Link(
                            "Analytics",
                            id="header-analytics-link",
                            href="/analytics",
                            style={
                                "color": "#34A853",
                                "fontSize": "12px",
                                "textDecoration": "none",
                                "border": "1px solid #34A853",
                                "borderRadius": "8px",
                                "padding": "10px 18px",
                                "whiteSpace": "nowrap",
                                "fontFamily": FONT,
                                "fontWeight": "600",
                            },
                        ),
                        dcc.Link(
                            "Settings",
                            id="header-settings-link",
                            href="/settings",
                            style={
                                "color": COLORS["text_muted"],
                                "fontSize": "12px",
                                "textDecoration": "none",
                                "border": f"1px solid {COLORS['card_border']}",
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
        html.Div(id="stock-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=stock_manager.layout()),

        # --- FORMS MANAGER PAGE (hidden by default, shown on /forms) ---
        html.Div(id="forms-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=forms_manager.layout()),

        # --- CROSS-SELL PAGE ---
        html.Div(id="crosssell-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=cross_sell.layout()),
        # --- SETTINGS PAGE (hidden by default, shown on /settings) ---
        html.Div(id="settings-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=settings_page.layout()),

        # --- GOOGLE ANALYTICS PAGE (hidden by default, shown on /analytics) ---
        html.Div(id="analytics-page", style={"display": "none", "padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=google_analytics.layout()),

        # --- DASHBOARD CONTENT (main page) ---
        html.Div(id="dashboard-page", style={"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto"},
                 children=main_dashboard.layout()),
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
    Output("forms-page", "style"),
    Output("crosssell-page", "style"),
    Output("settings-page", "style"),
    Output("analytics-page", "style"),
    Input("url", "pathname"),
)
def route_page(pathname):
    """Show/hide pages based on URL."""
    hidden = {"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto", "display": "none"}
    visible = {"padding": "28px 48px", "maxWidth": "1440px", "margin": "0 auto", "display": "block"}
    if pathname == "/stock":
        return hidden, visible, hidden, hidden, hidden, hidden
    if pathname == "/forms":
        return hidden, hidden, visible, hidden, hidden, hidden
    if pathname == "/cross-sell":
        return hidden, hidden, hidden, visible, hidden, hidden
    if pathname == "/settings":
        return hidden, hidden, hidden, hidden, visible, hidden
    if pathname == "/analytics":
        return hidden, hidden, hidden, hidden, hidden, visible
    return visible, hidden, hidden, hidden, hidden, hidden


# ============================================================
# GOOGLE SHEETS UPDATE
# ============================================================

@callback(
    Output("sheets-update-btn", "disabled"),
    Output("sheets-update-status", "children"),
    Input("sheets-update-btn", "n_clicks"),
    prevent_initial_call=True,
)
def update_google_sheet(n_clicks):
    if not n_clicks:
        return no_update, no_update
    try:
        import google_sheets_sales as gs
        added, msg = gs.update_sheet()
        return False, msg
    except Exception as e:
        return False, f"Erro: {e}"


# ============================================================
# RUN
# ============================================================

# Expose the Flask server for gunicorn (production)
server = app.server

# Ensure DB tables exist (including RBAC tables)
try:
    import db as _db
    if _db.test_connection():
        _db.create_tables()
except Exception as _e:
    print(f"  [WARNING] Could not create DB tables: {_e}")

# Setup authentication (JWT + cookies, seeds default roles/users)
import auth as _auth
_auth.setup_auth(server)
print("  [OK] Authentication enabled")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    debug = os.environ.get("RENDER") is None  # disable debug in production
    print(f"\n  Dashboard available at: http://localhost:{port}\n")
    app.run(debug=debug, host="0.0.0.0", port=port)
