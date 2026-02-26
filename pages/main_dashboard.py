"""Main dashboard page â€“ layout and callbacks (KPIs, charts, map, AI chat, orders)."""
import os
import sys
import json
import subprocess
import threading
import tempfile
import requests
from pathlib import Path
import dash
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from dash import html, dcc, callback, clientside_callback, Output, Input, State, no_update, ctx, ALL
import agent as ai_agent
import order_bumps as ob_api
from config import (
    COLORS, FONT, PLOT_LAYOUT, CATEGORY_COLORS, GENERIC_CATS, H_LEGEND,
    card_style, section_label, kpi_card, _th_style, _td_style,
    dropdown_style, parse_categories, build_product_cat_map,
    product_matches_cats, filter_by_categories, explode_categories,
)
from data_loader import (
    hist_df, pred_df, metrics_df, all_orders_df,
    product_cat_map, orders_cat_map, event_status_map,
    all_categories, product_sales,
    total_products, total_sales_qty, total_revenue,
    total_orders_days, date_min, date_max, pred_total_qty,
    exchange_rates, get_exchange_rates, convert_revenue,
    get_hourly_df, get_low_stock_df, get_source_df,
    get_cross_sell_df, get_geo_sales_df,
    invalidate_lazy_cache, reload_all_data, _lazy_cache,
    _get_db, build_event_status_map,
    DISPLAY_CURRENCY, currency_symbol, _format_converted_total,
    TODAY, ONLINE_COURSE_CATS, LOW_STOCK_THRESHOLD,
    filter_by_event_tab, filter_by_currency,
)


DATA_DIR = Path(__file__).resolve().parent.parent

n_active = sum(1 for v in event_status_map.values() if v == "active")
n_past = sum(1 for v in event_status_map.values() if v == "past")
n_courses = sum(1 for v in event_status_map.values() if v == "course")


def layout():
    """Return the list of children for the main dashboard page."""
    return [

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
                        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                        "marginBottom": "14px", "flexWrap": "wrap", "gap": "10px"}, children=[
                            html.Div(children=[
                                section_label("ACQUISITION"),
                                html.H3("Sales Sources", style={
                                    "margin": "0", "fontSize": "18px", "fontWeight": "700",
                                }),
                            ]),
                            html.Div(style={"display": "flex", "gap": "8px", "alignItems": "center"}, children=[
                                dcc.Dropdown(
                                    id="source-category-filter",
                                    options=[],
                                    value=[],
                                    multi=True,
                                    placeholder="All categories",
                                    style={
                                        "minWidth": "180px", "fontSize": "12px",
                                        "backgroundColor": COLORS["bg"],
                                    },
                                ),
                                html.Button("Export CSV", id="source-export-btn", n_clicks=0, style={
                                    "backgroundColor": "transparent", "color": COLORS["accent"],
                                    "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                                    "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                                    "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                                }),
                            ]),
                        ]),
                        dcc.Download(id="source-export-download"),
                        dcc.Graph(
                            id="source-chart",
                            config={"displayModeBar": False},
                            style={"height": "280px"},
                        ),
                    ],
                ),
            ]),

            # ============ AI SALES ASSISTANT ============
            html.Div(id="chat-section", style=card_style({"marginBottom": "28px", "borderTop": f"3px solid {COLORS['accent']}"}), children=[
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

                # --- SALES BY CITY TABLE ---
                html.Div(style=card_style({"marginTop": "24px"}), children=[
                    html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                    "marginBottom": "16px", "flexWrap": "wrap", "gap": "10px"}, children=[
                        html.Div(children=[
                            section_label("CITY BREAKDOWN"),
                            html.H3("Sales by City", style={
                                "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                            }),
                            html.P("Aggregated ticket sales per city. Use the filters to narrow results.",
                                   style={"color": COLORS["text_muted"], "fontSize": "12px", "margin": "0"}),
                        ]),
                        html.Div(style={"display": "flex", "gap": "8px", "alignItems": "center", "flexWrap": "wrap"}, children=[
                            dcc.Dropdown(
                                id="city-category-filter",
                                options=[],
                                value=[],
                                multi=True,
                                placeholder="All categories",
                                style={
                                    "minWidth": "200px", "fontSize": "12px",
                                    "backgroundColor": COLORS["bg"],
                                },
                            ),
                            dcc.Input(
                                id="city-search",
                                type="text",
                                placeholder="Search city...",
                                debounce=True,
                                style={
                                    "width": "160px", "padding": "8px 12px",
                                    "background": COLORS["bg"],
                                    "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "8px", "color": COLORS["text"],
                                    "fontFamily": FONT, "fontSize": "12px",
                                },
                            ),
                            html.Button("Export CSV", id="city-export-btn", n_clicks=0, style={
                                "backgroundColor": "transparent", "color": COLORS["accent"],
                                "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                                "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                                "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                            }),
                        ]),
                    ]),
                    dcc.Download(id="city-export-download"),
                    html.Div(id="city-sales-table", style={
                        "overflowX": "auto", "maxHeight": "500px", "overflowY": "auto",
                    }),
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
                        className="report-btn-wrapper",
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
    ]


# ============================================================
# MAIN DASHBOARD CALLBACKS
# ============================================================



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


def _prepare_source_df(categories=None):
    """Prepare source data, optionally filtered by categories. Returns (df_for_chart, df_full_detail)."""
    _source_df = get_source_df()
    if _source_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = _source_df.copy()
    if categories:
        df = df[df["category"].isin(categories)]
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

    df = df.assign(
        label=df["source"].apply(
            lambda s: _SOURCE_LABELS.get(str(s).lower().strip(), str(s).strip().title())
        )
    )

    # Full detail (for export): per source+category
    df_detail = df.copy()

    # Aggregated for chart
    df_agg = df.groupby("label").agg(
        quantity_sold=("quantity_sold", "sum"),
        revenue=("revenue", "sum"),
        order_count=("order_count", "sum"),
    ).reset_index().sort_values("quantity_sold", ascending=False)

    return df_agg, df_detail


@callback(
    Output("source-category-filter", "options"),
    Input("event-tabs", "value"),
)
def populate_source_category_options(_tab):
    """Populate category dropdown for the source chart."""
    _source_df = get_source_df()
    if _source_df.empty or "category" not in _source_df.columns:
        return []
    cats = sorted(_source_df["category"].dropna().unique())
    return [{"label": c, "value": c} for c in cats]


@callback(
    Output("source-chart", "figure"),
    Input("event-tabs", "value"),
    Input("source-category-filter", "value"),
)
def update_source_chart(_tab, selected_categories):
    """Render horizontal bar chart of sales by acquisition source."""
    fig = go.Figure()
    df, _ = _prepare_source_df(selected_categories or None)
    if df.empty:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

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

    total = df["quantity_sold"].sum()
    df["pct"] = (df["quantity_sold"] / total * 100).round(1)
    df = df.sort_values("quantity_sold", ascending=True)

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


@callback(
    Output("source-export-download", "data"),
    Input("source-export-btn", "n_clicks"),
    State("source-category-filter", "value"),
    prevent_initial_call=True,
)
def export_source_data(n_clicks, selected_categories):
    """Export sales-by-source data as CSV."""
    if not n_clicks:
        return no_update
    df_agg, df_detail = _prepare_source_df(selected_categories or None)
    if df_detail.empty:
        return no_update

    # Build a clean export: source label, category, quantity, revenue, orders
    export = df_detail.rename(columns={
        "label": "Source", "category": "Category",
        "quantity_sold": "Quantity Sold", "revenue": "Revenue",
        "order_count": "Orders",
    })
    cols = ["Source", "Category", "Quantity Sold", "Revenue", "Orders"]
    export = export[[c for c in cols if c in export.columns]].sort_values(
        ["Source", "Category"], ascending=True
    )

    return dcc.send_data_frame(export.to_csv, "sales_sources.csv", index=False)


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
# SALES BY CITY TABLE (inside map tab)
# ============================================================

@callback(
    Output("city-category-filter", "options"),
    Input("event-tabs", "value"),
)
def populate_city_cat_filter(tab_value):
    """Populate the category dropdown for city sales (pipe-separated categories)."""
    _geo_df = get_geo_sales_df()
    if tab_value != "map" or _geo_df.empty or "category" not in _geo_df.columns:
        return []
    cats = set()
    for val in _geo_df["category"].dropna().unique():
        for c in parse_categories(val):
            if c and c != "Uncategorized":
                cats.add(c)
    return [{"label": c, "value": c} for c in sorted(cats)]


def _build_city_data(selected_cats=None, search_text=None):
    """Build aggregated city sales DataFrame, optionally filtered."""
    _geo_df = get_geo_sales_df()
    if _geo_df.empty:
        return pd.DataFrame()

    df = _geo_df.copy()

    if selected_cats:
        cat_map = build_product_cat_map(df)
        df = filter_by_categories(df, selected_cats, cat_map)

    if not df.empty and search_text:
        q = search_text.strip().lower()
        df = df[
            df["city"].str.lower().str.contains(q, na=False)
            | df["state"].str.lower().str.contains(q, na=False)
            | df["country"].str.lower().str.contains(q, na=False)
        ]

    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby(["country", "state", "city"])
        .agg(
            quantity_sold=("quantity_sold", "sum"),
            revenue=("revenue", "sum"),
            products=("product_name", "nunique"),
        )
        .reset_index()
        .sort_values("quantity_sold", ascending=False)
    )
    return agg


@callback(
    Output("city-sales-table", "children"),
    Input("event-tabs", "value"),
    Input("city-category-filter", "value"),
    Input("city-search", "value"),
)
def render_city_sales_table(tab_value, selected_cats, search_text):
    """Render a table of sales aggregated by city."""
    if tab_value != "map":
        return html.P("Switch to the Sales Map tab to see city data.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px"})

    agg = _build_city_data(selected_cats or None, search_text)
    if agg.empty:
        return html.P("No location data available.", style={"color": COLORS["text_muted"], "fontSize": "13px"})

    total_qty = agg["quantity_sold"].sum()

    header = html.Tr([
        html.Th("#", style=_th_style({"width": "40px"})),
        html.Th("City", style=_th_style()),
        html.Th("State", style=_th_style()),
        html.Th("Country", style=_th_style()),
        html.Th("Qty Sold", style=_th_style({"textAlign": "right"})),
        html.Th("% of Total", style=_th_style({"textAlign": "right"})),
        html.Th("Revenue", style=_th_style({"textAlign": "right"})),
        html.Th("Products", style=_th_style({"textAlign": "right"})),
    ])

    rows = []
    for i, (_, r) in enumerate(agg.head(100).iterrows(), 1):
        pct = r["quantity_sold"] / total_qty * 100 if total_qty else 0
        rows.append(html.Tr([
            html.Td(str(i), style=_td_style({"color": COLORS["text_muted"], "width": "40px"})),
            html.Td(r["city"] or "â€”", style=_td_style({"fontWeight": "600"})),
            html.Td(r["state"] or "â€”", style=_td_style()),
            html.Td(r["country"] or "â€”", style=_td_style()),
            html.Td(f"{int(r['quantity_sold']):,}", style=_td_style({"textAlign": "right", "fontWeight": "600", "color": COLORS["accent"]})),
            html.Td(f"{pct:.1f}%", style=_td_style({"textAlign": "right", "color": COLORS["text_muted"]})),
            html.Td(f"${r['revenue']:,.2f}", style=_td_style({"textAlign": "right"})),
            html.Td(str(int(r["products"])), style=_td_style({"textAlign": "right", "color": COLORS["text_muted"]})),
        ]))

    summary = html.Div(
        f"Showing top {min(len(agg), 100)} of {len(agg)} cities  Â·  "
        f"Total: {int(total_qty):,} units  Â·  ${agg['revenue'].sum():,.2f} revenue",
        style={"color": COLORS["text_muted"], "fontSize": "12px", "marginTop": "10px"},
    )

    return html.Div([
        html.Table(
            [html.Thead(header), html.Tbody(rows)],
            style={"width": "100%", "borderCollapse": "collapse"},
        ),
        summary,
    ])


@callback(
    Output("city-export-download", "data"),
    Input("city-export-btn", "n_clicks"),
    State("city-category-filter", "value"),
    State("city-search", "value"),
    prevent_initial_call=True,
)
def export_city_data(n_clicks, selected_cats, search_text):
    """Export city sales data as CSV."""
    if not n_clicks:
        return no_update
    agg = _build_city_data(selected_cats or None, search_text)
    if agg.empty:
        return no_update
    export = agg.rename(columns={
        "country": "Country", "state": "State", "city": "City",
        "quantity_sold": "Quantity Sold", "revenue": "Revenue",
        "products": "Unique Products",
    })
    return dcc.send_data_frame(export.to_csv, "sales_by_city.csv", index=False)


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

_IS_RENDER = os.environ.get("RENDER") is not None
_SYNC_LOG_FILE = os.path.join(tempfile.gettempdir(), "tcche_sync.log")
_sync_lock = threading.Lock()
_sync_state = {"running": False, "exit_code": None}


def _trigger_render_cron() -> tuple[bool, str]:
    """
    Trigger the tcche-sync Cron Job via Render API.
    Returns (success, message).
    """
    api_key = os.environ.get("RENDER_API_KEY", "").strip()
    cron_id = os.environ.get("RENDER_CRON_JOB_ID", "").strip()
    if not api_key or not cron_id:
        return False, "Configure RENDER_API_KEY e RENDER_CRON_JOB_ID nas variáveis de ambiente do Render."
    try:
        resp = requests.post(
            f"https://api.render.com/v1/cron-jobs/{cron_id}/runs",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, "Cron Job acionado! O sync está rodando em background. Atualize a página em alguns minutos."
        return False, f"API Render retornou {resp.status_code}: {resp.text[:200]}"
    except requests.RequestException as e:
        return False, str(e)


def _run_sync_thread(full_mode: bool = False):
    """Run main.py in a background thread, streaming output to a log file."""
    main_py = str(DATA_DIR / "main.py")
    _sync_state["running"] = True
    _sync_state["exit_code"] = None

    try:
        with open(_SYNC_LOG_FILE, "w", encoding="utf-8") as f:
            f.write("[Starting sync...]\n")
            if full_mode:
                f.write("[Mode: FULL - fetching ALL orders from WooCommerce]\n")
            f.flush()

            env = os.environ.copy()
            env.setdefault("MPLCONFIGDIR", os.path.join(env.get("TMPDIR", "/tmp"), "matplotlib"))
            env.setdefault("MPLBACKEND", "Agg")

            args = [sys.executable, "-u", main_py]
            if full_mode:
                args.append("--full")
            proc = subprocess.Popen(
                args,
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
    Output("sync-log", "children"),
    Output("sync-step", "children"),
    Input("sync-btn", "n_clicks"),
    State("sync-full-check", "value"),
    prevent_initial_call=True,
)
def start_sync(n_clicks, full_check):
    """Start background sync when button is clicked."""
    if not n_clicks:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    if _sync_state["running"]:
        return True, "Sync already running...", True, False, {"display": "block"}, no_update, no_update

    # Em produção (Render): acionar Cron Job via API em vez de subprocess (evita exceder memória)
    if _IS_RENDER:
        ok, msg = _trigger_render_cron()
        log_text = msg if ok else f"[Erro] {msg}"
        return (
            False,
            msg,
            False,
            True,
            {"display": "block"},
            log_text,
            "Done" if ok else "Failed",
        )

    full_mode = bool(full_check and "full" in full_check)

    with open(_SYNC_LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")

    thread = threading.Thread(target=_run_sync_thread, daemon=True, args=(full_mode,))
    thread.start()

    return (
        True,
        "Syncing...",
        True,
        False,
        {"display": "block"},
        no_update,
        no_update,
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



