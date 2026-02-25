"""
Cross-sell analysis and order bump management page.
"""
import os
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc, callback, Output, Input, State, no_update, ctx, ALL
import order_bumps as ob_api
from config import (
    COLORS, FONT, PLOT_LAYOUT, CATEGORY_COLORS, GENERIC_CATS,
    card_style, section_label, kpi_card, _th_style, _td_style,
    dropdown_style, parse_categories,
)
from data_loader import (
    hist_df, event_status_map, get_cross_sell_df,
    get_multi_product_orders_df, get_multi_order_stats,
    DISPLAY_CURRENCY, currency_symbol, _format_converted_total,
    convert_revenue, get_exchange_rates, ONLINE_COURSE_CATS, _lazy_cache,
)


def layout():
    return [
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "28px"}, children=[
                html.Div(children=[
                    dcc.Link("< Back to Dashboard", href="/", style={
                        "color": COLORS["text_muted"], "fontSize": "13px", "textDecoration": "none",
                        "marginBottom": "8px", "display": "block",
                    }),
                    section_label("CROSS-SELL ANALYSIS"),
                    html.H2("Products Bought Together", style={
                        "margin": "0", "fontSize": "24px", "fontWeight": "700",
                        "background": "linear-gradient(90deg, #c8a44e, #e0c87a, #b87348)",
                        "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                    }),
                    html.P("Discover which products customers buy together to find cross-sell opportunities.",
                           style={"color": COLORS["text_muted"], "fontSize": "14px", "margin": "4px 0 0"}),
                ]),
            ]),

            # KPI row
            html.Div(id="crosssell-kpis",
                      style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "28px"}),

            # Filters
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                section_label("FILTERS"),
                html.Div(style={"display": "flex", "gap": "16px", "alignItems": "flex-end", "flexWrap": "wrap"}, children=[
                    html.Div(style={"flex": "1", "minWidth": "250px"}, children=[
                        html.Label("Filter by Category:", style={
                            "fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                        }),
                        dcc.Dropdown(
                            id="crosssell-category-filter",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="All categories...",
                            style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                        ),
                    ]),
                    html.Div(style={"flex": "1", "minWidth": "250px"}, children=[
                        html.Label("Focus on Product:", style={
                            "fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                        }),
                        dcc.Dropdown(
                            id="crosssell-product-filter",
                            options=[],
                            value=None,
                            multi=False,
                            placeholder="Select a product to see its pairings...",
                            style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                        ),
                    ]),
                    html.Div(style={"flex": "0"}, children=[
                        html.Label("\u00a0", style={"fontSize": "13px", "display": "block", "marginBottom": "4px"}),
                        html.Button("Export CSV", id="crosssell-export-btn", n_clicks=0, style={
                            "backgroundColor": "transparent", "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                            "padding": "8px 18px", "fontSize": "12px", "fontWeight": "600",
                            "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                        }),
                    ]),
                ]),
            ]),
            dcc.Download(id="crosssell-export-download"),

            # Top pairs table
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                section_label("TOP PRODUCT PAIRS"),
                html.P("Products most frequently purchased in the same order.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px"}),
                html.Div(id="crosssell-pairs-table", style={"overflowX": "auto", "maxHeight": "600px", "overflowY": "auto"}),
            ]),

            # Visualization: bar chart of top pairs
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                section_label("PAIR FREQUENCY"),
                html.P("Visual breakdown of the most common product combinations.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "14px"}),
                dcc.Graph(id="crosssell-chart", config={"displayModeBar": False}, style={"height": "500px"}),
            ]),

            # â”€â”€ ORDER BUMP MANAGEMENT â”€â”€
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                "marginBottom": "18px", "flexWrap": "wrap", "gap": "10px"}, children=[
                    html.Div(children=[
                        section_label("CHECKOUT OFFERS"),
                        html.H3("Checkout Offers (Order Bumps)", style={
                            "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                        }),
                        html.P("Show extra products at checkout when customers are ready to buy. AI suggests offers based on what's frequently bought together.",
                               style={"color": COLORS["text_muted"], "fontSize": "12px", "margin": "0"}),
                    ]),
                    html.Div(style={"display": "flex", "gap": "8px", "alignItems": "center"}, children=[
                        html.Button("Refresh", id="ob-refresh-btn", n_clicks=0, style={
                            "backgroundColor": "transparent", "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                            "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                            "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                        }),
                    ]),
                ]),
                # ── Optional product page URL for AI context ──
                html.Div(style={
                    "display": "flex", "alignItems": "flex-end", "gap": "12px",
                    "flexWrap": "wrap", "marginBottom": "16px",
                    "padding": "12px 16px", "borderRadius": "8px",
                    "background": "rgba(200,164,78,0.04)",
                    "border": f"1px solid {COLORS['card_border']}",
                }, children=[
                    html.Div(style={"flex": "1", "minWidth": "280px"}, children=[
                        html.Label(
                            "🔗 Product Page URL (optional — AI reads the page to write better copy):",
                            style={
                                "fontSize": "12px", "color": COLORS["text_muted"],
                                "marginBottom": "4px", "display": "block",
                            },
                        ),
                        dcc.Input(
                            id="ob-page-url-input",
                            type="url",
                            placeholder="https://tcche.org/produto/nome-do-produto",
                            debounce=False,
                            style={
                                "width": "100%", "backgroundColor": COLORS["card"],
                                "color": COLORS["text"],
                                "border": f"1px solid {COLORS['card_border']}",
                                "borderRadius": "6px", "padding": "8px 12px",
                                "fontSize": "13px", "fontFamily": FONT,
                            },
                        ),
                    ]),
                    html.Div(style={"paddingBottom": "2px"}, children=[
                        html.Span(
                            "Applies to all AI generation on this page",
                            style={
                                "fontSize": "11px", "color": COLORS["text_muted"],
                                "fontStyle": "italic",
                            },
                        ),
                    ]),
                ]),

                html.Div(id="ob-status-msg", style={"marginBottom": "12px"}),
                html.Div(id="ob-existing-bumps", style={"marginBottom": "20px"}),
                html.Div(id="ob-suggestions-table", style={"overflowX": "auto", "maxHeight": "600px", "overflowY": "auto"}),
                dcc.Store(id="ob-create-trigger", data=0),
                html.Div(id="ob-create-result", style={"marginTop": "12px"}),

                # â”€â”€ Manual bump creation form â”€â”€
                html.Hr(style={"border": "none", "borderTop": f"1px solid {COLORS['card_border']}", "margin": "24px 0"}),
                html.Div(children=[
                    html.H4("Create New Offer", style={
                        "fontSize": "14px", "fontWeight": "600", "marginBottom": "4px",
                        "color": COLORS["accent"],
                    }),
                    html.P("Set up a checkout offer: choose what to offer and when to show it.",
                           style={"color": COLORS["text_muted"], "fontSize": "12px", "marginBottom": "14px"}),
                    html.Div(style={"marginBottom": "12px"}, children=[
                        html.Label("Product to Offer (shown to the customer at checkout):", style={
                            "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                        }),
                        dcc.Dropdown(
                            id="ob-manual-bump-product",
                            options=[],
                            value=None,
                            placeholder="Choose the product to offer...",
                            style={"backgroundColor": COLORS["bg"], "fontSize": "13px", "maxWidth": "500px"},
                        ),
                    ]),
                    html.Div(style={"marginBottom": "12px"}, children=[
                        html.Label("When to Show This Offer:", style={
                            "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "6px", "display": "block",
                        }),
                        dcc.RadioItems(
                            id="ob-trigger-mode",
                            options=[
                                {"label": " When customer adds specific products to cart", "value": "products"},
                                {"label": " When customer adds any product from a category", "value": "category"},
                                {"label": " Show to everyone at checkout", "value": "none"},
                            ],
                            value="products",
                            inline=False,
                            style={"fontSize": "13px", "display": "flex", "flexDirection": "column", "gap": "8px"},
                            inputStyle={"marginRight": "6px"},
                            labelStyle={"color": COLORS["text"], "cursor": "pointer"},
                        ),
                    ]),
                    html.Div(id="ob-trigger-products-row", children=[
                        html.Label("Show this offer when any of these products are in the cart:", style={
                            "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                        }),
                        dcc.Dropdown(
                            id="ob-manual-trigger-product",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select one or more products...",
                            style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                        ),
                    ]),
                    html.Div(id="ob-trigger-category-row", style={"display": "none"}, children=[
                        html.Label("Show this offer when any product from this category is in the cart:", style={
                            "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                        }),
                        dcc.Dropdown(
                            id="ob-manual-trigger-category",
                            options=[],
                            value=None,
                            placeholder="Select a category...",
                            style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                        ),
                    ]),
                    # Generate button
                    html.Div(style={"marginTop": "14px"}, children=[
                        html.Button("Generate with AI", id="ob-manual-create-btn", n_clicks=0, style={
                            "backgroundColor": COLORS["accent"], "color": "#0b0b14",
                            "border": "none", "borderRadius": "6px",
                            "padding": "9px 22px", "fontSize": "12px", "fontWeight": "700",
                            "cursor": "pointer", "fontFamily": FONT,
                        }),
                    ]),
                    html.Div(id="ob-manual-create-result", style={"marginTop": "10px"}),
                ]),

                # â”€â”€ Uncovered Products section â”€â”€
                html.Hr(style={"border": "none", "borderTop": f"1px solid {COLORS['card_border']}", "margin": "24px 0"}),
                html.Div(children=[
                    html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                    "marginBottom": "14px", "flexWrap": "wrap", "gap": "10px"}, children=[
                        html.Div(children=[
                            html.H4("Products Without Checkout Offers", style={
                                "fontSize": "14px", "fontWeight": "600", "marginBottom": "4px",
                                "color": COLORS["red"],
                            }),
                            html.P("These products don't have any checkout offer yet. "
                                   "Select the ones you want and use Auto-Fill, or create offers manually above.",
                                   style={"color": COLORS["text_muted"], "fontSize": "12px", "margin": "0"}),
                        ]),
                        html.Div(children=[
                            html.Button("Delete All Offers", id="ob-delete-all-btn", n_clicks=0, style={
                                "backgroundColor": "transparent", "color": COLORS["red"],
                                "border": f"1px solid {COLORS['red']}", "borderRadius": "6px",
                                "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                                "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                            }),
                        ]),
                    ]),
                    html.Div(id="ob-delete-all-result", style={"marginBottom": "10px"}),
                    # Auto-fill controls
                    html.Div(style={
                        "display": "flex", "gap": "12px", "alignItems": "flex-end",
                        "flexWrap": "wrap", "marginBottom": "14px",
                        "padding": "14px", "borderRadius": "8px",
                        "background": "rgba(200,164,78,0.04)",
                        "border": f"1px solid {COLORS['card_border']}",
                    }, children=[
                        html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                            html.Label("Product to offer as checkout add-on:", style={
                                "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                            }),
                            dcc.Dropdown(
                                id="ob-autofill-product",
                                options=[],
                                value=None,
                                placeholder="Select product to offer...",
                                style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                            ),
                        ]),
                        html.Div(style={"display": "flex", "alignItems": "center", "gap": "6px", "paddingBottom": "4px"}, children=[
                            dcc.Checklist(
                                id="ob-autofill-random",
                                options=[{"label": " Pick a random course each time", "value": "random"}],
                                value=[],
                                style={"fontSize": "12px"},
                                inputStyle={"marginRight": "4px"},
                                labelStyle={"color": COLORS["text"], "cursor": "pointer"},
                            ),
                        ]),
                        html.Div(style={"minWidth": "140px"}, children=[
                            html.Label("Design Preset:", style={
                                "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                            }),
                            dcc.Dropdown(
                                id="ob-autofill-design-style",
                                options=[
                                    {"label": "Classic", "value": "classic"},
                                    {"label": "Minimal", "value": "minimal"},
                                    {"label": "Bold", "value": "bold"},
                                    {"label": "Rounded", "value": "rounded"},
                                ],
                                value="classic",
                                clearable=False,
                                style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                            ),
                        ]),
                        html.Div(style={"paddingBottom": "0px"}, children=[
                            html.Button("Create Offers for Selected", id="ob-autofill-btn", n_clicks=0, style={
                                "backgroundColor": COLORS["accent"], "color": "#0b0b14",
                                "border": "none", "borderRadius": "6px",
                                "padding": "8px 18px", "fontSize": "12px", "fontWeight": "700",
                                "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                            }),
                        ]),
                    ]),
                    html.P("Creates one checkout offer per selected product using AI-generated text. "
                           "When a customer adds one of these products to their cart, they'll see the offer you chose above. "
                           "You can delete all offers at any time with the button above.",
                           style={"color": COLORS["text_muted"], "fontSize": "11px", "margin": "0 0 12px",
                                  "fontStyle": "italic"}),
                    dcc.Loading(id="ob-autofill-loading", type="dot", color=COLORS["accent"],
                                children=html.Div(id="ob-autofill-result", style={"marginBottom": "10px"})),
                    html.Div(id="ob-uncovered-header"),
                    html.Div(style={"display": "flex", "gap": "8px", "marginBottom": "10px", "alignItems": "center"}, children=[
                        html.Button("Select All", id="ob-select-all-btn", n_clicks=0, style={
                            "backgroundColor": "transparent", "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}", "borderRadius": "4px",
                            "padding": "4px 12px", "fontSize": "11px", "fontWeight": "600",
                            "cursor": "pointer", "fontFamily": FONT,
                        }),
                        html.Button("Deselect All", id="ob-deselect-all-btn", n_clicks=0, style={
                            "backgroundColor": "transparent", "color": COLORS["text_muted"],
                            "border": f"1px solid {COLORS['card_border']}", "borderRadius": "4px",
                            "padding": "4px 12px", "fontSize": "11px", "fontWeight": "600",
                            "cursor": "pointer", "fontFamily": FONT,
                        }),
                        html.Span(id="ob-selected-count", style={"color": COLORS["text_muted"], "fontSize": "12px", "marginLeft": "6px"}),
                    ]),
                    html.Div(style={
                        "display": "grid",
                        "gridTemplateColumns": "28px 36px 1fr 1fr 90px",
                        "gap": "10px", "padding": "8px 6px",
                        "borderBottom": f"1px solid {COLORS['card_border']}",
                        "alignItems": "center",
                    }, children=[
                        html.Span(""),
                        html.Span("#", style={"fontSize": "11px", "color": COLORS["text_muted"], "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "1px"}),
                        html.Span("Product", style={"fontSize": "11px", "color": COLORS["text_muted"], "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "1px"}),
                        html.Span("Event / Course", style={"fontSize": "11px", "color": COLORS["text_muted"], "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "1px"}),
                        html.Span("Type", style={"fontSize": "11px", "color": COLORS["text_muted"], "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "1px"}),
                    ]),
                    html.Div(style={"overflowY": "auto", "maxHeight": "500px"}, children=[
                        dcc.Checklist(
                            id="ob-uncovered-checklist",
                            options=[],
                            value=[],
                            style={"width": "100%"},
                            inputStyle={"marginRight": "8px", "cursor": "pointer", "flexShrink": "0"},
                            labelStyle={
                                "display": "flex", "alignItems": "center", "width": "100%",
                                "padding": "7px 0", "borderBottom": f"1px solid rgba(255,255,255,0.04)",
                                "color": COLORS["text"], "cursor": "pointer",
                            },
                        ),
                    ]),
                ]),

                # â”€â”€ AI Preview / Edit panel (shared by suggestions + manual) â”€â”€
                html.Div(id="ob-preview-panel", style={"display": "none"}, children=[
                    html.Hr(style={"border": "none", "borderTop": f"1px solid {COLORS['card_border']}", "margin": "24px 0"}),
                    html.Div(style={
                        "padding": "20px", "borderRadius": "10px",
                        "background": "rgba(200,164,78,0.06)",
                        "border": f"1px solid {COLORS['accent']}",
                    }, children=[
                        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                                        "marginBottom": "14px"}, children=[
                            html.H4("Review & Edit Offer Text", style={
                                "fontSize": "15px", "fontWeight": "700", "margin": "0",
                                "color": COLORS["accent"],
                            }),
                            html.Span("Generated by AI â€” feel free to edit before confirming", style={
                                "fontSize": "11px", "color": COLORS["text_muted"],
                                "fontStyle": "italic",
                            }),
                        ]),
                        dcc.Store(id="ob-preview-store", data={}),
                        # ── URL field for AI context ──
                        html.Div(style={"marginBottom": "14px"}, children=[
                            html.Label(
                                "🔗 URL da página do produto (opcional — a IA lê a página e escreve um copy melhor):",
                                style={
                                    "fontSize": "12px", "color": COLORS["text_muted"],
                                    "marginBottom": "4px", "display": "block",
                                },
                            ),
                            html.Div(style={"display": "flex", "gap": "8px", "alignItems": "center"}, children=[
                                dcc.Input(
                                    id="ob-preview-url-input",
                                    type="url",
                                    placeholder="https://tcche.org/produto/nome-do-produto",
                                    debounce=False,
                                    style={
                                        "flex": "1", "backgroundColor": COLORS["bg"],
                                        "color": COLORS["text"],
                                        "border": f"1px solid {COLORS['card_border']}",
                                        "borderRadius": "6px", "padding": "8px 12px",
                                        "fontSize": "13px", "fontFamily": FONT,
                                    },
                                ),
                                html.Span(
                                    "Cole a URL e clique Regenerate",
                                    style={
                                        "fontSize": "11px", "color": COLORS["text_muted"],
                                        "fontStyle": "italic", "whiteSpace": "nowrap",
                                    },
                                ),
                            ]),
                        ]),
                        html.Div(style={"display": "flex", "gap": "14px", "flexWrap": "wrap", "marginBottom": "12px"}, children=[
                            html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                                html.Label("Internal Name (only you see this):", style={
                                    "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                                }),
                                dcc.Input(id="ob-preview-title", type="text", value="", style={
                                    "width": "100%", "backgroundColor": COLORS["card"],
                                    "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "6px", "padding": "8px 12px", "fontSize": "13px",
                                    "fontFamily": FONT,
                                }),
                            ]),
                            html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                                html.Label("Headline (customers see this at checkout):", style={
                                    "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                                }),
                                dcc.Input(id="ob-preview-headline", type="text", value="", style={
                                    "width": "100%", "backgroundColor": COLORS["card"],
                                    "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                    "borderRadius": "6px", "padding": "8px 12px", "fontSize": "13px",
                                    "fontFamily": FONT,
                                }),
                            ]),
                            html.Div(style={"flex": "1", "minWidth": "160px"}, children=[
                                html.Label("Design Preset:", style={
                                    "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                                }),
                                dcc.Dropdown(
                                    id="ob-preview-design-style",
                                    options=[
                                        {"label": "Classic", "value": "classic"},
                                        {"label": "Minimal", "value": "minimal"},
                                        {"label": "Bold", "value": "bold"},
                                        {"label": "Rounded", "value": "rounded"},
                                    ],
                                    value="classic",
                                    clearable=False,
                                    style={"backgroundColor": COLORS["bg"], "fontSize": "13px"},
                                ),
                            ]),
                        ]),
                        html.Div(style={"marginBottom": "14px"}, children=[
                            html.Label("Description (shown below headline, supports <b>bold</b> HTML):", style={
                                "fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "4px", "display": "block",
                            }),
                            dcc.Textarea(id="ob-preview-description", value="", style={
                                "width": "100%", "minHeight": "60px",
                                "backgroundColor": COLORS["card"],
                                "color": COLORS["text"], "border": f"1px solid {COLORS['card_border']}",
                                "borderRadius": "6px", "padding": "8px 12px", "fontSize": "13px",
                                "fontFamily": FONT, "resize": "vertical",
                            }),
                        ]),
                        html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center"}, children=[
                            html.Button("Confirm & Create Offer", id="ob-confirm-create-btn", n_clicks=0, style={
                                "backgroundColor": COLORS["accent3"], "color": "#fff",
                                "border": "none", "borderRadius": "6px",
                                "padding": "9px 24px", "fontSize": "13px", "fontWeight": "700",
                                "cursor": "pointer", "fontFamily": FONT,
                            }),
                            html.Button("Regenerate", id="ob-regenerate-btn", n_clicks=0, style={
                                "backgroundColor": "transparent", "color": COLORS["accent"],
                                "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                                "padding": "8px 16px", "fontSize": "12px", "fontWeight": "600",
                                "cursor": "pointer", "fontFamily": FONT,
                            }),
                            html.Button("Cancel", id="ob-cancel-preview-btn", n_clicks=0, style={
                                "backgroundColor": "transparent", "color": COLORS["text_muted"],
                                "border": f"1px solid {COLORS['card_border']}", "borderRadius": "6px",
                                "padding": "8px 16px", "fontSize": "12px", "fontWeight": "600",
                                "cursor": "pointer", "fontFamily": FONT,
                            }),
                            dcc.Loading(id="ob-confirm-loading", type="dot", color=COLORS["accent"],
                                        children=html.Div(id="ob-confirm-result", style={"marginLeft": "8px"})),
                        ]),
                    ]),
                ]),
            ]),

            # â”€â”€ ORDER BUMP ANALYTICS â”€â”€
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                "marginBottom": "18px", "flexWrap": "wrap", "gap": "10px"}, children=[
                    html.Div(children=[
                        section_label("OFFER PERFORMANCE"),
                        html.H3("How Are Your Offers Doing?", style={
                            "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                        }),
                        html.P("See how customers interact with your checkout offers â€” are they seeing them, accepting them, and how much extra revenue they generate.",
                               style={"color": COLORS["text_muted"], "fontSize": "12px", "margin": "0"}),
                    ]),
                ]),
                html.Div(id="ob-analytics-kpis", style={
                    "display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "20px",
                }),
                html.Div(id="ob-analytics-table", style={
                    "overflowX": "auto", "marginBottom": "20px",
                }),
                dcc.Graph(id="ob-analytics-chart", config={"displayModeBar": False}, style={"height": "300px"}),
            ]),

            # Multi-product orders detail
            html.Div(style=card_style({}), children=[
                html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start",
                                "marginBottom": "16px", "flexWrap": "wrap", "gap": "10px"}, children=[
                    html.Div(children=[
                        section_label("MULTI-PRODUCT ORDERS"),
                        html.H3("Orders with 2+ Products", style={
                            "margin": "0 0 4px", "fontSize": "18px", "fontWeight": "700",
                        }),
                        html.P("Each row is a line item from an order where the customer bought multiple products.",
                               style={"color": COLORS["text_muted"], "fontSize": "12px", "margin": "0"}),
                    ]),
                    html.Div(style={"display": "flex", "gap": "8px", "alignItems": "center"}, children=[
                        dcc.Input(
                            id="crosssell-orders-search",
                            type="text",
                            placeholder="Search order or product...",
                            debounce=True,
                            style={
                                "width": "200px", "padding": "8px 12px",
                                "background": COLORS["bg"],
                                "border": f"1px solid {COLORS['card_border']}",
                                "borderRadius": "8px", "color": COLORS["text"],
                                "fontFamily": FONT, "fontSize": "12px",
                            },
                        ),
                        html.Button("Export CSV", id="crosssell-orders-export-btn", n_clicks=0, style={
                            "backgroundColor": "transparent", "color": COLORS["accent"],
                            "border": f"1px solid {COLORS['accent']}", "borderRadius": "6px",
                            "padding": "6px 14px", "fontSize": "11px", "fontWeight": "600",
                            "cursor": "pointer", "fontFamily": FONT, "whiteSpace": "nowrap",
                        }),
                    ]),
                ]),
                dcc.Download(id="crosssell-orders-download"),
                html.Div(id="crosssell-orders-table", style={
                    "overflowX": "auto", "maxHeight": "700px", "overflowY": "auto",
                }),
            ]),
    ]


# ============================================================
# CROSS-SELL ANALYSIS CALLBACKS
# ============================================================


@callback(
    Output("crosssell-kpis", "children"),
    Input("url", "pathname"),
)
def render_crosssell_kpis(pathname):
    """Show KPI cards for multi-product orders."""
    if pathname != "/cross-sell":
        return no_update
    stats = get_multi_order_stats()
    total = int(stats.get("total_orders", 0))
    multi = int(stats.get("multi_orders", 0))
    pct = (multi / total * 100) if total else 0
    avg = float(stats.get("avg_products", 0))
    mx = int(stats.get("max_products", 0))
    cs_df = get_cross_sell_df()
    unique_pairs = len(cs_df)

    return [
        kpi_card("Total Orders", f"{total:,}"),
        kpi_card("Multi-Product Orders", f"{multi:,} ({pct:.1f}%)", color=COLORS["accent3"]),
        kpi_card("Avg Products/Order", f"{avg:.2f}", color=COLORS["accent4"]),
        kpi_card("Max Products in Order", str(mx), color=COLORS["accent2"]),
        kpi_card("Unique Pairs Found", f"{unique_pairs:,}", color="#6ea8d9"),
    ]


@callback(
    Output("crosssell-category-filter", "options"),
    Input("url", "pathname"),
)
def populate_crosssell_cats(pathname):
    if pathname != "/cross-sell":
        return no_update
    cs_df = get_cross_sell_df()
    if cs_df.empty:
        return []
    cats = set()
    for col in ["category_a", "category_b"]:
        if col in cs_df.columns:
            for val in cs_df[col].dropna().unique():
                for c in parse_categories(val):
                    if c and c != "Uncategorized" and c != "Sem categoria":
                        cats.add(c)
    return [{"label": c, "value": c} for c in sorted(cats)]


@callback(
    Output("crosssell-product-filter", "options"),
    Input("url", "pathname"),
    Input("crosssell-category-filter", "value"),
)
def populate_crosssell_products(pathname, selected_cats):
    if pathname != "/cross-sell":
        return no_update
    cs_df = get_cross_sell_df()
    if cs_df.empty:
        return []
    df = cs_df
    if selected_cats:
        mask_a = df["category_a"].apply(lambda c: any(sc in str(c) for sc in selected_cats))
        mask_b = df["category_b"].apply(lambda c: any(sc in str(c) for sc in selected_cats))
        df = df[mask_a | mask_b]
    products = {}
    for _, row in df.iterrows():
        products[int(row["product_a_id"])] = str(row["product_a_name"])
        products[int(row["product_b_id"])] = str(row["product_b_name"])
    return [{"label": f"{name} (#{pid})", "value": pid} for pid, name in sorted(products.items(), key=lambda x: x[1])]


def _filter_crosssell(selected_cats=None, product_id=None):
    """Apply category and product filters to cross-sell data."""
    cs_df = get_cross_sell_df()
    if cs_df.empty:
        return cs_df
    df = cs_df.copy()
    if selected_cats:
        mask_a = df["category_a"].apply(lambda c: any(sc in str(c) for sc in selected_cats))
        mask_b = df["category_b"].apply(lambda c: any(sc in str(c) for sc in selected_cats))
        df = df[mask_a | mask_b]
    if product_id is not None:
        df = df[(df["product_a_id"] == product_id) | (df["product_b_id"] == product_id)]
    return df.sort_values("pair_count", ascending=False)


@callback(
    Output("crosssell-pairs-table", "children"),
    Input("url", "pathname"),
    Input("crosssell-category-filter", "value"),
    Input("crosssell-product-filter", "value"),
)
def render_crosssell_table(pathname, selected_cats, product_id):
    """Render the top product pairs table."""
    if pathname != "/cross-sell":
        return no_update
    df = _filter_crosssell(selected_cats or None, product_id)
    if df.empty:
        return html.P("No cross-sell data found. This analysis requires orders with 2+ products.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px"})

    total_pairs = df["pair_count"].sum()
    header = html.Tr([
        html.Th("#", style=_th_style({"width": "40px"})),
        html.Th("Product A", style=_th_style()),
        html.Th("Product B", style=_th_style()),
        html.Th("Times Bought Together", style=_th_style({"textAlign": "right"})),
        html.Th("% of Pairs", style=_th_style({"textAlign": "right"})),
        html.Th("Combined Revenue", style=_th_style({"textAlign": "right"})),
    ])

    rows = []
    for i, (_, r) in enumerate(df.head(50).iterrows(), 1):
        pct = r["pair_count"] / total_pairs * 100 if total_pairs else 0
        # Truncate long product names
        name_a = str(r["product_a_name"])[:45] + ("..." if len(str(r["product_a_name"])) > 45 else "")
        name_b = str(r["product_b_name"])[:45] + ("..." if len(str(r["product_b_name"])) > 45 else "")
        rows.append(html.Tr([
            html.Td(str(i), style=_td_style({"color": COLORS["text_muted"], "width": "40px"})),
            html.Td(name_a, style=_td_style({"fontWeight": "600"})),
            html.Td(name_b, style=_td_style({"fontWeight": "600"})),
            html.Td(f"{int(r['pair_count']):,}", style=_td_style({"textAlign": "right", "fontWeight": "700", "color": COLORS["accent"]})),
            html.Td(f"{pct:.1f}%", style=_td_style({"textAlign": "right", "color": COLORS["text_muted"]})),
            html.Td(f"${r['total_revenue']:,.2f}", style=_td_style({"textAlign": "right"})),
        ]))

    summary = html.Div(
        f"Showing top {min(len(df), 50)} of {len(df)} pairs  Â·  "
        f"Total co-occurrences: {int(total_pairs):,}",
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
    Output("crosssell-chart", "figure"),
    Input("url", "pathname"),
    Input("crosssell-category-filter", "value"),
    Input("crosssell-product-filter", "value"),
)
def render_crosssell_chart(pathname, selected_cats, product_id):
    """Render horizontal bar chart of top product pairs."""
    fig = go.Figure()
    if pathname != "/cross-sell":
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    df = _filter_crosssell(selected_cats or None, product_id)
    if df.empty:
        fig.update_layout(**PLOT_LAYOUT)
        return fig

    top = df.head(15).copy()
    top = top.sort_values("pair_count", ascending=True)

    # Build pair labels
    top["pair_label"] = top.apply(
        lambda r: (str(r["product_a_name"])[:25] + "  +  " + str(r["product_b_name"])[:25]),
        axis=1,
    )

    fig.add_trace(go.Bar(
        y=top["pair_label"],
        x=top["pair_count"],
        orientation="h",
        marker_color=COLORS["accent"],
        marker_line_width=0,
        text=top["pair_count"].apply(lambda x: f"{int(x):,}"),
        textposition="auto",
        textfont=dict(size=11, color="#fff"),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Bought together: %{x:,} times<br>"
            "Revenue: $%{customdata[0]:,.0f}"
            "<extra></extra>"
        ),
        customdata=top[["total_revenue"]].values,
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=COLORS["text"], size=11),
        showlegend=False,
        margin=dict(l=300, r=30, t=10, b=10),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showline=False),
        bargap=0.2,
    )
    return fig


@callback(
    Output("crosssell-export-download", "data"),
    Input("crosssell-export-btn", "n_clicks"),
    State("crosssell-category-filter", "value"),
    State("crosssell-product-filter", "value"),
    prevent_initial_call=True,
)
def export_crosssell(n_clicks, selected_cats, product_id):
    """Export cross-sell data as CSV."""
    if not n_clicks:
        return no_update
    df = _filter_crosssell(selected_cats or None, product_id)
    if df.empty:
        return no_update
    export = df.rename(columns={
        "product_a_id": "Product A ID", "product_a_name": "Product A",
        "product_b_id": "Product B ID", "product_b_name": "Product B",
        "category_a": "Category A", "category_b": "Category B",
        "pair_count": "Times Bought Together", "total_qty": "Combined Qty",
        "total_revenue": "Combined Revenue",
    })
    return dcc.send_data_frame(export.to_csv, "cross_sell_analysis.csv", index=False)


# --- Multi-product orders table ---

def _filter_multi_orders(selected_cats=None, product_id=None, search_text=None):
    """Filter multi-product orders based on cross-sell page filters."""
    df = get_multi_product_orders_df()
    if df.empty:
        return df

    if selected_cats:
        mask = df["category"].apply(lambda c: any(sc in str(c) for sc in selected_cats))
        df = df[mask]

    if product_id is not None:
        order_ids = df.loc[df["product_id"] == product_id, "order_id"].unique()
        df = df[df["order_id"].isin(order_ids)]

    if search_text:
        q = search_text.strip().lower()
        df = df[
            df["order_id"].astype(str).str.contains(q, na=False)
            | df["product_name"].str.lower().str.contains(q, na=False)
            | df["billing_city"].str.lower().str.contains(q, na=False)
        ]

    return df


@callback(
    Output("crosssell-orders-table", "children"),
    Input("url", "pathname"),
    Input("crosssell-category-filter", "value"),
    Input("crosssell-product-filter", "value"),
    Input("crosssell-orders-search", "value"),
)
def render_multi_orders_table(pathname, selected_cats, product_id, search_text):
    """Render grouped table of multi-product orders."""
    if pathname != "/cross-sell":
        return html.P("")

    df = _filter_multi_orders(selected_cats or None, product_id, search_text)
    if df.empty:
        return html.P("No multi-product orders found.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px"})

    # Group by order_id and build visual rows
    grouped = df.groupby("order_id", sort=False)
    unique_orders = list(grouped.groups.keys())
    display_limit = 100

    header = html.Tr([
        html.Th("Order ID", style=_th_style()),
        html.Th("Date", style=_th_style()),
        html.Th("Products", style=_th_style()),
        html.Th("Qty", style=_th_style({"textAlign": "right"})),
        html.Th("Total", style=_th_style({"textAlign": "right"})),
        html.Th("Location", style=_th_style()),
    ])

    rows = []
    _accent_bg = "rgba(200, 164, 78, 0.06)"
    for idx, oid in enumerate(unique_orders[:display_limit]):
        grp = grouped.get_group(oid)
        n_items = len(grp)
        first = grp.iloc[0]
        order_total = grp["total"].sum()
        order_qty = grp["quantity"].sum()
        date_str = first["order_date"].strftime("%Y-%m-%d") if pd.notna(first["order_date"]) else "â€”"
        location = ", ".join(filter(None, [str(first.get("billing_city", "") or ""), str(first.get("billing_country", "") or "")]))

        product_lines = []
        for _, item in grp.iterrows():
            name = str(item["product_name"])[:50] + ("..." if len(str(item["product_name"])) > 50 else "")
            product_lines.append(
                html.Div(style={"display": "flex", "justifyContent": "space-between", "gap": "12px",
                                "padding": "2px 0"}, children=[
                    html.Span(name, style={"fontSize": "12px"}),
                    html.Span(f"x{int(item['quantity'])}  ${item['total']:,.2f}",
                              style={"fontSize": "12px", "color": COLORS["text_muted"], "whiteSpace": "nowrap"}),
                ])
            )

        row_bg = _accent_bg if idx % 2 == 0 else "transparent"
        rows.append(html.Tr(style={"backgroundColor": row_bg}, children=[
            html.Td(
                html.Span(f"#{oid}", style={"fontWeight": "700", "color": COLORS["accent"]}),
                style=_td_style({"verticalAlign": "top"}),
            ),
            html.Td(date_str, style=_td_style({"verticalAlign": "top", "whiteSpace": "nowrap"})),
            html.Td(
                html.Div(product_lines),
                style=_td_style({"verticalAlign": "top"}),
            ),
            html.Td(f"{int(order_qty)}", style=_td_style({"textAlign": "right", "verticalAlign": "top", "fontWeight": "600"})),
            html.Td(f"${order_total:,.2f}", style=_td_style({"textAlign": "right", "verticalAlign": "top", "fontWeight": "600"})),
            html.Td(location or "â€”", style=_td_style({"verticalAlign": "top", "color": COLORS["text_muted"]})),
        ]))

    total_orders = len(unique_orders)
    summary = html.Div(
        f"Showing {min(total_orders, display_limit)} of {total_orders} multi-product orders  Â·  "
        f"{len(df)} line items",
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
    Output("crosssell-orders-download", "data"),
    Input("crosssell-orders-export-btn", "n_clicks"),
    State("crosssell-category-filter", "value"),
    State("crosssell-product-filter", "value"),
    State("crosssell-orders-search", "value"),
    prevent_initial_call=True,
)
def export_multi_orders(n_clicks, selected_cats, product_id, search_text):
    """Export multi-product orders as CSV."""
    if not n_clicks:
        return no_update
    df = _filter_multi_orders(selected_cats or None, product_id, search_text)
    if df.empty:
        return no_update
    export = df[["order_id", "order_date", "product_id", "product_name",
                 "quantity", "total", "currency", "category",
                 "billing_country", "billing_city"]].copy()
    export.columns = ["Order ID", "Date", "Product ID", "Product Name",
                      "Qty", "Total", "Currency", "Category",
                      "Country", "City"]
    return dcc.send_data_frame(export.to_csv, "multi_product_orders.csv", index=False)


# ============================================================

# ============================================================
# ORDER BUMP SUGGESTIONS CALLBACKS
# ============================================================


def _get_existing_bumps():
    """Fetch existing order bumps from plugin API (cached per page load)."""
    if "ob_bumps" not in _lazy_cache:
        _lazy_cache["ob_bumps"] = ob_api.list_bumps()
    return _lazy_cache["ob_bumps"]


def _bump_exists_for_product(product_id: int, trigger_id: int | None = None) -> dict | None:
    """Check if an active bump already targets this product."""
    for b in _get_existing_bumps():
        if b.get("bump_product_id") == product_id:
            if trigger_id is None:
                return b
            triggers = b.get("trigger_product_ids") or []
            if trigger_id in triggers or not triggers:
                return b
    return None


def _get_future_event_and_course_pids() -> set:
    """Return product IDs classified as 'active' (future event) or 'course'."""
    return {pid for pid, st in event_status_map.items() if st in ("active", "course")}


@callback(
    Output("ob-existing-bumps", "children"),
    Output("ob-suggestions-table", "children"),
    Input("url", "pathname"),
    Input("ob-refresh-btn", "n_clicks"),
    Input("crosssell-category-filter", "value"),
    Input("crosssell-product-filter", "value"),
)
def render_order_bump_section(pathname, refresh_clicks, selected_cats, product_id):
    """Render existing bumps summary and suggestion table."""
    if pathname != "/cross-sell":
        return no_update, no_update

    if not ob_api.is_configured():
        msg = html.Div(
            "WordPress Application Password not configured. Add WP_USER and WP_APP_PASSWORD to your .env file.",
            style={"color": COLORS["text_muted"], "fontSize": "13px", "padding": "12px",
                   "background": "rgba(212,74,74,0.08)", "borderRadius": "8px"},
        )
        return msg, html.P("")

    if refresh_clicks and ctx.triggered_id == "ob-refresh-btn":
        _lazy_cache.pop("ob_bumps", None)

    existing = _get_existing_bumps()
    cs_df = _filter_crosssell(selected_cats or None, product_id)

    # Filter suggestions to only include future events and courses
    future_pids = _get_future_event_and_course_pids()
    if not cs_df.empty and future_pids:
        cs_df = cs_df[
            cs_df["product_a_id"].isin(future_pids) & cs_df["product_b_id"].isin(future_pids)
        ]

    # â”€â”€ Existing bumps summary â”€â”€
    if existing:
        bump_rows = []
        for b in existing[:20]:
            status_color = COLORS["accent3"] if b.get("status") == "publish" else COLORS["text_muted"]
            bump_rows.append(html.Tr([
                html.Td(str(b.get("id", "")), style=_td_style({"width": "50px", "color": COLORS["text_muted"]})),
                html.Td(str(b.get("title", "")), style=_td_style({"fontWeight": "600"})),
                html.Td(str(b.get("headline", "")), style=_td_style()),
                html.Td(
                    html.Span(b.get("status", ""), style={
                        "color": status_color, "fontWeight": "600", "fontSize": "11px",
                        "textTransform": "uppercase",
                    }),
                    style=_td_style(),
                ),
                html.Td(
                    f"{b.get('discount_value', 0)}{'%' if b.get('discount_type') == 'percentage' else ''}"
                    if b.get("discount_type") != "none" else "â€”",
                    style=_td_style({"textAlign": "right"}),
                ),
                html.Td(b.get("design_style", ""), style=_td_style({"color": COLORS["text_muted"]})),
                html.Td(
                    html.Button("Delete", id={"type": "ob-delete-btn", "index": b["id"]}, n_clicks=0, style={
                        "backgroundColor": "transparent", "color": COLORS["red"],
                        "border": f"1px solid {COLORS['red']}", "borderRadius": "4px",
                        "padding": "3px 10px", "fontSize": "10px", "fontWeight": "600",
                        "cursor": "pointer", "fontFamily": FONT,
                    }),
                    style=_td_style(),
                ),
            ]))

        bump_header = html.Tr([
            html.Th("ID", style=_th_style({"width": "50px"})),
            html.Th("Offer Name", style=_th_style()),
            html.Th("Customer Headline", style=_th_style()),
            html.Th("Status", style=_th_style()),
            html.Th("Discount", style=_th_style({"textAlign": "right"})),
            html.Th("Style", style=_th_style()),
            html.Th("", style=_th_style({"width": "80px"})),
        ])

        existing_section = html.Div([
            html.H4(f"Your Active Offers ({len(existing)})", style={
                "fontSize": "14px", "fontWeight": "600", "marginBottom": "10px",
                "color": COLORS["accent3"],
            }),
            html.Div(style={"overflowX": "auto"}, children=[
                html.Table(
                    [html.Thead(bump_header), html.Tbody(bump_rows)],
                    style={"width": "100%", "borderCollapse": "collapse"},
                ),
            ]),
        ])
    else:
        existing_section = html.P(
            "No checkout offers created yet. Use the suggestions below or create one manually.",
            style={"color": COLORS["text_muted"], "fontSize": "13px"},
        )

    # â”€â”€ Suggestions from cross-sell data (filtered to future events & courses) â”€â”€
    if cs_df.empty:
        suggestions = html.P(
            "No suggestions yet â€” this needs orders with 2+ products to find patterns.",
            style={"color": COLORS["text_muted"], "fontSize": "13px"},
        )
    else:
        existing_bump_pids = {b.get("bump_product_id") for b in existing}
        top = cs_df.head(20)

        sug_header = html.Tr([
            html.Th("#", style=_th_style({"width": "40px"})),
            html.Th("When in Cart", style=_th_style()),
            html.Th("Suggest This Product", style=_th_style()),
            html.Th("Bought Together", style=_th_style({"textAlign": "right"})),
            html.Th("Revenue", style=_th_style({"textAlign": "right"})),
            html.Th("Status", style=_th_style()),
            html.Th("Action", style=_th_style({"width": "120px"})),
        ])

        sug_rows = []
        for i, (_, r) in enumerate(top.iterrows(), 1):
            pid_a = int(r["product_a_id"])
            pid_b = int(r["product_b_id"])
            name_a = str(r["product_a_name"])[:40] + ("..." if len(str(r["product_a_name"])) > 40 else "")
            name_b = str(r["product_b_name"])[:40] + ("..." if len(str(r["product_b_name"])) > 40 else "")

            already = pid_b in existing_bump_pids
            status_badge = html.Span(
                "Active" if already else "Not created",
                style={
                    "fontSize": "10px", "fontWeight": "600", "textTransform": "uppercase",
                    "padding": "2px 8px", "borderRadius": "4px",
                    "backgroundColor": "rgba(90,170,136,0.15)" if already else "rgba(138,132,122,0.15)",
                    "color": COLORS["accent3"] if already else COLORS["text_muted"],
                },
            )

            action = html.Button(
                "Already Active" if already else "Create Offer",
                id={"type": "ob-create-btn", "index": f"{pid_a}_{pid_b}"},
                n_clicks=0,
                disabled=already,
                style={
                    "backgroundColor": COLORS["accent"] if not already else "transparent",
                    "color": "#0b0b14" if not already else COLORS["text_muted"],
                    "border": "none" if not already else f"1px solid {COLORS['card_border']}",
                    "borderRadius": "6px", "padding": "5px 12px",
                    "fontSize": "11px", "fontWeight": "600",
                    "cursor": "pointer" if not already else "default",
                    "fontFamily": FONT, "whiteSpace": "nowrap",
                    "opacity": "0.5" if already else "1",
                },
            )

            sug_rows.append(html.Tr([
                html.Td(str(i), style=_td_style({"color": COLORS["text_muted"], "width": "40px"})),
                html.Td(html.Span([name_a, html.Span(f"  #{pid_a}", style={"color": COLORS["text_muted"], "fontSize": "11px"})]),
                         style=_td_style({"fontWeight": "600"})),
                html.Td(html.Span([name_b, html.Span(f"  #{pid_b}", style={"color": COLORS["text_muted"], "fontSize": "11px"})]),
                         style=_td_style({"fontWeight": "600"})),
                html.Td(f"{int(r['pair_count']):,}", style=_td_style({"textAlign": "right", "fontWeight": "700", "color": COLORS["accent"]})),
                html.Td(f"${r['total_revenue']:,.2f}", style=_td_style({"textAlign": "right"})),
                html.Td(status_badge, style=_td_style()),
                html.Td(action, style=_td_style()),
            ]))

        suggestions = html.Div([
            html.H4("Suggested Checkout Offers", style={
                "fontSize": "14px", "fontWeight": "600", "marginBottom": "4px",
            }),
            html.P("AI-detected patterns: these products are frequently bought together by customers.",
                   style={"color": COLORS["text_muted"], "fontSize": "12px", "marginBottom": "12px"}),
            html.Table(
                [html.Thead(sug_header), html.Tbody(sug_rows)],
                style={"width": "100%", "borderCollapse": "collapse"},
            ),
        ])

    return existing_section, suggestions


def _resolve_product_name(pid: int) -> str:
    """Get product name from hist_df by ID."""
    subset = hist_df[hist_df["product_id"] == pid]
    if not subset.empty:
        return str(subset.iloc[0]["product_name"])
    return f"Product #{pid}"


def _get_wc_categories_cached() -> list[dict]:
    """Fetch WC categories (cached)."""
    if "wc_categories" not in _lazy_cache:
        _lazy_cache["wc_categories"] = ob_api.list_wc_categories()
    return _lazy_cache["wc_categories"]


def _get_course_pids() -> set:
    """Product IDs classified as online courses."""
    return {pid for pid, st in event_status_map.items() if st == "course"}


def _compute_uncovered_pids(existing_bumps: list[dict]) -> set:
    """Return future event/course PIDs not covered by any existing bump."""
    future_pids = _get_future_event_and_course_pids()
    if not future_pids or not ob_api.is_configured():
        return future_pids

    covered = set()
    wc_cats = _get_wc_categories_cached()
    wc_cat_id_to_name = {c["id"]: c["name"] for c in wc_cats}

    pid_cat_str = hist_df.groupby("product_id")["category"].first().to_dict()

    for b in existing_bumps:
        bp = b.get("bump_product_id")
        if bp:
            covered.add(bp)
        for tp in (b.get("trigger_product_ids") or []):
            covered.add(tp)
        for cat_id in (b.get("trigger_category_ids") or []):
            cat_name = wc_cat_id_to_name.get(cat_id, "")
            if cat_name:
                for pid in future_pids:
                    cats = set(parse_categories(pid_cat_str.get(pid, "")))
                    if cat_name in cats:
                        covered.add(pid)

    return future_pids - covered


# â”€â”€ Suggestion button -> open AI preview â”€â”€

@callback(
    Output("ob-preview-panel", "style", allow_duplicate=True),
    Output("ob-preview-store", "data", allow_duplicate=True),
    Output("ob-preview-title", "value", allow_duplicate=True),
    Output("ob-preview-headline", "value", allow_duplicate=True),
    Output("ob-preview-description", "value", allow_duplicate=True),
    Output("ob-preview-url-input", "value", allow_duplicate=True),
    Output("ob-create-result", "children"),
    Input({"type": "ob-create-btn", "index": ALL}, "n_clicks"),
    State("ob-page-url-input", "value"),
    prevent_initial_call=True,
)
def handle_suggestion_generate(n_clicks_list, page_url):
    """When a suggestion 'Create Bump' is clicked, generate AI copy and show preview."""
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    btn_id = ctx.triggered_id
    if not isinstance(btn_id, dict):
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    idx = btn_id.get("index", "")
    parts = str(idx).split("_")
    if len(parts) != 2:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    try:
        trigger_pid = int(parts[0])
        bump_pid = int(parts[1])
    except ValueError:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update

    cs_df = get_cross_sell_df()
    pair = cs_df[
        ((cs_df["product_a_id"] == trigger_pid) & (cs_df["product_b_id"] == bump_pid))
        | ((cs_df["product_a_id"] == bump_pid) & (cs_df["product_b_id"] == trigger_pid))
    ]
    if pair.empty:
        bump_name = _resolve_product_name(bump_pid)
        trigger_name = _resolve_product_name(trigger_pid)
    else:
        row = pair.iloc[0]
        if int(row["product_a_id"]) == trigger_pid:
            trigger_name = str(row["product_a_name"])
            bump_name = str(row["product_b_name"])
        else:
            trigger_name = str(row["product_b_name"])
            bump_name = str(row["product_a_name"])

    copy = ob_api.generate_bump_copy(bump_name, trigger_name, page_url=page_url or None)
    store = {
        "bump_pid": bump_pid,
        "trigger_pids": [trigger_pid],
        "bump_name": bump_name, "trigger_name": trigger_name,
        "trigger_mode": "products",
        "page_url": page_url or None,
    }
    panel_style = {"display": "block"}
    loading_msg = html.Div(
        "AI copy generated â€” review and edit below, then confirm.",
        style={"color": COLORS["accent"], "fontSize": "12px", "fontWeight": "600"},
    )
    return panel_style, store, copy["title"], copy["headline"], copy["description"], page_url or "", loading_msg


# â”€â”€ Manual form -> open AI preview â”€â”€

@callback(
    Output("ob-preview-panel", "style", allow_duplicate=True),
    Output("ob-preview-store", "data", allow_duplicate=True),
    Output("ob-preview-title", "value", allow_duplicate=True),
    Output("ob-preview-headline", "value", allow_duplicate=True),
    Output("ob-preview-description", "value", allow_duplicate=True),
    Output("ob-preview-url-input", "value", allow_duplicate=True),
    Output("ob-manual-create-result", "children"),
    Input("ob-manual-create-btn", "n_clicks"),
    State("ob-manual-bump-product", "value"),
    State("ob-trigger-mode", "value"),
    State("ob-manual-trigger-product", "value"),
    State("ob-manual-trigger-category", "value"),
    State("ob-page-url-input", "value"),
    prevent_initial_call=True,
)
def handle_manual_generate(n_clicks, bump_pid, trigger_mode, trigger_pids, trigger_cat_id, page_url):
    """Manual form 'Generate with AI' -> generate copy and show preview."""
    _no = no_update
    if not n_clicks or not bump_pid:
        return (_no, _no, _no, _no, _no, _no,
                html.Div("Please select a product to offer.", style={"color": COLORS["text_muted"], "fontSize": "12px"}))

    bump_pid = int(bump_pid)
    bump_name = _resolve_product_name(bump_pid)

    store = {"bump_pid": bump_pid, "bump_name": bump_name, "trigger_mode": trigger_mode,
             "page_url": page_url or None}
    trigger_product_name = None
    trigger_category_name = None

    if trigger_mode == "products" and trigger_pids:
        trigger_pids = [int(p) for p in trigger_pids]
        store["trigger_pids"] = trigger_pids
        trigger_product_name = _resolve_product_name(trigger_pids[0])
        if len(trigger_pids) > 1:
            trigger_product_name += f" (+{len(trigger_pids) - 1} more)"
        store["trigger_name"] = trigger_product_name
    elif trigger_mode == "category" and trigger_cat_id:
        trigger_cat_id = int(trigger_cat_id)
        store["trigger_cat_id"] = trigger_cat_id
        wc_cats = _get_wc_categories_cached()
        trigger_category_name = next((c["name"] for c in wc_cats if c["id"] == trigger_cat_id), f"Cat #{trigger_cat_id}")
        store["trigger_cat_name"] = trigger_category_name
    else:
        store["trigger_mode"] = "none"

    copy = ob_api.generate_bump_copy(bump_name, trigger_product_name, trigger_category_name,
                                     page_url=page_url or None)
    panel_style = {"display": "block"}
    msg = html.Div(
        "AI copy generated â€” review and edit below, then confirm.",
        style={"color": COLORS["accent"], "fontSize": "12px", "fontWeight": "600"},
    )
    return panel_style, store, copy["title"], copy["headline"], copy["description"], page_url or "", msg


# â”€â”€ Regenerate button â”€â”€

@callback(
    Output("ob-preview-title", "value", allow_duplicate=True),
    Output("ob-preview-headline", "value", allow_duplicate=True),
    Output("ob-preview-description", "value", allow_duplicate=True),
    Input("ob-regenerate-btn", "n_clicks"),
    State("ob-preview-store", "data"),
    State("ob-preview-url-input", "value"),
    prevent_initial_call=True,
)
def handle_regenerate(n_clicks, store, preview_url):
    if not n_clicks or not store:
        return no_update, no_update, no_update
    # URL from the preview panel field takes priority over the stored one
    page_url = (preview_url or "").strip() or store.get("page_url") or None
    copy = ob_api.generate_bump_copy(
        store.get("bump_name", ""),
        store.get("trigger_name"),
        store.get("trigger_cat_name"),
        page_url=page_url,
    )
    return copy["title"], copy["headline"], copy["description"]


# â”€â”€ Cancel preview â”€â”€

@callback(
    Output("ob-preview-panel", "style", allow_duplicate=True),
    Output("ob-confirm-result", "children", allow_duplicate=True),
    Input("ob-cancel-preview-btn", "n_clicks"),
    prevent_initial_call=True,
)
def handle_cancel_preview(n_clicks):
    if not n_clicks:
        return no_update, no_update
    return {"display": "none"}, ""


# â”€â”€ Confirm & Create bump â”€â”€

@callback(
    Output("ob-confirm-result", "children"),
    Output("ob-preview-panel", "style", allow_duplicate=True),
    Input("ob-confirm-create-btn", "n_clicks"),
    State("ob-preview-store", "data"),
    State("ob-preview-title", "value"),
    State("ob-preview-headline", "value"),
    State("ob-preview-description", "value"),
    State("ob-preview-design-style", "value"),
    prevent_initial_call=True,
)
def handle_confirm_create(n_clicks, store, title, headline, description, design_style):
    """Actually create the bump with user-edited copy."""
    if not n_clicks or not store:
        return no_update, no_update

    bump_pid = store.get("bump_pid")
    if not bump_pid:
        return "Missing product data.", no_update

    payload = {
        "title": title or f"Bump: {store.get('bump_name', '')}",
        "bump_product_id": int(bump_pid),
        "headline": headline or "Don't miss this!",
        "description": description or "",
        "discount_type": "none",
        "discount_value": 0,
        "position": "after_order_review",
        "design_style": design_style or "classic",
        "priority": 10,
        "status": "publish",
    }

    mode = store.get("trigger_mode", "none")
    if mode == "products":
        pids = store.get("trigger_pids") or []
        if pids:
            payload["trigger_product_ids"] = [int(p) for p in pids]
    elif mode == "category":
        cat_id = store.get("trigger_cat_id")
        if cat_id:
            payload["trigger_category_ids"] = [int(cat_id)]

    result = ob_api.create_bump(payload)
    _lazy_cache.pop("ob_bumps", None)

    if result and result.get("id"):
        bump_name = store.get("bump_name", "")
        if mode == "products":
            n_triggers = len(store.get("trigger_pids", []))
            trigger_info = f" (shows when {n_triggers} product{'s are' if n_triggers != 1 else ' is'} in cart)"
        elif mode == "category":
            trigger_info = f" (shows for category '{store.get('trigger_cat_name', '')}')"
        else:
            trigger_info = " (shows to everyone)"
        msg = html.Div(
            f"Offer #{result['id']} created for '{bump_name}'{trigger_info}. Click 'Refresh' to update.",
            style={
                "color": COLORS["accent3"], "fontSize": "13px", "padding": "10px 16px",
                "background": "rgba(90,170,136,0.10)", "borderRadius": "8px",
                "border": f"1px solid {COLORS['accent3']}",
            },
        )
        return msg, {"display": "none"}

    return html.Div(
        "Failed to create offer. Check server logs.",
        style={
            "color": COLORS["red"], "fontSize": "13px", "padding": "10px 16px",
            "background": "rgba(212,74,74,0.10)", "borderRadius": "8px",
            "border": f"1px solid {COLORS['red']}",
        },
    ), no_update


# â”€â”€ Delete bump â”€â”€

@callback(
    Output("ob-status-msg", "children"),
    Input({"type": "ob-delete-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_delete_bump(n_clicks_list):
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update
    btn_id = ctx.triggered_id
    if not isinstance(btn_id, dict):
        return no_update
    bump_id = btn_id.get("index")
    if not bump_id:
        return no_update
    try:
        bump_id = int(bump_id)
    except (ValueError, TypeError):
        return no_update

    success = ob_api.delete_bump(bump_id)
    _lazy_cache.pop("ob_bumps", None)

    if success:
        return html.Div(
            f"Offer #{bump_id} deleted. Click 'Refresh' to update the list.",
            style={
                "color": COLORS["accent3"], "fontSize": "13px", "padding": "10px 16px",
                "background": "rgba(90,170,136,0.10)", "borderRadius": "8px",
                "marginBottom": "8px",
            },
        )
    return html.Div(
        f"Failed to delete offer #{bump_id}.",
        style={
            "color": COLORS["red"], "fontSize": "13px", "padding": "10px 16px",
            "background": "rgba(212,74,74,0.10)", "borderRadius": "8px",
            "marginBottom": "8px",
        },
    )


# â”€â”€ Populate product & category dropdowns â”€â”€

@callback(
    Output("ob-manual-bump-product", "options"),
    Output("ob-manual-trigger-product", "options"),
    Output("ob-manual-trigger-category", "options"),
    Output("ob-autofill-product", "options"),
    Input("url", "pathname"),
)
def populate_manual_bump_dropdowns(pathname):
    if pathname != "/cross-sell":
        return no_update, no_update, no_update, no_update
    future_pids = _get_future_event_and_course_pids()
    all_products = hist_df.groupby(
        ["product_id", "product_name"]
    ).size().reset_index(name="_cnt").sort_values("product_name")
    all_opts = [
        {"label": f"{row['product_name']}  (#{int(row['product_id'])})", "value": int(row["product_id"])}
        for _, row in all_products.iterrows()
    ]
    future_products = all_products[all_products["product_id"].isin(future_pids)] if future_pids else all_products
    prod_opts = [
        {"label": f"{row['product_name']}  (#{int(row['product_id'])})", "value": int(row["product_id"])}
        for _, row in future_products.iterrows()
    ]
    wc_cats = _get_wc_categories_cached()
    _skip_cats = {"uncategorized", "test"}
    cat_opts = [
        {"label": c["name"], "value": c["id"]}
        for c in wc_cats if c["name"].lower() not in _skip_cats
    ]
    course_pids = _get_course_pids()
    course_products = hist_df[hist_df["product_id"].isin(course_pids)].groupby(
        ["product_id", "product_name"]
    ).size().reset_index(name="_cnt").sort_values("product_name")
    autofill_opts = [
        {"label": f"{row['product_name']}  (#{int(row['product_id'])})", "value": int(row["product_id"])}
        for _, row in course_products.iterrows()
    ]
    if not autofill_opts:
        autofill_opts = prod_opts
    return all_opts, all_opts, cat_opts, autofill_opts


# â”€â”€ Toggle trigger mode visibility â”€â”€

@callback(
    Output("ob-trigger-products-row", "style"),
    Output("ob-trigger-category-row", "style"),
    Input("ob-trigger-mode", "value"),
)
def toggle_trigger_mode(mode):
    if mode == "products":
        return {"display": "block"}, {"display": "none"}
    elif mode == "category":
        return {"display": "none"}, {"display": "block"}
    return {"display": "none"}, {"display": "none"}


# â”€â”€ Render uncovered products checklist â”€â”€

@callback(
    Output("ob-uncovered-checklist", "options"),
    Output("ob-uncovered-checklist", "value"),
    Output("ob-uncovered-header", "children"),
    Input("url", "pathname"),
    Input("ob-refresh-btn", "n_clicks"),
)
def render_uncovered_products(pathname, _refresh):
    if pathname != "/cross-sell":
        return no_update, no_update, no_update
    if not ob_api.is_configured():
        return [], [], ""

    existing = _get_existing_bumps()
    uncovered = _compute_uncovered_pids(existing)

    if not uncovered:
        return [], [], html.P(
            "All future events and courses already have a checkout offer.",
            style={"color": COLORS["accent3"], "fontSize": "13px"},
        )

    pid_cat = hist_df.groupby("product_id")["category"].first().to_dict()
    options = []
    sorted_pids = sorted(uncovered)
    for i, pid in enumerate(sorted_pids, 1):
        name = _resolve_product_name(pid)
        raw_cat = pid_cat.get(pid, "")
        all_cats = set(parse_categories(raw_cat))
        type_cats = all_cats & GENERIC_CATS
        event_cats = all_cats - GENERIC_CATS
        event_label = ", ".join(sorted(event_cats)) if event_cats else raw_cat
        if "ONLINE COURSE" in type_cats:
            type_label, type_color = "Course", COLORS["accent3"]
            type_bg = "rgba(90,170,136,0.15)"
        elif "LIVESTREAM" in type_cats:
            type_label, type_color = "Livestream", COLORS["accent2"]
            type_bg = "rgba(224,184,74,0.15)"
        else:
            type_label, type_color = "Event", COLORS["accent"]
            type_bg = "rgba(200,164,78,0.15)"

        label = html.Div(
            style={"display": "grid", "gridTemplateColumns": "36px 1fr 1fr 90px",
                   "gap": "10px", "width": "100%", "alignItems": "center"},
            children=[
                html.Span(str(i), style={"color": COLORS["text_muted"], "fontSize": "12px"}),
                html.Span(
                    [name, html.Span(f"  #{pid}", style={"color": COLORS["text_muted"], "fontSize": "11px"})],
                    style={"fontWeight": "600", "fontSize": "13px"},
                ),
                html.Span(event_label, style={"color": COLORS["text_muted"], "fontSize": "12px"}),
                html.Span(type_label, style={
                    "fontSize": "10px", "fontWeight": "600", "textTransform": "uppercase",
                    "padding": "2px 8px", "borderRadius": "4px",
                    "backgroundColor": type_bg, "color": type_color, "textAlign": "center",
                }),
            ],
        )
        options.append({"label": label, "value": pid})

    header = html.P(
        f"{len(uncovered)} product{'s' if len(uncovered) != 1 else ''} without any checkout offer:",
        style={"fontSize": "13px", "color": COLORS["red"], "fontWeight": "600", "marginBottom": "8px"},
    )
    return options, sorted_pids, header


# â”€â”€ Select All / Deselect All â”€â”€

@callback(
    Output("ob-uncovered-checklist", "value", allow_duplicate=True),
    Input("ob-select-all-btn", "n_clicks"),
    State("ob-uncovered-checklist", "options"),
    prevent_initial_call=True,
)
def select_all_uncovered(_n, options):
    return [o["value"] for o in (options or [])]


@callback(
    Output("ob-uncovered-checklist", "value", allow_duplicate=True),
    Input("ob-deselect-all-btn", "n_clicks"),
    prevent_initial_call=True,
)
def deselect_all_uncovered(_n):
    return []


@callback(
    Output("ob-selected-count", "children"),
    Input("ob-uncovered-checklist", "value"),
    Input("ob-uncovered-checklist", "options"),
)
def update_selected_count(value, options):
    total = len(options or [])
    sel = len(value or [])
    if total == 0:
        return ""
    return f"{sel} of {total} selected"


# â”€â”€ Auto-fill selected products â”€â”€

@callback(
    Output("ob-autofill-result", "children"),
    Input("ob-autofill-btn", "n_clicks"),
    State("ob-autofill-product", "value"),
    State("ob-autofill-random", "value"),
    State("ob-autofill-design-style", "value"),
    State("ob-uncovered-checklist", "value"),
    State("ob-page-url-input", "value"),
    prevent_initial_call=True,
)
def handle_autofill(n_clicks, selected_product, random_opts, design_style, checked_pids, page_url):
    if not n_clicks:
        return no_update

    use_random = "random" in (random_opts or [])

    if not use_random and not selected_product:
        return html.Div("Please select a product to offer, or enable 'Pick a random course'.",
                         style={"color": COLORS["text_muted"], "fontSize": "12px"})

    target_pids = checked_pids or []
    if not target_pids:
        return html.Div("No products selected. Use the checkboxes to choose which products to auto-fill.",
                         style={"color": COLORS["text_muted"], "fontSize": "12px"})

    course_pids_list = sorted(_get_course_pids())
    if use_random and not course_pids_list:
        return html.Div("No online courses found for random selection.",
                         style={"color": COLORS["red"], "fontSize": "12px"})

    _lazy_cache.pop("ob_bumps", None)
    import random as _rng
    created = 0
    errors = 0

    for i, trigger_pid in enumerate(sorted(target_pids)):
        if use_random:
            bump_pid = course_pids_list[i % len(course_pids_list)]
        else:
            bump_pid = int(selected_product)

        if bump_pid == trigger_pid:
            continue

        bump_name = _resolve_product_name(bump_pid)
        trigger_name = _resolve_product_name(trigger_pid)

        copy = ob_api.generate_bump_copy(bump_name, trigger_name, page_url=page_url or None)

        payload = {
            "title": copy["title"],
            "bump_product_id": bump_pid,
            "headline": copy["headline"],
            "description": copy["description"],
            "discount_type": "none",
            "discount_value": 0,
            "position": "after_order_review",
            "design_style": design_style or "classic",
            "priority": 10,
            "status": "publish",
            "trigger_product_ids": [trigger_pid],
        }
        result = ob_api.create_bump(payload)
        if result and result.get("id"):
            created += 1
        else:
            errors += 1

    _lazy_cache.pop("ob_bumps", None)

    parts = [f"{created} offer{'s' if created != 1 else ''} created"]
    if errors:
        parts.append(f"{errors} failed")
    parts.append("Click 'Refresh' to update.")

    return html.Div(
        " â€” ".join(parts),
        style={
            "color": COLORS["accent3"] if created else COLORS["red"],
            "fontSize": "13px", "padding": "10px 16px",
            "background": "rgba(90,170,136,0.10)" if created else "rgba(212,74,74,0.10)",
            "borderRadius": "8px",
        },
    )


# â”€â”€ Delete all offers â”€â”€

@callback(
    Output("ob-delete-all-result", "children"),
    Input("ob-delete-all-btn", "n_clicks"),
    prevent_initial_call=True,
)
def handle_delete_all_bumps(n_clicks):
    if not n_clicks:
        return no_update

    existing = _get_existing_bumps()
    if not existing:
        return html.Div("No offers to delete.", style={"color": COLORS["text_muted"], "fontSize": "12px"})

    deleted = 0
    errors = 0
    for b in existing:
        if ob_api.delete_bump(b["id"]):
            deleted += 1
        else:
            errors += 1

    _lazy_cache.pop("ob_bumps", None)

    parts = [f"{deleted} offer{'s' if deleted != 1 else ''} deleted"]
    if errors:
        parts.append(f"{errors} failed")
    parts.append("Click 'Refresh' to update.")

    return html.Div(
        " â€” ".join(parts),
        style={
            "color": COLORS["accent3"] if deleted else COLORS["red"],
            "fontSize": "13px", "padding": "10px 16px",
            "background": "rgba(90,170,136,0.10)" if deleted else "rgba(212,74,74,0.10)",
            "borderRadius": "8px",
        },
    )


# â”€â”€ Order Bump Analytics â”€â”€

@callback(
    Output("ob-analytics-kpis", "children"),
    Output("ob-analytics-table", "children"),
    Output("ob-analytics-chart", "figure"),
    Input("url", "pathname"),
    Input("ob-refresh-btn", "n_clicks"),
)
def render_ob_analytics(pathname, _refresh):
    empty_fig = go.Figure()
    empty_fig.update_layout(**PLOT_LAYOUT, height=300, title=None)

    if pathname != "/cross-sell":
        return no_update, no_update, no_update
    if not ob_api.is_configured():
        return "", "", empty_fig

    summary = ob_api.analytics_summary()
    by_bump = ob_api.analytics_by_bump()
    daily = ob_api.analytics_daily()

    def _mini_kpi(title, value, subtitle, color=COLORS["accent"]):
        return html.Div(style={
            "flex": "1", "minWidth": "140px", "textAlign": "center",
            "padding": "14px 12px", "borderRadius": "10px",
            "backgroundColor": COLORS["card"],
            "border": f"1px solid {COLORS['card_border']}",
            "borderTop": f"3px solid {color}",
        }, children=[
            html.P(title, style={
                "color": COLORS["text_muted"], "fontSize": "10px", "marginBottom": "2px",
                "textTransform": "uppercase", "letterSpacing": "1.5px", "fontWeight": "600",
            }),
            html.H3(str(value), style={
                "color": color, "margin": "6px 0 0", "fontSize": "22px", "fontWeight": "700",
            }),
            html.P(subtitle, style={
                "color": COLORS["text_muted"], "fontSize": "10px", "margin": "4px 0 0",
                "fontStyle": "italic", "lineHeight": "1.3",
            }),
        ])

    impressions = summary.get("impressions", 0)
    conversions = summary.get("conversions", 0)
    conv_rate = summary.get("conversion_rate", 0)
    total_rev = summary.get("total_revenue", 0)
    avg_val = summary.get("avg_order_value", 0)

    kpis = [
        _mini_kpi("Times Shown", f"{impressions:,}", "How many times offers appeared at checkout", COLORS["accent2"]),
        _mini_kpi("Times Accepted", f"{conversions:,}", "How many times customers said 'yes'", COLORS["accent3"]),
        _mini_kpi("Accept Rate", f"{conv_rate}%", "% of shown offers that were accepted", COLORS["accent"]),
        _mini_kpi("Extra Revenue", f"${total_rev:,.2f}", "Total revenue from accepted offers", COLORS["accent3"]),
        _mini_kpi("Avg per Offer", f"${avg_val:,.2f}", "Average revenue per accepted offer", COLORS["accent4"]),
    ]

    if by_bump:
        bump_header = html.Tr([
            html.Th("Offer Name", style=_th_style()),
            html.Th("Product Offered", style=_th_style()),
            html.Th("Times Shown", style=_th_style({"textAlign": "right"})),
            html.Th("Times Accepted", style=_th_style({"textAlign": "right"})),
            html.Th("Accept Rate", style=_th_style({"textAlign": "right"})),
            html.Th("Revenue", style=_th_style({"textAlign": "right"})),
        ])
        bump_rows = []
        for b in by_bump:
            bump_info = b.get("bump", {})
            bump_rows.append(html.Tr([
                html.Td(str(bump_info.get("title", "")), style=_td_style({"fontWeight": "600"})),
                html.Td(
                    _resolve_product_name(bump_info.get("bump_product_id", 0)),
                    style=_td_style({"color": COLORS["text_muted"], "fontSize": "12px"}),
                ),
                html.Td(f"{b.get('impressions', 0):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{b.get('conversions', 0):,}", style=_td_style({"textAlign": "right"})),
                html.Td(f"{b.get('conversion_rate', 0)}%", style=_td_style({"textAlign": "right"})),
                html.Td(f"${b.get('total_revenue', 0):,.2f}", style=_td_style({"textAlign": "right"})),
            ]))
        bump_table = html.Table(
            [html.Thead(bump_header), html.Tbody(bump_rows)],
            style={"width": "100%", "borderCollapse": "collapse"},
        )
    else:
        bump_table = html.P(
            "No performance data yet. Numbers will appear here once customers start seeing your offers at checkout.",
            style={"color": COLORS["text_muted"], "fontSize": "13px", "fontStyle": "italic"},
        )

    fig = go.Figure()
    if daily:
        dates = [d["date"] for d in daily]
        fig.add_trace(go.Bar(
            x=dates, y=[d["impressions"] for d in daily],
            name="Times Shown",
            marker_color=COLORS["accent2"], opacity=0.7,
        ))
        fig.add_trace(go.Bar(
            x=dates, y=[d["conversions"] for d in daily],
            name="Times Accepted",
            marker_color=COLORS["accent3"], opacity=0.9,
        ))
        fig.add_trace(go.Scatter(
            x=dates, y=[d["revenue"] for d in daily],
            name="Extra Revenue ($)", yaxis="y2",
            line=dict(color=COLORS["accent"], width=2),
            mode="lines+markers", marker=dict(size=4),
        ))
    layout_overrides = {k: v for k, v in PLOT_LAYOUT.items() if k not in ("yaxis",)}
    fig.update_layout(
        **layout_overrides,
        height=300,
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(gridcolor=COLORS["grid"], showline=False, zeroline=False, rangemode="tozero", title=None),
        yaxis2=dict(
            overlaying="y", side="right", showgrid=False,
            zeroline=False, rangemode="tozero",
            tickprefix="$", title=None,
            gridcolor=COLORS["grid"],
            tickfont=dict(color=COLORS["accent"]),
        ),
    )

    return kpis, bump_table, fig


