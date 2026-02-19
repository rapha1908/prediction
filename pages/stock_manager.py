"""Stock Manager page – layout and callbacks."""

import pandas as pd
from dash import html, dcc, callback, Output, Input, State, no_update, ctx, ALL

from config import COLORS, FONT, card_style, section_label
from data_loader import _get_db


def layout():
    """Return stock-manager page layout (children of #stock-page)."""
    return [
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
    ]


# ── Callbacks ──

@callback(
    Output("stock-product-picker", "options"),
    Input("url", "pathname"),
    Input("stock-refresh", "data"),
)
def load_stock_picker_options(pathname, _refresh):
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
    if pathname != "/stock":
        return no_update
    try:
        import db as _db_mod
        df = _db_mod.load_stock_manager()
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
