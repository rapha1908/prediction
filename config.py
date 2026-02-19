"""
Shared configuration: colors, fonts, plot layout, and reusable UI helpers.
"""

import pandas as pd
from dash import html

# ── Color palette ──

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

GENERIC_CATS = frozenset({
    "Uncategorized", "Sem categoria",
    "EVENTS", "LIVESTREAM", "ONLINE COURSE",
    "THE BREATHWORK REVOLUTION",
})


# ── Reusable UI helpers ──

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
    """Small uppercase label above section titles."""
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


def _th_style(extra=None):
    base = {
        "textAlign": "left", "padding": "10px 14px", "fontSize": "11px",
        "color": COLORS["text_muted"], "textTransform": "uppercase",
        "letterSpacing": "1px", "fontWeight": "600",
        "borderBottom": f"1px solid {COLORS['card_border']}",
    }
    if extra:
        base.update(extra)
    return base


def _td_style(extra=None):
    base = {
        "padding": "10px 14px", "fontSize": "13px",
        "borderBottom": f"1px solid {COLORS['card_border']}",
        "verticalAlign": "middle",
    }
    if extra:
        base.update(extra)
    return base


# ── Category parsing helpers ──

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
    return (df.assign(category_list=df["category"].apply(parse_categories))
              .explode("category_list")
              .rename(columns={"category_list": "cat_single"}))
