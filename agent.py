"""
Sales AI Agent powered by OpenAI ChatGPT.
Answers questions about sales, products, and forecasts.
Can generate custom reports on demand.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# API key (support both naming conventions)
API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("OpenAI_API_KEY")


def get_client():
    if not OPENAI_AVAILABLE:
        raise RuntimeError("openai package not installed. Run: pip install openai")
    if not API_KEY:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY in .env")
    return OpenAI(api_key=API_KEY)


# ------------------------------------------------------------------
# DATA CONTEXT BUILDER
# ------------------------------------------------------------------

def build_data_summary(hist_df, pred_df, metrics_df):
    """Build a concise text summary of the sales data for the AI context."""
    lines = []
    today = pd.Timestamp.now().normalize()

    # --- General overview ---
    n_products = hist_df["product_id"].nunique()
    total_sales = int(hist_df["quantity_sold"].sum())
    total_revenue = hist_df["revenue"].sum()
    date_min = hist_df["order_date"].min()
    date_max = hist_df["order_date"].max()

    lines.append("=== SALES DATA OVERVIEW ===")
    lines.append(f"Date range: {date_min.strftime('%Y-%m-%d')} to {date_max.strftime('%Y-%m-%d')}")
    lines.append(f"Total products: {n_products}")
    lines.append(f"Total units sold: {total_sales:,}")
    lines.append(f"Total revenue: ${total_revenue:,.2f}")
    lines.append(f"Today: {today.strftime('%Y-%m-%d')}")
    lines.append("")

    # --- Top 20 products by sales ---
    top_products = (
        hist_df.groupby(["product_id", "product_name"])
        .agg(
            total_qty=("quantity_sold", "sum"),
            total_rev=("revenue", "sum"),
            first_sale=("order_date", "min"),
            last_sale=("order_date", "max"),
        )
        .reset_index()
        .sort_values("total_qty", ascending=False)
        .head(20)
    )

    lines.append("=== TOP 20 PRODUCTS BY QUANTITY SOLD ===")
    for _, r in top_products.iterrows():
        lines.append(
            f"  #{int(r['product_id'])} {r['product_name']}: "
            f"{int(r['total_qty'])} units, ${r['total_rev']:,.2f} revenue, "
            f"sales {r['first_sale'].strftime('%Y-%m-%d')} to {r['last_sale'].strftime('%Y-%m-%d')}"
        )
    lines.append("")

    # --- Categories ---
    if "category" in hist_df.columns:
        cat_sales = {}
        for _, row in hist_df.drop_duplicates("product_id").iterrows():
            pid = row["product_id"]
            cats = [c.strip() for c in str(row.get("category", "")).split("|") if c.strip()]
            pid_data = hist_df[hist_df["product_id"] == pid]
            qty = pid_data["quantity_sold"].sum()
            rev = pid_data["revenue"].sum()
            for c in cats:
                if c not in cat_sales:
                    cat_sales[c] = {"qty": 0, "rev": 0.0}
                cat_sales[c]["qty"] += qty
                cat_sales[c]["rev"] += rev

        lines.append("=== SALES BY CATEGORY ===")
        for cat, data in sorted(cat_sales.items(), key=lambda x: x[1]["qty"], reverse=True)[:15]:
            lines.append(f"  {cat}: {int(data['qty'])} units, ${data['rev']:,.2f}")
        lines.append("")

    # --- Monthly trends (last 6 months) ---
    recent = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=180)]
    if not recent.empty:
        monthly = recent.groupby(recent["order_date"].dt.to_period("M")).agg(
            qty=("quantity_sold", "sum"),
            rev=("revenue", "sum"),
        )
        lines.append("=== MONTHLY SALES (LAST 6 MONTHS) ===")
        for period, r in monthly.iterrows():
            lines.append(f"  {period}: {int(r['qty'])} units, ${r['rev']:,.2f}")
        lines.append("")

    # --- Last 14 days (daily totals) ---
    last_14d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=14)]
    if not last_14d.empty:
        daily = last_14d.groupby("order_date").agg(
            qty=("quantity_sold", "sum"),
            rev=("revenue", "sum"),
        ).sort_index()
        lines.append("=== DAILY SALES TOTALS (LAST 14 DAYS) ===")
        for date, r in daily.iterrows():
            lines.append(f"  {date.strftime('%Y-%m-%d')}: {int(r['qty'])} units, ${r['rev']:,.2f}")
        lines.append("")

    # --- Last 7 days (product-level detail per day) ---
    last_7d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=7)]
    if not last_7d.empty:
        lines.append("=== PRODUCT-LEVEL SALES PER DAY (LAST 7 DAYS) ===")
        for date in sorted(last_7d["order_date"].unique()):
            day_data = last_7d[last_7d["order_date"] == date]
            day_by_product = (
                day_data.groupby(["product_id", "product_name"])
                .agg(qty=("quantity_sold", "sum"), rev=("revenue", "sum"))
                .reset_index()
                .sort_values("qty", ascending=False)
            )
            date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
            day_total_qty = int(day_by_product["qty"].sum())
            day_total_rev = day_by_product["rev"].sum()
            lines.append(f"  --- {date_str} (total: {day_total_qty} units, ${day_total_rev:,.2f}) ---")
            for _, r in day_by_product.iterrows():
                lines.append(
                    f"    #{int(r['product_id'])} {r['product_name']}: "
                    f"{int(r['qty'])} units, ${r['rev']:,.2f}"
                )
        lines.append("")

    # --- Last 30 days aggregated per product ---
    last_30d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=30)]
    if not last_30d.empty:
        monthly_products = (
            last_30d.groupby(["product_id", "product_name"])
            .agg(qty=("quantity_sold", "sum"), rev=("revenue", "sum"),
                 days_with_sales=("order_date", "nunique"))
            .reset_index()
            .sort_values("qty", ascending=False)
            .head(25)
        )
        lines.append("=== TOP 25 PRODUCTS - LAST 30 DAYS ===")
        for _, r in monthly_products.iterrows():
            lines.append(
                f"  #{int(r['product_id'])} {r['product_name']}: "
                f"{int(r['qty'])} units, ${r['rev']:,.2f}, "
                f"active on {int(r['days_with_sales'])} days"
            )
        lines.append("")

    # --- Forecasts: daily predictions per product (next 7 days) ---
    if not pred_df.empty:
        # Overall 30-day totals
        total_pred_all = pred_df["predicted_quantity"].sum()
        pred_30d_summary = (
            pred_df.groupby(["product_id", "product_name"])
            .agg(total_pred=("predicted_quantity", "sum"), avg_pred=("predicted_quantity", "mean"))
            .reset_index()
            .sort_values("total_pred", ascending=False)
        )
        lines.append(f"=== 30-DAY FORECAST TOTALS (total: {total_pred_all:.0f} units) ===")
        for _, r in pred_30d_summary.head(20).iterrows():
            lines.append(
                f"  #{int(r['product_id'])} {r['product_name']}: "
                f"{r['total_pred']:.1f} units total (avg {r['avg_pred']:.2f}/day)"
            )
        lines.append("")

        # Detailed daily forecast per product for the next 7 days
        next_7d = pred_df[pred_df["order_date"] <= today + pd.Timedelta(days=7)]
        if not next_7d.empty:
            lines.append("=== DAILY FORECAST PER PRODUCT (NEXT 7 DAYS) ===")
            lines.append("  These are the actual model predictions. Use ONLY these numbers when asked about forecasts.")
            lines.append("")
            for date in sorted(next_7d["order_date"].unique()):
                day_pred = next_7d[next_7d["order_date"] == date]
                day_by_product = (
                    day_pred.groupby(["product_id", "product_name"])["predicted_quantity"]
                    .sum().reset_index()
                    .sort_values("predicted_quantity", ascending=False)
                )
                date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
                day_total = day_by_product["predicted_quantity"].sum()
                lines.append(f"  --- {date_str} (total forecast: {day_total:.1f} units) ---")
                for _, r in day_by_product.iterrows():
                    if r["predicted_quantity"] > 0.05:
                        lines.append(
                            f"    #{int(r['product_id'])} {r['product_name']}: "
                            f"{r['predicted_quantity']:.1f} units"
                        )
            lines.append("")

    # --- Model metrics ---
    if not metrics_df.empty and "r2_score" in metrics_df.columns:
        avg_r2 = metrics_df["r2_score"].mean()
        avg_mae = metrics_df["mae"].mean() if "mae" in metrics_df.columns else None
        lines.append("=== MODEL PERFORMANCE ===")
        lines.append(f"  Average R² score: {avg_r2:.3f}")
        if avg_mae is not None:
            lines.append(f"  Average MAE: {avg_mae:.3f}")
        if len(metrics_df) > 3:
            best = metrics_df.nlargest(3, "r2_score")
            worst = metrics_df.nsmallest(3, "r2_score")
            lines.append("  Best predicted products:")
            for _, r in best.iterrows():
                lines.append(f"    {r['product_name']}: R²={r['r2_score']:.3f}")
            lines.append("  Hardest to predict:")
            for _, r in worst.iterrows():
                lines.append(f"    {r['product_name']}: R²={r['r2_score']:.3f}")
        lines.append("")

    return "\n".join(lines)


# ------------------------------------------------------------------
# SYSTEM PROMPT
# ------------------------------------------------------------------

SYSTEM_PROMPT = """You are a Sales Intelligence AI Assistant for an event/ticketing company.
You have access to detailed sales data, forecasts, and model metrics.

Your capabilities:
1. Answer questions about sales performance, trends, and patterns
2. Compare products and categories
3. Provide insights about forecasts and predictions
4. Generate structured reports (daily, weekly, monthly, custom)
5. Identify anomalies and opportunities

Rules:
- Always base your answers on the provided data
- Use specific numbers and dates when available
- Format currency as USD ($)
- When generating reports, use clear markdown headers, bullet points, and tables
- If you don't have enough data to answer, say so clearly
- Use markdown formatting for better readability
- Be concise but thorough
- When showing tables, use markdown table format
- Respond in the same language the user writes in
- CRITICAL: You have detailed product-level sales for each of the last 7 days.
  When the user asks "what products were sold today/yesterday/on date X", look at
  the PRODUCT-LEVEL SALES PER DAY section and list the specific products, quantities,
  and revenue. Never say "N/A" if the data is available in the context.
- CRITICAL: You have the EXACT daily forecasts per product for the next 7 days in
  the DAILY FORECAST PER PRODUCT section. When asked about predictions/forecasts for
  tomorrow or any specific day, use ONLY these exact numbers. NEVER calculate, estimate,
  or make up forecast values. If the data is there, quote it directly.
- NEVER hallucinate or invent numbers. If you don't have data for something, say so.
  Do not approximate or guess. Only use the exact values provided in the data context.

The data below represents the current state of the sales database:

{data_summary}"""


# ------------------------------------------------------------------
# CHAT FUNCTION
# ------------------------------------------------------------------

def chat(question, hist_df, pred_df, metrics_df, chat_history=None):
    """
    Send a question to ChatGPT with sales data context.

    Parameters
    ----------
    question : str
    hist_df, pred_df, metrics_df : pd.DataFrame
    chat_history : list[dict] | None
        Previous messages as [{"role": "user"/"assistant", "content": "..."}]

    Returns
    -------
    str : The AI response text.
    """
    client = get_client()

    data_summary = build_data_summary(hist_df, pred_df, metrics_df)
    system_msg = SYSTEM_PROMPT.format(data_summary=data_summary)

    messages = [{"role": "system", "content": system_msg}]

    # Keep last 10 exchanges from history to stay within token limits
    if chat_history:
        for msg in chat_history[-20:]:
            messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": question})

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
        max_tokens=3000,
    )

    return response.choices[0].message.content


# ------------------------------------------------------------------
# QUICK ACTIONS (predefined prompts)
# ------------------------------------------------------------------

QUICK_ACTIONS = {
    "daily_report": (
        "Generate a detailed daily sales report for today. Include:\n"
        "- Summary of today's sales (if any) and comparison with yesterday\n"
        "- Top selling products today\n"
        "- Revenue breakdown\n"
        "- Forecast for tomorrow\n"
        "- Any notable trends or anomalies\n"
        "Format as a professional report with headers and tables."
    ),
    "weekly_summary": (
        "Generate a weekly summary report for the last 7 days. Include:\n"
        "- Total sales and revenue with daily breakdown table\n"
        "- Top 10 products of the week\n"
        "- Week-over-week comparison\n"
        "- Category performance\n"
        "- Key insights and recommendations\n"
        "Format as a professional report."
    ),
    "top_products": (
        "Analyze the top performing products. Include:\n"
        "- Top 15 products by sales volume\n"
        "- Top 10 by revenue\n"
        "- Products with the strongest recent momentum (last 30 days)\n"
        "- Comparison of actual sales vs forecasts where available\n"
        "Use tables and clear formatting."
    ),
    "forecast_analysis": (
        "Analyze the current sales forecasts in detail. Include:\n"
        "- Overall 30-day forecast summary\n"
        "- Top predicted products\n"
        "- Model reliability assessment (R², MAE)\n"
        "- Products where forecast differs significantly from recent trends\n"
        "- Recommendations for improving predictions\n"
        "Format as a professional analysis report."
    ),
}
