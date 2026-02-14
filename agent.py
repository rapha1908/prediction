"""
Sales AI Agent powered by OpenAI ChatGPT.
Answers questions about sales, products, and forecasts.
Can generate custom reports on demand.
"""

import os
import requests
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

# Display currency – all totals are converted to this currency
DISPLAY_CURRENCY = os.getenv("DISPLAY_CURRENCY", "USD").upper()

# Map of currency code -> symbol for display
CURRENCY_SYMBOLS = {
    "USD": "$", "BRL": "R$", "EUR": "€", "GBP": "£",
    "CAD": "C$", "AUD": "A$", "JPY": "¥", "CHF": "CHF",
    "MXN": "MX$", "ARS": "AR$", "CLP": "CL$", "COP": "CO$",
    "PEN": "S/.", "UYU": "$U", "PYG": "₲", "BOB": "Bs",
}


def _sym(code):
    """Return display symbol for a currency code."""
    if not code:
        return "$"
    return CURRENCY_SYMBOLS.get(str(code).upper(), str(code))


# ------------------------------------------------------------------
# EXCHANGE RATES
# ------------------------------------------------------------------

# Fallback rates to DISPLAY_CURRENCY=USD (approximate, updated manually)
_FALLBACK_RATES_TO_USD = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "BRL": 0.17,
    "CAD": 0.74, "AUD": 0.65, "JPY": 0.0067, "CHF": 1.12,
    "MXN": 0.058, "ARS": 0.001, "CLP": 0.001, "COP": 0.00024,
    "PEN": 0.27, "UYU": 0.024, "PYG": 0.00013, "BOB": 0.14,
}

_exchange_rates = {}  # cache: {(from, to): rate}


def fetch_exchange_rates(currencies, target=None):
    """
    Fetch live exchange rates from frankfurter.app (free, no API key).
    Returns dict {currency_code: rate_to_target}.
    Rate means: 1 unit of currency_code = rate units of target.
    """
    target = target or DISPLAY_CURRENCY
    rates = {target: 1.0}

    source_currencies = [c for c in currencies if c != target]
    if not source_currencies:
        return rates

    try:
        # frankfurter.app: GET /latest?from=TARGET&to=CUR1,CUR2,...
        # Returns how many units of CUR1/CUR2 you get per 1 TARGET
        # We need the inverse: how many TARGET per 1 CUR
        resp = requests.get(
            f"https://api.frankfurter.app/latest",
            params={"from": target, "to": ",".join(source_currencies)},
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            api_rates = data.get("rates", {})
            for cur, rate_from_target in api_rates.items():
                if rate_from_target > 0:
                    rates[cur] = 1.0 / rate_from_target  # invert: 1 CUR = ? TARGET
            print(f"  [OK] Exchange rates fetched ({target}): "
                  + ", ".join(f"1 {c}={rates[c]:.4f} {target}" for c in source_currencies if c in rates))
        else:
            raise ValueError(f"HTTP {resp.status_code}")
    except Exception as e:
        print(f"  [WARNING] Could not fetch exchange rates: {e}. Using fallback rates.")
        # Use fallback rates (convert via USD as pivot)
        for cur in source_currencies:
            usd_per_cur = _FALLBACK_RATES_TO_USD.get(cur, 1.0)
            usd_per_target = _FALLBACK_RATES_TO_USD.get(target, 1.0)
            if usd_per_target > 0:
                rates[cur] = usd_per_cur / usd_per_target
            else:
                rates[cur] = usd_per_cur

    return rates


def convert_revenue(df, rates, target=None, rev_col="revenue"):
    """
    Add 'revenue_converted' column to df by converting revenue using rates.
    Returns a copy of df with the new column.
    """
    target = target or DISPLAY_CURRENCY
    df = df.copy()
    if "currency" not in df.columns:
        df["revenue_converted"] = df[rev_col]
        return df

    def _convert(row):
        cur = row.get("currency", target)
        rate = rates.get(cur, 1.0)
        return row[rev_col] * rate

    df["revenue_converted"] = df.apply(_convert, axis=1)
    return df


def _format_converted_total(df, rates, rev_col="revenue"):
    """
    Format total revenue converted to DISPLAY_CURRENCY.
    Shows: '$347,401.00' if single currency, or '$347,401.00 (£50k + €20k + $277k)' if multi.
    """
    target = DISPLAY_CURRENCY
    sym_target = _sym(target)

    if df.empty:
        return f"{sym_target}0.00"

    has_currency = "currency" in df.columns
    if not has_currency:
        total = df[rev_col].sum()
        return f"{sym_target}{total:,.2f}"

    currencies = sorted(df["currency"].dropna().unique())
    multi = len(currencies) > 1

    # Convert and sum
    total_converted = 0.0
    breakdown_parts = []
    for cur in currencies:
        cur_total = df[df["currency"] == cur][rev_col].sum()
        if cur_total == 0:
            continue
        rate = rates.get(cur, 1.0)
        converted = cur_total * rate
        total_converted += converted
        if multi:
            breakdown_parts.append(f"{_sym(cur)}{cur_total:,.2f}")

    result = f"{sym_target}{total_converted:,.2f}"
    if multi and breakdown_parts:
        result += f"  ({' + '.join(breakdown_parts)})"
    return result


def get_client():
    if not OPENAI_AVAILABLE:
        raise RuntimeError("openai package not installed. Run: pip install openai")
    if not API_KEY:
        raise RuntimeError("OpenAI API key not found. Set OPENAI_API_KEY in .env")
    return OpenAI(api_key=API_KEY)


# ------------------------------------------------------------------
# DATA CONTEXT BUILDER
# ------------------------------------------------------------------

def build_data_summary(hist_df, pred_df, metrics_df, rates=None):
    """Build a concise text summary of the sales data for the AI context."""
    lines = []
    today = pd.Timestamp.now().normalize()

    # Fetch exchange rates if not provided
    has_currency = "currency" in hist_df.columns
    if rates is None and has_currency:
        currencies = list(hist_df["currency"].dropna().unique())
        rates = fetch_exchange_rates(currencies)
    elif rates is None:
        rates = {DISPLAY_CURRENCY: 1.0}

    # --- General overview ---
    n_products = hist_df["product_id"].nunique()
    total_sales = int(hist_df["quantity_sold"].sum())
    date_min = hist_df["order_date"].min()
    date_max = hist_df["order_date"].max()

    lines.append("=== SALES DATA OVERVIEW ===")
    lines.append(f"Date range: {date_min.strftime('%Y-%m-%d')} to {date_max.strftime('%Y-%m-%d')}")
    lines.append(f"Total products: {n_products}")
    lines.append(f"Total units sold: {total_sales:,}")
    lines.append(f"Total revenue: {_format_converted_total(hist_df, rates)}")
    lines.append(f"Display currency: {DISPLAY_CURRENCY} (all totals converted to {_sym(DISPLAY_CURRENCY)})")
    if has_currency:
        currencies_found = sorted(hist_df["currency"].dropna().unique())
        if len(currencies_found) > 1:
            lines.append(f"Currencies in data: {', '.join(currencies_found)}")
            rate_info = ", ".join(f"1 {c}={rates.get(c, 1.0):.4f} {DISPLAY_CURRENCY}" for c in currencies_found if c != DISPLAY_CURRENCY)
            if rate_info:
                lines.append(f"Exchange rates used: {rate_info}")
    lines.append(f"Today: {today.strftime('%Y-%m-%d')}")
    lines.append("")

    # --- Top 20 products by sales ---
    top_products = (
        hist_df.groupby(["product_id", "product_name"])
        .agg(
            total_qty=("quantity_sold", "sum"),
            first_sale=("order_date", "min"),
            last_sale=("order_date", "max"),
        )
        .reset_index()
        .sort_values("total_qty", ascending=False)
        .head(20)
    )

    lines.append("=== TOP 20 PRODUCTS BY QUANTITY SOLD ===")
    for _, r in top_products.iterrows():
        pid = int(r['product_id'])
        pid_data = hist_df[hist_df["product_id"] == pid]
        rev_str = _format_converted_total(pid_data, rates)
        lines.append(
            f"  #{pid} {r['product_name']}: "
            f"{int(r['total_qty'])} units, {rev_str} revenue, "
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
            for c in cats:
                if c not in cat_sales:
                    cat_sales[c] = {"qty": 0, "pids": set()}
                cat_sales[c]["qty"] += qty
                cat_sales[c]["pids"].add(pid)

        lines.append("=== SALES BY CATEGORY ===")
        for cat, data in sorted(cat_sales.items(), key=lambda x: x[1]["qty"], reverse=True)[:15]:
            cat_data = hist_df[hist_df["product_id"].isin(data["pids"])]
            rev_str = _format_converted_total(cat_data, rates)
            lines.append(f"  {cat}: {int(data['qty'])} units, {rev_str}")
        lines.append("")

    # --- Monthly trends (last 6 months) ---
    recent = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=180)]
    if not recent.empty:
        recent_copy = recent.copy()
        recent_copy["_period"] = recent_copy["order_date"].dt.to_period("M")
        lines.append("=== MONTHLY SALES (LAST 6 MONTHS) ===")
        for period in sorted(recent_copy["_period"].unique()):
            period_data = recent_copy[recent_copy["_period"] == period]
            qty = int(period_data["quantity_sold"].sum())
            rev_str = _format_converted_total(period_data, rates)
            lines.append(f"  {period}: {qty} units, {rev_str}")
        lines.append("")

    # --- Last 14 days (daily totals) ---
    last_14d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=14)]
    if not last_14d.empty:
        lines.append("=== DAILY SALES TOTALS (LAST 14 DAYS) ===")
        for date in sorted(last_14d["order_date"].unique()):
            day_data = last_14d[last_14d["order_date"] == date]
            qty = int(day_data["quantity_sold"].sum())
            rev_str = _format_converted_total(day_data, rates)
            lines.append(f"  {date.strftime('%Y-%m-%d')}: {qty} units, {rev_str}")
        lines.append("")

    # --- Last 7 days (product-level detail per day) ---
    last_7d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=7)]
    if not last_7d.empty:
        lines.append("=== PRODUCT-LEVEL SALES PER DAY (LAST 7 DAYS) ===")
        for date in sorted(last_7d["order_date"].unique()):
            day_data = last_7d[last_7d["order_date"] == date]
            date_str = pd.Timestamp(date).strftime('%Y-%m-%d')
            day_total_qty = int(day_data["quantity_sold"].sum())
            day_rev_str = _format_converted_total(day_data, rates)
            lines.append(f"  --- {date_str} (total: {day_total_qty} units, {day_rev_str}) ---")

            day_by_product = (
                day_data.groupby(["product_id", "product_name"])
                .agg(qty=("quantity_sold", "sum"))
                .reset_index()
                .sort_values("qty", ascending=False)
            )
            for _, r in day_by_product.iterrows():
                pid = int(r['product_id'])
                prod_day = day_data[day_data["product_id"] == pid]
                rev_str = _format_converted_total(prod_day, rates)
                lines.append(
                    f"    #{pid} {r['product_name']}: "
                    f"{int(r['qty'])} units, {rev_str}"
                )
        lines.append("")

    # --- Last 30 days aggregated per product ---
    last_30d = hist_df[hist_df["order_date"] >= today - pd.Timedelta(days=30)]
    if not last_30d.empty:
        monthly_products = (
            last_30d.groupby(["product_id", "product_name"])
            .agg(qty=("quantity_sold", "sum"),
                 days_with_sales=("order_date", "nunique"))
            .reset_index()
            .sort_values("qty", ascending=False)
            .head(25)
        )
        lines.append("=== TOP 25 PRODUCTS - LAST 30 DAYS ===")
        for _, r in monthly_products.iterrows():
            pid = int(r['product_id'])
            prod_30d = last_30d[last_30d["product_id"] == pid]
            rev_str = _format_converted_total(prod_30d, rates)
            lines.append(
                f"  #{pid} {r['product_name']}: "
                f"{int(r['qty'])} units, {rev_str}, "
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

    # --- Hourly sales pattern ---
    try:
        import db as _db_hourly
        _hourly = _db_hourly.load_hourly_sales()
        if not _hourly.empty and "hour" in _hourly.columns:
            hr_agg = _hourly.groupby("hour")["quantity_sold"].sum().sort_index()
            if not hr_agg.empty:
                lines.append("=== SALES BY HOUR OF DAY ===")
                for h, qty in hr_agg.items():
                    lines.append(f"  {int(h):02d}:00 - {int(qty)} units")
                top3 = hr_agg.nlargest(3)
                lines.append(f"  Best hours: {', '.join(f'{int(h):02d}:00' for h in top3.index)}")
                lines.append("")
    except Exception:
        pass  # hourly data not available

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
- Revenue data may come from MULTIPLE currencies. All totals in the data are already
  CONVERTED to the display currency using current exchange rates. When the original
  currencies differ, the breakdown is shown in parentheses. Use the converted totals
  for comparisons and summaries.
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
