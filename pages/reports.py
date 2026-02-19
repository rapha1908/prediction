"""Report generation callbacks."""
import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import html, dcc, callback, clientside_callback, Output, Input, State, no_update, ctx
import agent as ai_agent
from config import (COLORS, FONT, PLOT_LAYOUT, CATEGORY_COLORS, GENERIC_CATS, H_LEGEND,
                    card_style, section_label, kpi_card, _th_style, _td_style,
                    parse_categories, explode_categories, filter_by_categories,
                    build_product_cat_map)
from data_loader import (
    hist_df, pred_df, metrics_df, all_orders_df, event_status_map,
    product_cat_map, all_categories, product_sales,
    total_products, total_sales_qty, total_revenue,
    total_orders_days, date_min, date_max, pred_total_qty,
    exchange_rates, get_exchange_rates, convert_revenue,
    get_source_df, get_cross_sell_df, get_geo_sales_df,
    DISPLAY_CURRENCY, currency_symbol, _format_converted_total,
    ONLINE_COURSE_CATS, build_event_status_map,
    filter_by_event_tab, filter_by_currency,
)

# ============================================================

def _build_report_charts(selected_cats, tab_value, selected_currencies, product_id):
    """Build all report chart figures and statistical summary."""
    from datetime import datetime

    charts = []
    analysis_lines = []
    sym = currency_symbol(DISPLAY_CURRENCY)

    # --- Filter base data ---
    fh = filter_by_currency(filter_by_event_tab(hist_df, tab_value), selected_currencies)
    fp = filter_by_event_tab(pred_df, tab_value)
    fm = filter_by_event_tab(metrics_df, tab_value)

    if selected_cats:
        fh = filter_by_categories(fh, selected_cats, product_cat_map)
        fp = filter_by_categories(fp, selected_cats, product_cat_map)
        fm = filter_by_categories(fm, selected_cats, product_cat_map)

    if selected_currencies and "currency" not in fh.columns:
        pass
    elif selected_currencies:
        valid_pids = set(fh["product_id"].unique()) if not fh.empty else set()
        fp = fp[fp["product_id"].isin(valid_pids)]
        fm = fm[fm["product_id"].isin(valid_pids)]

    # --- KPI summary ---
    n_products = fh["product_id"].nunique() if not fh.empty else 0
    total_qty = int(fh["quantity_sold"].sum()) if not fh.empty else 0
    rev_col = "revenue_converted" if "revenue_converted" in fh.columns else "revenue"
    total_rev = fh[rev_col].sum() if not fh.empty else 0
    forecast_qty = fp["predicted_quantity"].sum() if not fp.empty else 0

    cats_label = ", ".join(selected_cats) if selected_cats else "All"
    tab_labels = {"active": "Active Events", "past": "Past Events", "course": "Online Courses", "map": "All"}
    tab_label = tab_labels.get(tab_value, "All")

    analysis_lines.append(f"Report generated on {datetime.now().strftime('%B %d, %Y at %H:%M')}")
    analysis_lines.append(f"Scope: {tab_label} | Categories: {cats_label}")
    analysis_lines.append(f"Currency filter: {', '.join(selected_currencies) if selected_currencies else 'All'}")
    analysis_lines.append("")
    analysis_lines.append(f"Products: {n_products}")
    analysis_lines.append(f"Total historical sales: {total_qty:,} units")
    analysis_lines.append(f"Total revenue: {sym}{total_rev:,.2f}")
    analysis_lines.append(f"30-day forecast: {forecast_qty:,.0f} units")

    if not fh.empty:
        date_range_start = fh["order_date"].min().strftime("%d/%m/%Y")
        date_range_end = fh["order_date"].max().strftime("%d/%m/%Y")
        analysis_lines.append(f"Data range: {date_range_start} to {date_range_end}")

    # Pre-compute exploded DataFrames (used by timeline + forecast sections)
    hist_exp = explode_categories(fh) if selected_cats and not fh.empty else pd.DataFrame()
    pred_exp = explode_categories(fp) if selected_cats and not fp.empty else pd.DataFrame()

    # --- 1. Category Timeline ---
    if selected_cats and not fh.empty:
        fig_timeline = go.Figure()
        exploded = hist_exp[hist_exp["cat_single"].isin(selected_cats)]
        for i, cat in enumerate(selected_cats):
            cat_data = exploded[exploded["cat_single"] == cat]
            if cat_data.empty:
                continue
            agg = cat_data.groupby("order_date")["quantity_sold"].sum().reset_index()
            color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]
            fig_timeline.add_trace(go.Scatter(
                x=agg["order_date"], y=agg["quantity_sold"],
                mode="lines", name=cat, line=dict(color=color, width=2),
            ))
        fig_timeline.update_layout(**PLOT_LAYOUT, title="Sales by Category Over Time",
                                   xaxis_title="Date", yaxis_title="Quantity Sold",
                                   legend=H_LEGEND, height=400)
        charts.append(("Sales by Category Over Time", fig_timeline))

    # --- 2. Category Forecast ---
    if selected_cats and not fh.empty:
        fig_fcst = go.Figure()
        for i, cat in enumerate(selected_cats):
            h = hist_exp[hist_exp["cat_single"] == cat]
            h_daily = h.groupby("order_date")["quantity_sold"].sum().reset_index()
            color = CATEGORY_COLORS[i % len(CATEGORY_COLORS)]
            if not h_daily.empty:
                cutoff = h_daily["order_date"].max() - pd.Timedelta(days=60)
                h_recent = h_daily[h_daily["order_date"] >= cutoff]
                fig_fcst.add_trace(go.Scatter(
                    x=h_recent["order_date"], y=h_recent["quantity_sold"],
                    mode="lines", name=f"{cat} (historical)",
                    line=dict(color=color, width=2), legendgroup=cat,
                ))
            if not pred_exp.empty:
                p = pred_exp[pred_exp["cat_single"] == cat]
                p_daily = p.groupby("order_date")["predicted_quantity"].sum().reset_index()
                if not p_daily.empty:
                    fig_fcst.add_trace(go.Scatter(
                        x=p_daily["order_date"], y=p_daily["predicted_quantity"],
                        mode="lines+markers", name=f"{cat} (forecast)",
                        line=dict(color=color, width=2.5, dash="dash"),
                        marker=dict(size=4), legendgroup=cat,
                    ))
        fig_fcst.update_layout(**PLOT_LAYOUT, title="Category Forecast (Next 30 Days)",
                               xaxis_title="Date", yaxis_title="Quantity",
                               legend=H_LEGEND, height=400)
        charts.append(("Daily Forecast by Category", fig_fcst))

        # Analysis: category breakdown
        analysis_lines.append("")
        analysis_lines.append("--- Category Breakdown ---")
        for cat in selected_cats:
            cat_h = hist_exp[hist_exp["cat_single"] == cat]
            cat_qty = int(cat_h["quantity_sold"].sum()) if not cat_h.empty else 0
            cat_rev_col = "revenue_converted" if "revenue_converted" in cat_h.columns else "revenue"
            cat_rev = cat_h[cat_rev_col].sum() if not cat_h.empty else 0
            n_prod = cat_h["product_id"].nunique() if not cat_h.empty else 0
            analysis_lines.append(f"  {cat}: {n_prod} products | {cat_qty:,} sold | {sym}{cat_rev:,.2f} revenue")

    # --- 3. Top Products ---
    if selected_cats and not fh.empty:
        fig_top = go.Figure()
        filtered_ps = filter_by_categories(product_sales, selected_cats, product_cat_map)
        filtered_ps = filter_by_event_tab(filtered_ps, tab_value)
        if selected_currencies:
            valid_pids_ps = set(fh["product_id"].unique())
            filtered_ps = filtered_ps[filtered_ps["product_id"].isin(valid_pids_ps)]
        top15 = filtered_ps.head(15).iloc[::-1]
        if not top15.empty:
            fig_top.add_trace(go.Bar(
                x=top15["quantity_sold"], y=top15["product_name"],
                orientation="h", marker_color=COLORS["accent"],
                marker_line_width=0, texttemplate="%{x:.0f}",
                textposition="outside", textfont_size=11,
            ))
            fig_top.update_layout(**PLOT_LAYOUT)
            fig_top.update_layout(title="Top 15 Products",
                                  margin=dict(l=10, r=40, t=40, b=30),
                                  showlegend=False, yaxis_title="",
                                  xaxis_title="Quantity Sold", height=500)
            charts.append(("Top 15 Products", fig_top))

            analysis_lines.append("")
            analysis_lines.append("--- Top 5 Products ---")
            for _, row in filtered_ps.head(5).iterrows():
                analysis_lines.append(f"  {row['product_name'][:60]}: {int(row['quantity_sold']):,} units")

    # --- 4. Monthly Revenue ---
    if selected_cats and not fh.empty:
        fig_rev = go.Figure()
        rev_data = fh.assign(month=lambda d: d["order_date"].dt.to_period("M").apply(lambda r: r.start_time))
        currencies = sorted(rev_data["currency"].dropna().unique()) if "currency" in rev_data.columns else [DISPLAY_CURRENCY]
        bar_colors = [COLORS["accent3"], COLORS["accent"], COLORS["accent4"],
                      COLORS["accent2"], "#7b8de0", "#e06070"]
        for i, cur in enumerate(currencies):
            cur_data = rev_data[rev_data["currency"] == cur] if "currency" in rev_data.columns else rev_data
            monthly = cur_data.groupby("month")[rev_col].sum().reset_index()
            if not monthly.empty:
                fig_rev.add_trace(go.Bar(
                    x=monthly["month"], y=monthly[rev_col],
                    name=f"from {currency_symbol(cur)} ({cur})" if len(currencies) > 1 else "Revenue",
                    marker_color=bar_colors[i % len(bar_colors)],
                    marker_line_width=0, opacity=0.85,
                ))
        fig_rev.update_layout(**PLOT_LAYOUT, title="Monthly Revenue",
                              xaxis_title="Month", yaxis_title=f"Revenue ({sym})",
                              barmode="stack", showlegend=len(currencies) > 1,
                              height=400)
        charts.append(("Monthly Revenue", fig_rev))

        if not rev_data.empty:
            analysis_lines.append("")
            analysis_lines.append("--- Revenue Trend ---")
            monthly_total = rev_data.groupby("month")[rev_col].sum().reset_index()
            if len(monthly_total) >= 2:
                last_month_rev = monthly_total[rev_col].iloc[-1]
                prev_month_rev = monthly_total[rev_col].iloc[-2]
                if prev_month_rev > 0:
                    change_pct = ((last_month_rev - prev_month_rev) / prev_month_rev) * 100
                    direction = "up" if change_pct > 0 else "down"
                    analysis_lines.append(
                        f"  Last month vs previous: {direction} {abs(change_pct):.1f}% "
                        f"({sym}{prev_month_rev:,.2f} -> {sym}{last_month_rev:,.2f})")
                avg_monthly = monthly_total[rev_col].mean()
                analysis_lines.append(f"  Average monthly revenue: {sym}{avg_monthly:,.2f}")

    # --- 5. Day of Week ---
    if selected_cats and not fh.empty:
        fig_wd = go.Figure()
        weekday_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        wd_data = fh.assign(weekday=lambda d: d["order_date"].dt.dayofweek)
        wd = wd_data.groupby("weekday")["quantity_sold"].sum().reset_index()
        wd["weekday_name"] = wd["weekday"].map(lambda x: weekday_names[x])
        colors_wd = [COLORS["accent3"] if x >= 5 else COLORS["accent"] for x in wd["weekday"]]
        fig_wd.add_trace(go.Bar(
            x=wd["weekday_name"], y=wd["quantity_sold"],
            marker_color=colors_wd, marker_line_width=0,
        ))
        fig_wd.update_layout(**PLOT_LAYOUT, title="Sales by Day of Week",
                             xaxis_title="", yaxis_title="Quantity",
                             showlegend=False, height=350)
        charts.append(("Sales by Day of Week", fig_wd))

        if not wd.empty:
            best_day = wd.loc[wd["quantity_sold"].idxmax()]
            worst_day = wd.loc[wd["quantity_sold"].idxmin()]
            analysis_lines.append("")
            analysis_lines.append("--- Day of Week Patterns ---")
            analysis_lines.append(f"  Best day: {best_day['weekday_name']} ({int(best_day['quantity_sold']):,} units)")
            analysis_lines.append(f"  Slowest day: {worst_day['weekday_name']} ({int(worst_day['quantity_sold']):,} units)")

    # --- 6. Product Detail (if selected) ---
    if product_id is not None:
        pid = int(product_id)
        h = hist_df[hist_df["product_id"] == pid].sort_values("order_date")
        p = pred_df[pred_df["product_id"] == pid].sort_values("order_date")

        if not h.empty or not p.empty:
            fig_prod = go.Figure()
            pname = h["product_name"].iloc[0] if not h.empty else p["product_name"].iloc[0] if not p.empty else f"Product {pid}"

            if not h.empty:
                h_agg = h.groupby("order_date")["quantity_sold"].sum().reset_index()
                fig_prod.add_trace(go.Scatter(
                    x=h_agg["order_date"], y=h_agg["quantity_sold"],
                    mode="lines", name="Actual",
                    line=dict(color=COLORS["accent"], width=1.5),
                ))

            if not p.empty:
                has_ci = "yhat_lower" in p.columns and "yhat_upper" in p.columns
                if has_ci:
                    fig_prod.add_trace(go.Scatter(
                        x=pd.concat([p["order_date"], p["order_date"][::-1]]),
                        y=pd.concat([p["yhat_upper"], p["yhat_lower"][::-1]]),
                        fill="toself", fillcolor="rgba(184, 115, 72, 0.15)",
                        line=dict(color="rgba(0,0,0,0)"), name="80% interval",
                        showlegend=True, hoverinfo="skip",
                    ))
                fig_prod.add_trace(go.Scatter(
                    x=p["order_date"], y=p["predicted_quantity"],
                    mode="lines", name="Forecast",
                    line=dict(color=COLORS["accent4"], width=2),
                ))

            fig_prod.update_layout(**PLOT_LAYOUT, title=f"Product Detail: {pname[:70]}",
                                   height=400, legend=H_LEGEND)
            charts.append((f"Product Detail: {pname[:50]}", fig_prod))

            pm = metrics_df[metrics_df["product_id"] == pid]
            if not pm.empty:
                row = pm.iloc[0]
                analysis_lines.append("")
                analysis_lines.append(f"--- Product Detail: {pname[:60]} ---")
                analysis_lines.append(f"  MAE: {row.get('mae', 0):.2f}")
                analysis_lines.append(f"  RMSE: {row.get('rmse', 0):.2f}")
                analysis_lines.append(f"  R2 Score: {row.get('r2_score', 0):.3f}")
                if not p.empty:
                    analysis_lines.append(f"  30-day forecast: {p['predicted_quantity'].sum():.1f} units")
                    analysis_lines.append(f"  Daily average forecast: {p['predicted_quantity'].mean():.2f} units/day")

    # --- 7. Model Metrics Summary ---
    if not fm.empty:
        analysis_lines.append("")
        analysis_lines.append("--- Model Performance Summary ---")
        avg_mae = fm["mae"].mean()
        avg_r2 = fm["r2_score"].mean()
        analysis_lines.append(f"  Average MAE: {avg_mae:.2f}")
        analysis_lines.append(f"  Average R2 Score: {avg_r2:.3f}")
        if "method" in fm.columns:
            method_counts = fm["method"].value_counts()
            for method, count in method_counts.items():
                analysis_lines.append(f"  {method}: {count} products")

    return charts, "\n".join(analysis_lines), fh, fp, fm


def _get_ai_report_analysis(selected_cats, tab_value, selected_currencies, product_id,
                            fh, fp, fm):
    """Ask the AI agent to generate a comprehensive report analysis."""
    try:
        cats_label = ", ".join(selected_cats) if selected_cats else "All"
        tab_labels = {"active": "Active Events", "past": "Past Events",
                      "course": "Online Courses", "map": "All"}
        tab_label = tab_labels.get(tab_value, "All")

        prompt_parts = [
            "Generate a comprehensive, professional sales report for the following scope:",
            f"- Event type: {tab_label}",
            f"- Categories: {cats_label}",
            f"- Currencies: {', '.join(selected_currencies) if selected_currencies else 'All'}",
        ]

        if product_id is not None:
            pid = int(product_id)
            prows = hist_df[hist_df["product_id"] == pid]
            pname = prows["product_name"].iloc[0] if not prows.empty else f"Product {pid}"
            prompt_parts.append(f"- Focus product: {pname} (ID #{pid})")

        prompt_parts.extend([
            "",
            "Structure the report with these sections:",
            "## Executive Summary",
            "A brief 2-3 sentence overview of the current state.",
            "",
            "## Key Performance Metrics",
            "Show the most important KPIs in a table.",
            "",
            "## Sales Trends & Analysis",
            "Analyze recent trends, compare periods, highlight growth or decline.",
            "",
            "## Top Products Performance",
            "Rank and analyze the best-performing products.",
            "",
            "## Revenue Analysis",
            "Break down revenue by category and trends over time.",
            "",
            "## Forecast & Outlook",
            "Summarize the 30-day forecast and what it means.",
            "",
            "## Recommendations",
            "Provide 3-5 actionable recommendations based on the data.",
            "",
            "Use markdown formatting with headers, tables, bullet points.",
            "Be specific with numbers - use exact values from the data.",
        ])

        prompt = "\n".join(prompt_parts)
        response = ai_agent.chat(prompt, fh, fp, fm)
        return response
    except Exception as e:
        print(f"  [WARNING] AI report generation failed: {e}")
        return None


def _safe_text(text):
    """Convert text to latin-1 safe string for PDF."""
    if not text:
        return ""
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _generate_pdf_report(charts, stats_text, ai_text, selected_cats,
                         tab_value, selected_currencies, product_id):
    """Generate a PDF report with AI analysis, stats, and chart images."""
    from fpdf import FPDF
    import plotly.io as pio
    from datetime import datetime
    import tempfile
    import io

    cats_label = ", ".join(selected_cats) if selected_cats else "All"
    tab_labels = {"active": "Active Events", "past": "Past Events",
                  "course": "Online Courses", "map": "All"}
    tab_label = tab_labels.get(tab_value, "All")
    now_str = datetime.now().strftime("%B %d, %Y at %H:%M")

    class ReportPDF(FPDF):
        def header(self):
            if self.page_no() > 1:
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(160, 140, 80)
                self.cell(0, 6, "TCCHE - Sales Report", align="L")
                self.cell(0, 6, now_str, align="R", new_x="LMARGIN", new_y="NEXT")
                self.set_draw_color(200, 164, 78)
                self.line(10, self.get_y(), 200, self.get_y())
                self.ln(4)

        def footer(self):
            self.set_y(-15)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(140)
            self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ---- Title Page ----
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 8, "TCCHE", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 16, "Sales Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_draw_color(200, 164, 78)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(100)
    pdf.cell(0, 8, _safe_text(now_str), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(f"Scope: {tab_label}"), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(f"Categories: {cats_label}"), align="C", new_x="LMARGIN", new_y="NEXT")
    if selected_currencies:
        pdf.cell(0, 8, _safe_text(f"Currencies: {', '.join(selected_currencies)}"),
                 align="C", new_x="LMARGIN", new_y="NEXT")

    # ---- AI Analysis ----
    content_w = pdf.w - pdf.l_margin - pdf.r_margin

    def _pdf_write_line(text, font_style="", font_size=10,
                        color=(60, 60, 60), spacing_before=0):
        """Write a line of text to the PDF, resetting X to left margin."""
        if spacing_before:
            pdf.ln(spacing_before)
        pdf.set_x(pdf.l_margin)
        pdf.set_font("Helvetica", font_style, font_size)
        pdf.set_text_color(*color)
        pdf.multi_cell(w=content_w, h=6, text=_safe_text(text),
                        new_x="LMARGIN", new_y="NEXT")

    if ai_text:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(200, 164, 78)
        pdf.cell(0, 6, "AI ANALYSIS", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        for line in ai_text.split("\n"):
            line = line.rstrip()
            if line.startswith("## "):
                _pdf_write_line(line[3:], "B", 14, spacing_before=4)
                pdf.set_draw_color(200, 164, 78)
                pdf.line(10, pdf.get_y(), 80, pdf.get_y())
                pdf.ln(2)
            elif line.startswith("### "):
                _pdf_write_line(line[4:], "B", 11, color=(80, 80, 80),
                                spacing_before=2)
            elif line.startswith("# "):
                _pdf_write_line(line[2:], "B", 16, color=(50, 50, 50),
                                spacing_before=4)
            elif line.startswith("|") and "---" in line:
                continue
            elif line.startswith("|"):
                pdf.set_x(pdf.l_margin)
                pdf.set_font("Courier", "", 8)
                pdf.set_text_color(60, 60, 60)
                pdf.multi_cell(w=content_w, h=5, text=_safe_text(line),
                               new_x="LMARGIN", new_y="NEXT")
            elif line.startswith("- ") or line.startswith("* "):
                _pdf_write_line("  - " + line[2:])
            elif line.startswith("**") and line.endswith("**"):
                _pdf_write_line(line.strip("*"), "B")
            elif line.strip() == "":
                pdf.ln(3)
            else:
                _pdf_write_line(line)

    # ---- Statistical Summary ----
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 6, "STATISTICAL SUMMARY", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(60, 60, 60)
    for line in stats_text.split("\n"):
        if line.startswith("---"):
            _pdf_write_line(line.strip("- "), "B", 11, color=(80, 80, 80),
                            spacing_before=2)
        elif line.strip() == "":
            pdf.ln(3)
        else:
            _pdf_write_line(line)

    # ---- Charts ----
    for title, fig in charts:
        try:
            fig_export = go.Figure(fig)
            fig_export.update_layout(
                paper_bgcolor="white", plot_bgcolor="white",
                font=dict(color="#333", size=12),
                xaxis=dict(gridcolor="#eee", showline=True, linecolor="#ccc"),
                yaxis=dict(gridcolor="#eee", showline=True, linecolor="#ccc",
                           rangemode="tozero"),
            )
            img_bytes = pio.to_image(fig_export, format="png", width=1100, height=480,
                                     scale=2)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(img_bytes)
                tmp_path = tmp.name

            pdf.add_page()
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(50, 50, 50)
            pdf.cell(0, 10, _safe_text(title), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
            pdf.image(tmp_path, x=5, w=200)

            import os as _os
            _os.unlink(tmp_path)
        except Exception as e:
            pdf.set_font("Helvetica", "I", 10)
            pdf.set_text_color(180, 80, 80)
            pdf.cell(0, 8, f"[Chart could not be rendered: {e}]",
                     new_x="LMARGIN", new_y="NEXT")

    # ---- Footer page ----
    pdf.add_page()
    pdf.ln(60)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(200, 164, 78)
    pdf.cell(0, 8, "TCCHE", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(140)
    pdf.cell(0, 8, "Sales Forecast Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _safe_text(now_str), align="C", new_x="LMARGIN", new_y="NEXT")

    return bytes(pdf.output())


# Step 1a: Open modal instantly on Generate Report click
clientside_callback(
    "function(n) { return [{display: 'block'}, Date.now()]; }",
    Output("report-modal", "style"),
    Output("report-trigger", "data"),
    Input("report-btn", "n_clicks"),
    prevent_initial_call=True,
)

# Step 1b: Close modal on Close / overlay click
clientside_callback(
    "function(a, b) { return {display: 'none'}; }",
    Output("report-modal", "style", allow_duplicate=True),
    Input("report-close-btn", "n_clicks"),
    Input("report-overlay", "n_clicks"),
    prevent_initial_call=True,
)


# Step 2: Populate report content (server-side, triggered by Store change)
@callback(
    Output("report-content", "children"),
    Output("report-cache", "data"),
    Input("report-trigger", "data"),
    State("category-filter", "value"),
    State("event-tabs", "value"),
    State("currency-filter", "value"),
    State("product-selector", "value"),
    prevent_initial_call=True,
)
def generate_report_content(trigger, selected_cats, tab_value,
                            selected_currencies, product_id):
    """Generate report content and cache for PDF reuse."""
    if not selected_cats:
        return html.Div(style={"textAlign": "center", "padding": "60px"}, children=[
            html.P("No categories selected.", style={
                "color": COLORS["text_muted"], "fontSize": "18px",
            }),
            html.P("Select categories in the filter above, then click Generate Report.",
                   style={"color": COLORS["text_muted"], "fontSize": "14px"}),
        ]), None

    charts, stats_text, fh, fp, fm = _build_report_charts(
        selected_cats, tab_value, selected_currencies, product_id
    )

    ai_text = _get_ai_report_analysis(
        selected_cats, tab_value, selected_currencies, product_id, fh, fp, fm
    )

    # Cache charts as JSON + text for PDF reuse (avoids 2nd AI call)
    import plotly.io as pio
    cache_data = {
        "charts_json": [(title, pio.to_json(fig)) for title, fig in charts],
        "stats_text": stats_text,
        "ai_text": ai_text or "",
        "selected_cats": selected_cats,
        "tab_value": tab_value,
        "selected_currencies": selected_currencies,
        "product_id": product_id,
    }

    report_children = []

    if ai_text:
        report_children.append(
            html.Div(style=card_style({"marginBottom": "24px",
                                       "borderTop": f"3px solid {COLORS['accent']}"}), children=[
                section_label("AI ANALYSIS"),
                dcc.Markdown(
                    ai_text,
                    style={
                        "fontSize": "14px",
                        "color": COLORS["text"],
                        "lineHeight": "1.8",
                    },
                    className="report-ai-markdown",
                ),
            ])
        )

    report_children.append(
        html.Div(style=card_style({"marginBottom": "24px"}), children=[
            section_label("STATISTICAL SUMMARY"),
            html.Pre(stats_text, style={
                "fontFamily": "'Outfit', sans-serif",
                "fontSize": "13px",
                "color": COLORS["text"],
                "backgroundColor": "transparent",
                "margin": "0",
                "whiteSpace": "pre-wrap",
                "lineHeight": "1.8",
            }),
        ])
    )

    for title, fig in charts:
        report_children.append(
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                dcc.Graph(figure=fig, config={"displayModeBar": True, "toImageButtonOptions": {
                    "format": "png", "width": 1200, "height": 500,
                }}),
            ])
        )

    return report_children, cache_data


# Instant spinner on Download PDF click
clientside_callback(
    """
    function(n) {
        if (!n) return [window.dash_clientside.no_update,
                        window.dash_clientside.no_update,
                        window.dash_clientside.no_update];
        return [{display: 'inline-block'}, 'Preparing...', true];
    }
    """,
    Output("pdf-spinner", "style"),
    Output("pdf-btn-text", "children"),
    Output("report-download-btn", "disabled"),
    Input("report-download-btn", "n_clicks"),
    prevent_initial_call=True,
)


@callback(
    Output("report-download", "data"),
    Output("pdf-spinner", "style", allow_duplicate=True),
    Output("pdf-btn-text", "children", allow_duplicate=True),
    Output("report-download-btn", "disabled", allow_duplicate=True),
    Input("report-download-btn", "n_clicks"),
    State("report-cache", "data"),
    State("category-filter", "value"),
    State("event-tabs", "value"),
    State("currency-filter", "value"),
    State("product-selector", "value"),
    prevent_initial_call=True,
)
def download_report_pdf(n_clicks, cache, selected_cats, tab_value,
                        selected_currencies, product_id):
    """Generate PDF from cached report data (no duplicate AI call)."""
    from datetime import datetime
    import plotly.io as pio

    if not n_clicks or not selected_cats:
        return no_update, no_update, no_update, no_update

    # Reuse cached data when available (same filters)
    if (cache
            and cache.get("selected_cats") == selected_cats
            and cache.get("tab_value") == tab_value
            and cache.get("selected_currencies") == selected_currencies
            and cache.get("product_id") == product_id):
        charts = [(t, pio.from_json(j)) for t, j in cache["charts_json"]]
        stats_text = cache["stats_text"]
        ai_text = cache["ai_text"] or None
    else:
        charts, stats_text, fh, fp, fm = _build_report_charts(
            selected_cats, tab_value, selected_currencies, product_id
        )
        ai_text = _get_ai_report_analysis(
            selected_cats, tab_value, selected_currencies, product_id, fh, fp, fm
        )

    pdf_bytes = _generate_pdf_report(
        charts, stats_text, ai_text,
        selected_cats, tab_value, selected_currencies, product_id
    )

    filename = f"tcche_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"

    return (
        dcc.send_bytes(pdf_bytes, filename, mime_type="application/pdf"),
        {"display": "none"},
        "Download PDF",
        False,
    )


