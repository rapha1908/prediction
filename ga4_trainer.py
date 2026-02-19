"""
GA4-enhanced forecasting trainer.
Trains two Prophet models per product (base vs GA4-enriched) and compares them.
"""

import gc
import logging
import threading
import time
import warnings

import numpy as np
import pandas as pd

import ga4_loader

warnings.filterwarnings("ignore")
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

FORECAST_DAYS = 30
ACTIVE_WINDOW_WEEKS = 12
MIN_DAYS_PROPHET = 14
MAX_GAP_DAYS = 42

_GA4_REGRESSORS = ["sessions", "ad_clicks", "ad_cost", "paid_sessions"]

# Training state (shared with dashboard callbacks)
_state = {
    "running": False,
    "progress": "",
    "current": 0,
    "total": 0,
    "results": None,
}
_lock = threading.Lock()


def get_state():
    with _lock:
        return dict(_state)


def get_results():
    with _lock:
        return _state["results"]


# ── GA4 data fetching ──

def _fetch_ga4_daily(days_back: int) -> pd.DataFrame:
    """Fetch and merge traffic + ads daily data from GA4."""
    date_range = f"{days_back}daysAgo"

    traffic_df = ga4_loader.get_traffic_overview(date_range)
    ads_df = ga4_loader.get_google_ads_daily(date_range)

    # Get paid sessions by channel by date
    paid_df = pd.DataFrame()
    try:
        rows = ga4_loader._run_report(
            dimensions=["date", "sessionDefaultChannelGroup"],
            metrics=["sessions"],
            date_range=date_range,
        )
        if rows:
            pdf = pd.DataFrame(rows)
            pdf["date"] = pd.to_datetime(pdf["date"], format="%Y%m%d")
            paid_channels = {"Paid Search", "Paid Social", "Paid Other"}
            paid_df = (
                pdf[pdf["sessionDefaultChannelGroup"].isin(paid_channels)]
                .groupby("date")["sessions"]
                .sum()
                .reset_index()
                .rename(columns={"sessions": "paid_sessions"})
            )
    except Exception:
        pass

    if traffic_df.empty:
        return pd.DataFrame()

    result = traffic_df[["date", "sessions"]].copy()

    if not ads_df.empty:
        ads_merge = ads_df[["date", "clicks", "cost"]].rename(
            columns={"clicks": "ad_clicks", "cost": "ad_cost"}
        )
        result = result.merge(ads_merge, on="date", how="left")
    else:
        result["ad_clicks"] = 0.0
        result["ad_cost"] = 0.0

    if not paid_df.empty:
        result = result.merge(paid_df, on="date", how="left")
    else:
        result["paid_sessions"] = 0.0

    result = result.fillna(0.0)
    return result.sort_values("date")


# ── Prophet helpers ──

def _find_active_phase(daily_data):
    sorted_dates = np.sort(daily_data["order_date"].unique())
    if len(sorted_dates) <= 1:
        return daily_data
    for i in range(len(sorted_dates) - 1, 0, -1):
        gap = (sorted_dates[i] - sorted_dates[i - 1]) / np.timedelta64(1, "D")
        if gap > MAX_GAP_DAYS:
            cutoff = pd.Timestamp(sorted_dates[i])
            return daily_data[daily_data["order_date"] >= cutoff]
    max_date = pd.Timestamp(sorted_dates[-1])
    cutoff = max_date - pd.Timedelta(days=365)
    return daily_data[daily_data["order_date"] >= cutoff]


def _prepare_prophet_data(daily_data, today):
    active = _find_active_phase(daily_data)
    daily_agg = active.groupby("order_date")["quantity_sold"].sum().reset_index()
    daily_agg.columns = ["ds", "y"]
    if daily_agg.empty:
        return pd.DataFrame(columns=["ds", "y"])
    date_range = pd.date_range(daily_agg["ds"].min(), today)
    full = pd.DataFrame({"ds": date_range})
    full = full.merge(daily_agg, on="ds", how="left")
    full["y"] = full["y"].fillna(0)
    return full


def _train_prophet_base(prophet_data, forecast_days, today):
    """Train base Prophet (univariate) and return (predictions_df, metrics_dict)."""
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    n = len(prophet_data)
    has_yearly = n > 365
    split_idx = int(n * 0.8)
    train = prophet_data.iloc[:split_idx]
    test = prophet_data.iloc[split_idx:]

    model_eval = Prophet(
        daily_seasonality=False, weekly_seasonality=True,
        yearly_seasonality=has_yearly, seasonality_mode="additive",
        changepoint_prior_scale=0.15, seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    model_eval.fit(train)

    if len(test) > 0:
        tf = model_eval.predict(test[["ds"]])
        y_pred = tf["yhat"].clip(lower=0).values
        y_true = test["y"].values
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred) if len(y_true) > 1 else 0
    else:
        mae = rmse = r2 = 0

    del model_eval
    gc.collect()

    model = Prophet(
        daily_seasonality=False, weekly_seasonality=True,
        yearly_seasonality=has_yearly, seasonality_mode="additive",
        changepoint_prior_scale=0.15, seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    model.fit(prophet_data)
    future = model.make_future_dataframe(periods=forecast_days)
    forecast = model.predict(future)
    future_pred = forecast[forecast["ds"] > today].copy()

    del model, forecast, future
    gc.collect()

    if future_pred.empty:
        return None, None

    preds = pd.DataFrame({
        "ds": future_pred["ds"].values,
        "yhat": future_pred["yhat"].clip(lower=0).round(2).values,
        "yhat_lower": future_pred["yhat_lower"].clip(lower=0).round(2).values,
        "yhat_upper": future_pred["yhat_upper"].clip(lower=0).round(2).values,
    })
    metrics = {"mae": round(mae, 2), "rmse": round(rmse, 2), "r2": round(r2, 3),
               "train_size": split_idx, "test_size": len(test)}
    return preds, metrics


def _train_prophet_ga4(prophet_data, ga4_daily, forecast_days, today):
    """Train Prophet with GA4 regressors and return (predictions_df, metrics_dict)."""
    from prophet import Prophet
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    merged = prophet_data.merge(ga4_daily.rename(columns={"date": "ds"}), on="ds", how="left")
    for col in _GA4_REGRESSORS:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0.0)

    n = len(merged)
    has_yearly = n > 365
    split_idx = int(n * 0.8)
    train = merged.iloc[:split_idx]
    test = merged.iloc[split_idx:]

    model_eval = Prophet(
        daily_seasonality=False, weekly_seasonality=True,
        yearly_seasonality=has_yearly, seasonality_mode="additive",
        changepoint_prior_scale=0.15, seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    for reg in _GA4_REGRESSORS:
        model_eval.add_regressor(reg)
    model_eval.fit(train)

    if len(test) > 0:
        tf = model_eval.predict(test)
        y_pred = tf["yhat"].clip(lower=0).values
        y_true = test["y"].values
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred) if len(y_true) > 1 else 0
    else:
        mae = rmse = r2 = 0

    del model_eval
    gc.collect()

    model = Prophet(
        daily_seasonality=False, weekly_seasonality=True,
        yearly_seasonality=has_yearly, seasonality_mode="additive",
        changepoint_prior_scale=0.15, seasonality_prior_scale=1.0,
        interval_width=0.80,
    )
    for reg in _GA4_REGRESSORS:
        model.add_regressor(reg)
    model.fit(merged)

    future = model.make_future_dataframe(periods=forecast_days)
    # For future regressor values, use the mean of the last 14 days
    recent = merged.tail(14)
    for reg in _GA4_REGRESSORS:
        future[reg] = future["ds"].apply(
            lambda d: merged.loc[merged["ds"] == d, reg].values[0]
            if d in merged["ds"].values
            else recent[reg].mean()
        )

    forecast = model.predict(future)
    future_pred = forecast[forecast["ds"] > today].copy()

    del model, forecast, future
    gc.collect()

    if future_pred.empty:
        return None, None

    preds = pd.DataFrame({
        "ds": future_pred["ds"].values,
        "yhat": future_pred["yhat"].clip(lower=0).round(2).values,
        "yhat_lower": future_pred["yhat_lower"].clip(lower=0).round(2).values,
        "yhat_upper": future_pred["yhat_upper"].clip(lower=0).round(2).values,
    })
    metrics = {"mae": round(mae, 2), "rmse": round(rmse, 2), "r2": round(r2, 3),
               "train_size": split_idx, "test_size": len(test)}
    return preds, metrics


# ── Main training pipeline ──

def _run_training(days_back: int):
    """Run the full training pipeline in a background thread."""
    import db

    try:
        with _lock:
            _state["progress"] = "Loading sales data..."
            _state["current"] = 0

        daily_sales = db.load_daily_sales()
        if daily_sales.empty:
            with _lock:
                _state["progress"] = "ERROR: No sales data found"
                _state["running"] = False
            return

        with _lock:
            _state["progress"] = "Fetching GA4 data..."

        ga4_daily = _fetch_ga4_daily(days_back)
        ga4_available = not ga4_daily.empty

        if not ga4_available:
            with _lock:
                _state["progress"] = "WARNING: No GA4 data – training base model only"

        today = pd.Timestamp.now().normalize()
        cutoff = today - pd.Timedelta(weeks=ACTIVE_WINDOW_WEEKS)
        active_pids = sorted(
            daily_sales[daily_sales["order_date"] >= cutoff]["product_id"].unique()
        )

        with _lock:
            _state["total"] = len(active_pids)
            _state["progress"] = f"Training {len(active_pids)} products..."

        comparison_rows = []
        base_predictions = {}
        ga4_predictions = {}
        historical = {}

        for idx, pid in enumerate(active_pids):
            product_data = daily_sales[daily_sales["product_id"] == pid].copy()
            if product_data.empty:
                continue

            pname = product_data["product_name"].iloc[-1]
            cat = product_data["category"].iloc[-1]

            prophet_data = _prepare_prophet_data(product_data, today)
            n_days = len(prophet_data)
            n_nonzero = int((prophet_data["y"] > 0).sum()) if not prophet_data.empty else 0

            with _lock:
                _state["current"] = idx + 1
                short_name = pname[:40] + "..." if len(pname) > 40 else pname
                _state["progress"] = f"[{idx + 1}/{len(active_pids)}] {short_name}"

            if n_days < MIN_DAYS_PROPHET or n_nonzero < 3:
                continue

            # Store historical for this product
            historical[pid] = {
                "product_name": pname, "category": cat,
                "data": prophet_data[["ds", "y"]].copy(),
            }

            # Train base model
            base_preds, base_metrics = _train_prophet_base(
                prophet_data, FORECAST_DAYS, today
            )
            if base_preds is None:
                continue

            base_predictions[pid] = base_preds

            row = {
                "product_id": pid, "product_name": pname, "category": cat,
                "mae_base": base_metrics["mae"],
                "rmse_base": base_metrics["rmse"],
                "r2_base": base_metrics["r2"],
                "train_size": base_metrics["train_size"],
                "test_size": base_metrics["test_size"],
            }

            # Train GA4 model if data available
            if ga4_available:
                ga4_preds, ga4_metrics = _train_prophet_ga4(
                    prophet_data, ga4_daily, FORECAST_DAYS, today
                )
                if ga4_preds is not None:
                    ga4_predictions[pid] = ga4_preds
                    row["mae_ga4"] = ga4_metrics["mae"]
                    row["rmse_ga4"] = ga4_metrics["rmse"]
                    row["r2_ga4"] = ga4_metrics["r2"]
                else:
                    row["mae_ga4"] = row["rmse_ga4"] = None
                    row["r2_ga4"] = None
            else:
                row["mae_ga4"] = row["rmse_ga4"] = row["r2_ga4"] = None

            comparison_rows.append(row)

        comparison_df = pd.DataFrame(comparison_rows)

        if not comparison_df.empty:
            # Determine best model per product
            def _best(r):
                if pd.isna(r.get("mae_ga4")):
                    return "base"
                return "ga4" if r["mae_ga4"] < r["mae_base"] else "base"

            comparison_df["best_model"] = comparison_df.apply(_best, axis=1)

            mask = comparison_df["mae_ga4"].notna() & (comparison_df["mae_base"] > 0)
            comparison_df.loc[mask, "improvement_pct"] = (
                (comparison_df.loc[mask, "mae_base"] - comparison_df.loc[mask, "mae_ga4"])
                / comparison_df.loc[mask, "mae_base"] * 100
            ).round(1)
            comparison_df["improvement_pct"] = comparison_df["improvement_pct"].fillna(0)

        with _lock:
            _state["results"] = {
                "comparison": comparison_df,
                "base_predictions": base_predictions,
                "ga4_predictions": ga4_predictions,
                "historical": historical,
                "trained_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
                "ga4_available": ga4_available,
                "days_back": days_back,
            }
            _state["progress"] = f"Done! {len(comparison_rows)} products trained."
            _state["running"] = False

    except Exception as e:
        with _lock:
            _state["progress"] = f"ERROR: {e}"
            _state["running"] = False


def start_training(days_back: int = 90):
    """Start training in background thread. Returns immediately."""
    with _lock:
        if _state["running"]:
            return False
        _state["running"] = True
        _state["progress"] = "Starting..."
        _state["current"] = 0
        _state["total"] = 0
        _state["results"] = None

    t = threading.Thread(target=_run_training, args=(days_back,), daemon=True)
    t.start()
    return True
