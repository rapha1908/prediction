"""
Google Analytics 4 data loader.
Fetches traffic, campaign, and Google Ads data via the GA4 Data API.
"""

import os
import time
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "")
GA4_CREDENTIALS_FILE = os.getenv("GA4_CREDENTIALS_FILE", "ga4-credentials.json")

_CLIENT = None
_CACHE = {}
_CACHE_TTL = 900  # 15 min


def _get_client():
    """Lazily create and cache the GA4 BetaAnalyticsDataClient."""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    if not GA4_PROPERTY_ID:
        raise RuntimeError("GA4_PROPERTY_ID not set in .env")

    creds_path = Path(GA4_CREDENTIALS_FILE)
    if not creds_path.is_absolute():
        creds_path = Path(__file__).parent / creds_path

    if not creds_path.exists():
        raise FileNotFoundError(f"GA4 credentials file not found: {creds_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)

    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    _CLIENT = BetaAnalyticsDataClient()
    return _CLIENT


def _run_report(dimensions, metrics, date_range="90daysAgo", limit=10000):
    """Run a GA4 report and return rows as list of dicts."""
    from google.analytics.data_v1beta.types import (
        RunReportRequest, Dimension, Metric, DateRange,
    )

    client = _get_client()
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=date_range, end_date="today")],
        limit=limit,
    )
    response = client.run_report(request)

    rows = []
    for row in response.rows:
        entry = {}
        for i, dim in enumerate(dimensions):
            entry[dim] = row.dimension_values[i].value
        for i, met in enumerate(metrics):
            val = row.metric_values[i].value
            try:
                entry[met] = float(val)
            except (ValueError, TypeError):
                entry[met] = val
        rows.append(entry)
    return rows


def _cached(key, fetcher, ttl=None):
    """Simple time-based cache wrapper."""
    ttl = ttl or _CACHE_TTL
    now = time.time()
    if key in _CACHE and (now - _CACHE[key]["ts"]) < ttl:
        return _CACHE[key]["data"]
    data = fetcher()
    _CACHE[key] = {"data": data, "ts": now}
    return data


def get_traffic_overview(days="90daysAgo"):
    """Sessions, users, pageviews by date."""
    def _fetch():
        rows = _run_report(
            dimensions=["date"],
            metrics=["sessions", "totalUsers", "screenPageViews", "bounceRate",
                      "averageSessionDuration"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        return df.sort_values("date")

    return _cached(f"traffic_{days}", _fetch)


def get_channel_breakdown(days="90daysAgo"):
    """Sessions by channel group (Paid Search, Organic, Direct, etc.)."""
    def _fetch():
        rows = _run_report(
            dimensions=["sessionDefaultChannelGroup"],
            metrics=["sessions", "totalUsers", "conversions", "purchaseRevenue"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.sort_values("sessions", ascending=False)
        return df

    return _cached(f"channels_{days}", _fetch)


def get_google_ads_campaigns(days="90daysAgo"):
    """Google Ads campaign performance (requires Google Ads linked to GA4)."""
    def _fetch():
        rows = _run_report(
            dimensions=["sessionGoogleAdsCampaignName"],
            metrics=["sessions", "totalUsers", "conversions",
                      "purchaseRevenue", "advertiserAdCost",
                      "advertiserAdClicks", "advertiserAdImpressions"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.rename(columns={
            "sessionGoogleAdsCampaignName": "campaign",
            "advertiserAdCost": "cost",
            "advertiserAdClicks": "clicks",
            "advertiserAdImpressions": "impressions",
        })
        df = df[df["campaign"] != "(not set)"]
        if not df.empty:
            df["cpc"] = df.apply(lambda r: round(r["cost"] / r["clicks"], 2) if r["clicks"] > 0 else 0.0, axis=1)
            df["ctr"] = df.apply(lambda r: round(r["clicks"] / r["impressions"] * 100, 2) if r["impressions"] > 0 else 0.0, axis=1)
            df["roas"] = df.apply(lambda r: round(r["purchaseRevenue"] / r["cost"], 2) if r["cost"] > 0 else 0.0, axis=1)
        return df.sort_values("sessions", ascending=False)

    return _cached(f"gads_campaigns_{days}", _fetch)


def get_google_ads_daily(days="90daysAgo"):
    """Google Ads daily spend and performance."""
    def _fetch():
        rows = _run_report(
            dimensions=["date", "sessionGoogleAdsCampaignName"],
            metrics=["advertiserAdCost", "advertiserAdClicks",
                      "advertiserAdImpressions", "conversions",
                      "purchaseRevenue"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df[df["sessionGoogleAdsCampaignName"] != "(not set)"]
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.rename(columns={
            "advertiserAdCost": "cost",
            "advertiserAdClicks": "clicks",
            "advertiserAdImpressions": "impressions",
        })
        df = df.groupby("date").agg(
            cost=("cost", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
            conversions=("conversions", "sum"),
            purchaseRevenue=("purchaseRevenue", "sum"),
        ).reset_index()
        return df.sort_values("date")

    return _cached(f"gads_daily_{days}", _fetch)


def get_source_medium(days="90daysAgo"):
    """Sessions by source/medium (granular)."""
    def _fetch():
        rows = _run_report(
            dimensions=["sessionSource", "sessionMedium"],
            metrics=["sessions", "totalUsers", "conversions", "purchaseRevenue"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.sort_values("sessions", ascending=False)
        return df

    return _cached(f"source_medium_{days}", _fetch)


def get_landing_pages(days="90daysAgo"):
    """Top landing pages by sessions."""
    def _fetch():
        rows = _run_report(
            dimensions=["landingPagePlusQueryString"],
            metrics=["sessions", "totalUsers", "conversions", "bounceRate"],
            date_range=days,
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df = df.rename(columns={"landingPagePlusQueryString": "landing_page"})
        return df.sort_values("sessions", ascending=False).head(20)

    return _cached(f"landing_{days}", _fetch)


def invalidate_cache():
    """Clear all cached GA4 data."""
    _CACHE.clear()


def is_configured():
    """Check if GA4 is properly configured."""
    if not GA4_PROPERTY_ID:
        return False
    creds_path = Path(GA4_CREDENTIALS_FILE)
    if not creds_path.is_absolute():
        creds_path = Path(__file__).parent / creds_path
    return creds_path.exists()


def test_connection():
    """Test GA4 connection by fetching 1 day of sessions."""
    try:
        rows = _run_report(
            dimensions=["date"],
            metrics=["sessions"],
            date_range="7daysAgo",
            limit=1,
        )
        return True, f"OK – {len(rows)} rows returned"
    except Exception as e:
        return False, str(e)
