"""
API wrapper for the Tcche Order Bumps WordPress plugin.
Base URL: {WOOCOMMERCE_URL_BASE}/wp-json/tcche-ob/v1
Auth: WordPress Application Passwords (Basic Auth).
"""

import os
import json
from dotenv import load_dotenv
import requests

load_dotenv(override=True)

try:
    from openai import OpenAI
    _OPENAI_OK = True
except ImportError:
    _OPENAI_OK = False

_WC_URL = os.getenv("WOOCOMMERCE_URL", "https://tcche.org/wp-json/wc/v3/")
_WC_KEY = os.getenv("WOOCOMMERCE_KEY", "")
_WC_SECRET = os.getenv("WOOCOMMERCE_SECRET", "")
_BASE_URL = _WC_URL.split("/wp-json/")[0] + "/wp-json/tcche-ob/v1"

_WP_USER = os.getenv("WP_USER", "")
_WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")

_TIMEOUT = 15


def _auth():
    if not _WP_USER or not _WP_APP_PASSWORD:
        return None
    return (_WP_USER, _WP_APP_PASSWORD)


def _headers():
    return {"Content-Type": "application/json", "Accept": "application/json"}


def is_configured() -> bool:
    return bool(_WP_USER and _WP_APP_PASSWORD)


# ── Bumps CRUD ──────────────────────────────────────────────

def list_bumps(status: str | None = None) -> list[dict]:
    auth = _auth()
    if not auth:
        return []
    params = {}
    if status:
        params["status"] = status
    try:
        resp = requests.get(
            f"{_BASE_URL}/bumps",
            auth=auth, headers=_headers(), params=params,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
        print(f"[OrderBumps] list_bumps HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[OrderBumps] list_bumps error: {e}")
    return []


def get_bump(bump_id: int) -> dict | None:
    auth = _auth()
    if not auth:
        return None
    try:
        resp = requests.get(
            f"{_BASE_URL}/bumps/{bump_id}",
            auth=auth, headers=_headers(),
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[OrderBumps] get_bump error: {e}")
    return None


def create_bump(payload: dict) -> dict | None:
    """Create a new order bump. Required fields: title, bump_product_id."""
    auth = _auth()
    if not auth:
        return None
    try:
        resp = requests.post(
            f"{_BASE_URL}/bumps",
            auth=auth, headers=_headers(), json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code in (200, 201):
            return resp.json()
        print(f"[OrderBumps] create_bump HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as e:
        print(f"[OrderBumps] create_bump error: {e}")
    return None


def update_bump(bump_id: int, payload: dict) -> dict | None:
    auth = _auth()
    if not auth:
        return None
    try:
        resp = requests.put(
            f"{_BASE_URL}/bumps/{bump_id}",
            auth=auth, headers=_headers(), json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[OrderBumps] update_bump error: {e}")
    return None


def delete_bump(bump_id: int) -> bool:
    auth = _auth()
    if not auth:
        return False
    try:
        resp = requests.delete(
            f"{_BASE_URL}/bumps/{bump_id}",
            auth=auth, headers=_headers(),
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("deleted", False)
    except Exception as e:
        print(f"[OrderBumps] delete_bump error: {e}")
    return False


# ── WooCommerce categories ───────────────────────────────────

def list_wc_categories() -> list[dict]:
    """Fetch WooCommerce product categories (id, name, count).
    Uses WC REST API consumer key/secret auth.
    """
    if not _WC_KEY or not _WC_SECRET:
        return []
    all_cats: list[dict] = []
    page = 1
    try:
        while True:
            resp = requests.get(
                f"{_WC_URL}products/categories",
                params={
                    "consumer_key": _WC_KEY,
                    "consumer_secret": _WC_SECRET,
                    "per_page": 100,
                    "page": page,
                },
                timeout=_TIMEOUT,
            )
            if resp.status_code != 200:
                print(f"[OrderBumps] list_wc_categories HTTP {resp.status_code}: {resp.text[:200]}")
                break
            batch = resp.json()
            if not batch:
                break
            all_cats.extend({"id": c["id"], "name": c["name"], "count": c.get("count", 0)} for c in batch)
            if len(batch) < 100:
                break
            page += 1
    except Exception as e:
        print(f"[OrderBumps] list_wc_categories error: {e}")
    return sorted(all_cats, key=lambda c: c["name"])


# ── Analytics ───────────────────────────────────────────────

def analytics_summary(bump_id: int | None = None,
                      date_from: str | None = None,
                      date_to: str | None = None) -> dict:
    auth = _auth()
    if not auth:
        return {}
    params = {}
    if bump_id:
        params["bump_id"] = bump_id
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    try:
        resp = requests.get(
            f"{_BASE_URL}/analytics/summary",
            auth=auth, headers=_headers(), params=params,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[OrderBumps] analytics_summary error: {e}")
    return {}


def analytics_by_bump(date_from: str | None = None,
                      date_to: str | None = None) -> list[dict]:
    auth = _auth()
    if not auth:
        return []
    params = {}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    try:
        resp = requests.get(
            f"{_BASE_URL}/analytics/by-bump",
            auth=auth, headers=_headers(), params=params,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[OrderBumps] analytics_by_bump error: {e}")
    return []


def analytics_daily(bump_id: int | None = None,
                    date_from: str | None = None,
                    date_to: str | None = None) -> list[dict]:
    auth = _auth()
    if not auth:
        return []
    params = {}
    if bump_id:
        params["bump_id"] = bump_id
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    try:
        resp = requests.get(
            f"{_BASE_URL}/analytics/daily",
            auth=auth, headers=_headers(), params=params,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[OrderBumps] analytics_daily error: {e}")
    return []


# ── Health Check ─────────────────────────────────────────────

def health() -> dict:
    """Call the plugin health endpoint to verify tables, bump count, etc."""
    auth = _auth()
    if not auth:
        return {"error": "WP_USER / WP_APP_PASSWORD not configured"}
    try:
        resp = requests.get(
            f"{_BASE_URL}/health",
            auth=auth, headers=_headers(),
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"error": f"HTTP {resp.status_code}", "body": resp.text[:300]}
    except Exception as e:
        return {"error": str(e)}


def setup() -> dict:
    """Force-create analytics tables and return health status."""
    auth = _auth()
    if not auth:
        return {"error": "WP_USER / WP_APP_PASSWORD not configured"}
    try:
        resp = requests.post(
            f"{_BASE_URL}/setup",
            auth=auth, headers=_headers(),
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"error": f"HTTP {resp.status_code}", "body": resp.text[:300]}
    except Exception as e:
        return {"error": str(e)}


# ── AI-generated bump copy ──────────────────────────────────

def _scrape_page_content(url: str) -> str:
    """Fetch a webpage and extract relevant text content for AI context.

    Returns a condensed string with: Open Graph tags, page title,
    meta description, all headings, and key paragraphs (max ~3000 chars).
    Returns empty string on any error.
    """
    try:
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url.strip(), headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        parts = []

        # Open Graph title (usually the most descriptive)
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content", "").strip():
            parts.append(f"Event/Product title: {og_title['content'].strip()}")

        # Open Graph description
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content", "").strip():
            parts.append(f"Event/Product description: {og_desc['content'].strip()}")

        # Page title (fallback)
        title_tag = soup.find("title")
        if title_tag and title_tag.get_text(strip=True):
            t = title_tag.get_text(strip=True)
            if not any(t in p for p in parts):
                parts.append(f"Page title: {t}")

        # Meta description (fallback)
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content", "").strip():
            d = meta_desc["content"].strip()
            if not any(d in p for p in parts):
                parts.append(f"Meta description: {d}")

        # All headings H1-H4
        for tag in ["h1", "h2", "h3", "h4"]:
            for el in soup.find_all(tag)[:6]:
                text = el.get_text(strip=True)
                if text and len(text) > 3:
                    parts.append(f"{tag.upper()}: {text}")

        # Key paragraphs — keep document order so the most prominent content
        # (usually at the top of the page, about the main product/speaker) comes first
        para_count = 0
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 60:
                parts.append(text)
                para_count += 1
                if para_count >= 6:
                    break

        content = "\n".join(parts)
        # Limit to ~3000 chars to stay within token budget
        return content[:3000]

    except Exception as e:
        print(f"[OrderBumps] _scrape_page_content error for {url}: {e}")
        return ""


def generate_bump_copy(bump_product_name: str,
                       trigger_product_name: str | None = None,
                       trigger_category_name: str | None = None,
                       page_url: str | None = None) -> dict:
    """Use OpenAI to generate a compelling title, headline and description
    for a checkout order bump.

    If page_url is provided, the page content is scraped and included in
    the prompt so the AI can write more specific, relevant copy.

    Returns dict with keys: title, headline, description.
    Falls back to simple defaults if OpenAI is unavailable.
    """
    fallback = _fallback_copy(bump_product_name, trigger_product_name, trigger_category_name)

    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OpenAI_API_KEY")
    if not _OPENAI_OK or not api_key:
        return fallback

    if trigger_product_name:
        context = (
            f"Bump product: {bump_product_name}\n"
            f"Trigger product (already in cart): {trigger_product_name}\n"
            f"The bump is shown when the trigger product is in the cart."
        )
    elif trigger_category_name:
        context = (
            f"Bump product: {bump_product_name}\n"
            f"Trigger category: {trigger_category_name}\n"
            f"The bump is shown when any product from the '{trigger_category_name}' category is in the cart."
        )
    else:
        context = (
            f"Bump product: {bump_product_name}\n"
            f"No trigger — this bump is shown to every customer at checkout."
        )

    # Optionally enrich with scraped page content
    page_context = ""
    if page_url and page_url.strip().startswith("http"):
        scraped = _scrape_page_content(page_url)
        if scraped:
            page_context = f"\n\nProduct page content (use this to write specific, relevant copy):\n{scraped}"

    if page_context:
        page_instruction = (
            "\n\nIMPORTANT: You MUST use the specific details from the page content above to write the copy. "
            "Mention the specific speaker name, event name, key benefits, or unique selling points found on the page. "
            "DO NOT write generic copy like 'Elevate your experience' or 'Don't miss this'. "
            "The copy must be specific to THIS product based on what you read from the page."
        )
    else:
        page_instruction = ""

    prompt = f"""You are a conversion copywriter for an events & courses company called TCCHE.
Write checkout order-bump copy for the product below.

{context}{page_context}{page_instruction}

Return ONLY valid JSON with exactly these keys (no markdown, no extra text):
{{
  "title": "short internal title (max 60 chars)",
  "headline": "catchy one-liner shown to customer (max 80 chars) — must reference specific content from the page",
  "description": "1-2 sentence persuasive description with <b>product name</b> in bold HTML (max 200 chars) — must mention specific speaker, topic, or benefit from the page"
}}"""

    try:
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=300,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        data = json.loads(text)
        return {
            "title": str(data.get("title", fallback["title"]))[:60],
            "headline": str(data.get("headline", fallback["headline"]))[:80],
            "description": str(data.get("description", fallback["description"]))[:200],
        }
    except Exception as e:
        print(f"[OrderBumps] generate_bump_copy error: {e}")
        return fallback


def _fallback_copy(bump_name: str, trigger_name: str | None,
                   trigger_cat: str | None = None) -> dict:
    """Simple fallback when OpenAI is unavailable."""
    if trigger_name:
        return {
            "title": f"Bump: {bump_name}"[:60],
            "headline": "Customers also bought this!"[:80],
            "description": f"People who purchased <b>{trigger_name}</b> often add <b>{bump_name}</b>."[:200],
        }
    if trigger_cat:
        return {
            "title": f"Bump: {bump_name}"[:60],
            "headline": "Complete your experience!"[:80],
            "description": f"Add <b>{bump_name}</b> to complement your <b>{trigger_cat}</b> purchase."[:200],
        }
    return {
        "title": f"Bump: {bump_name}"[:60],
        "headline": "Don't miss this!"[:80],
        "description": f"Add <b>{bump_name}</b> to your order."[:200],
    }
