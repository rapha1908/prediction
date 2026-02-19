# -*- coding: utf-8 -*-
"""
HubSpot Forms Manager.
Scrapes events/courses from tcche.org and manages which items
appear in which HubSpot forms.
"""

import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN", "")

# ============================================================
# FORM DEFINITIONS
# Each form has:
#   - form_id: HubSpot form GUID
#   - form_name: Display name
#   - item_types: which item types it accepts ("events", "courses", "both")
#   - update_mode: "direct" or "dependent"
#   - field_name: field to update (for direct mode)
#   - dependent_parent: parent field name (for dependent mode)
#   - dependent_fields: dict mapping item_type -> field_name (for dependent mode)
# ============================================================

FORM_DEFINITIONS = {
    "contact": {
        "form_id": "d447bb00-1d72-43c6-a891-0efe36cde544",
        "form_name": "Contact Form",
        "item_types": "both",
        "update_mode": "dependent",
        "dependent_parent": "type_of_request",
        "dependent_fields": {
            "event": "test",
            "course": "online_courses",
        },
    },
    "volunteer": {
        "form_id": "95821582-73af-42ef-92ea-462884518af6",
        "form_name": "Volunteer Form",
        "item_types": "events",
        "update_mode": "direct",
        "field_name": "test",
    },
    "vendors": {
        "form_id": "b01b24bc-ae59-4bdd-8ac2-493d3fa6b55a",
        "form_name": "Vendors Form",
        "item_types": "events",
        "update_mode": "direct",
        "field_name": "test",
    },
    "scholarship": {
        "form_id": "1ccd76d5-f6e7-41aa-b2e8-35ffb7dfafaa",
        "form_name": "Scholarship Form",
        "item_types": "both",
        "update_mode": "direct",
        "field_name": "event___course",
    },
}


def _transform_title(title):
    """Capitalize each word, keeping 'TCCHE' uppercase."""
    return " ".join(
        word.upper() if word.lower() == "tcche" else word.capitalize()
        for word in title.lower().split()
    )


# ============================================================
# SCRAPING
# ============================================================

def _get_soup(url):
    """Fetch a URL and return a BeautifulSoup object with proper encoding."""
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    return BeautifulSoup(response.text, "html.parser")


def scrape_events():
    """Scrape event titles from tcche.org/events-listing/."""
    url = "https://tcche.org/events-listing/"
    try:
        soup = _get_soup(url)
    except requests.RequestException as e:
        print(f"  [FORMS] Failed to fetch events: {e}")
        return []

    # Strategy 1: Original tribe-events list view
    titles = soup.find_all("h3", class_="tribe-events-calendar-list__event-title")
    if titles:
        events = [_transform_title(t.text.strip()) for t in titles if t.text.strip()]
        print(f"  [FORMS] Scraped {len(events)} events (tribe list view).")
        return events

    # Strategy 2: Custom page with event links (current layout)
    # Find all links pointing to /event/ pages, keep longest title per URL
    url_to_title = {}
    skip_labels = {"get tickets", "buy tickets", "learn more", "read more", ""}
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        text = link.text.strip()
        if "/event/" not in href:
            continue
        if "#" in href:
            continue
        if text.lower() in skip_labels:
            continue
        norm_url = href.rstrip("/").lower()
        # Keep the longest title for each URL (more descriptive)
        if norm_url not in url_to_title or len(text) > len(url_to_title[norm_url]):
            url_to_title[norm_url] = text

    events = [_transform_title(t) for t in url_to_title.values()]
    print(f"  [FORMS] Scraped {len(events)} events (event links).")
    return events


def scrape_courses():
    """Scrape online course titles from tcche.org/online-courses/."""
    url = "https://tcche.org/online-courses/"
    try:
        soup = _get_soup(url)
    except requests.RequestException as e:
        print(f"  [FORMS] Failed to fetch courses: {e}")
        return []
    titles = soup.select(
        ".nectar-post-grid .nectar-post-grid-item .content .post-heading"
    )
    courses = [_transform_title(t.text.strip()) for t in titles if t.text.strip()]
    print(f"  [FORMS] Scraped {len(courses)} courses from website.")
    return courses


# ============================================================
# HUBSPOT API
# ============================================================

def _hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _get_hubspot_form(form_id):
    """Fetch a HubSpot form by ID."""
    url = f"https://api.hubapi.com/forms/v2/forms/{form_id}"
    resp = requests.get(url, headers=_hubspot_headers(), timeout=15)
    if resp.status_code == 200:
        return resp.json()
    print(f"  [FORMS] Failed to get form {form_id}: HTTP {resp.status_code}")
    return None


def _update_hubspot_form(form_id, form_data):
    """Update a HubSpot form."""
    url = f"https://api.hubapi.com/forms/v2/forms/{form_id}"
    resp = requests.put(url, headers=_hubspot_headers(), json=form_data, timeout=15)
    if resp.status_code == 200:
        return True
    print(f"  [FORMS] Failed to update form {form_id}: HTTP {resp.status_code} - {resp.text[:200]}")
    return False


def _build_options(item_names):
    """Build HubSpot option list from item names."""
    return [{"label": name, "value": name} for name in item_names]


def read_current_options_from_hubspot(form_key):
    """
    Read current options from a HubSpot form.
    Returns dict {"events": [names], "courses": [names]}.
    """
    defn = FORM_DEFINITIONS.get(form_key)
    if not defn:
        return {"events": [], "courses": []}

    form_data = _get_hubspot_form(defn["form_id"])
    if not form_data:
        return {"events": [], "courses": []}

    if defn["update_mode"] == "direct":
        field_name = defn["field_name"]
        names = []
        for group in form_data.get("formFieldGroups", []):
            for field in group.get("fields", []):
                if field["name"] == field_name:
                    for opt in field.get("options", []):
                        val = opt.get("value", "").strip()
                        if val:
                            names.append(val)

        if defn["item_types"] == "events":
            return {"events": names, "courses": []}
        elif defn["item_types"] == "courses":
            return {"events": [], "courses": names}
        else:
            # "both" in a single field – we can't distinguish, return all as mixed
            return {"events": names, "courses": names}

    elif defn["update_mode"] == "dependent":
        dep_parent = defn["dependent_parent"]
        dep_fields = defn["dependent_fields"]
        result = {"events": [], "courses": []}

        for group in form_data.get("formFieldGroups", []):
            for field in group.get("fields", []):
                if field["name"] == dep_parent:
                    for filt in field.get("dependentFieldFilters", []):
                        dep_field = filt.get("dependentFormField", {})
                        dep_name = dep_field.get("name", "")
                        opts = [
                            o.get("value", "").strip()
                            for o in dep_field.get("options", [])
                            if o.get("value", "").strip()
                        ]
                        if dep_name == dep_fields.get("event"):
                            result["events"] = opts
                        elif dep_name == dep_fields.get("course"):
                            result["courses"] = opts

        return result

    return {"events": [], "courses": []}


def read_all_forms_current_state():
    """
    Read current options from ALL HubSpot forms.
    Returns dict of form_key -> {"events": [names], "courses": [names]}.
    """
    state = {}
    for form_key in FORM_DEFINITIONS:
        print(f"  [FORMS] Reading current state of '{FORM_DEFINITIONS[form_key]['form_name']}'...")
        state[form_key] = read_current_options_from_hubspot(form_key)
        n_events = len(state[form_key]["events"])
        n_courses = len(state[form_key]["courses"])
        print(f"    -> {n_events} events, {n_courses} courses")
    return state


def push_form_to_hubspot(form_key, event_names, course_names):
    """
    Update a single HubSpot form with the given events and/or courses.
    Returns (success: bool, message: str).
    """
    defn = FORM_DEFINITIONS.get(form_key)
    if not defn:
        return False, f"Unknown form key: {form_key}"

    form_id = defn["form_id"]
    form_data = _get_hubspot_form(form_id)
    if not form_data:
        return False, f"Could not fetch form '{defn['form_name']}' from HubSpot"

    if defn["update_mode"] == "direct":
        # Build combined options based on what the form accepts
        options = []
        if defn["item_types"] in ("events", "both"):
            options.extend(_build_options(event_names))
        if defn["item_types"] in ("courses", "both"):
            options.extend(_build_options(course_names))

        field_name = defn["field_name"]
        updated = False
        for group in form_data.get("formFieldGroups", []):
            for field in group.get("fields", []):
                if field["name"] == field_name:
                    field["options"] = options
                    updated = True

        if not updated:
            return False, f"Field '{field_name}' not found in form '{defn['form_name']}'"

    elif defn["update_mode"] == "dependent":
        dep_parent = defn["dependent_parent"]
        dep_fields = defn["dependent_fields"]

        event_options = _build_options(event_names)
        course_options = _build_options(course_names)

        updated_fields = set()
        for group in form_data.get("formFieldGroups", []):
            for field in group.get("fields", []):
                if field["name"] == dep_parent:
                    for filt in field.get("dependentFieldFilters", []):
                        dep_field = filt.get("dependentFormField", {})
                        dep_name = dep_field.get("name", "")
                        if dep_name == dep_fields.get("event"):
                            dep_field["options"] = event_options
                            updated_fields.add("event")
                        elif dep_name == dep_fields.get("course"):
                            dep_field["options"] = course_options
                            updated_fields.add("course")

        if not updated_fields:
            return False, f"Dependent fields not found in form '{defn['form_name']}'"

    success = _update_hubspot_form(form_id, form_data)
    if success:
        return True, f"Form '{defn['form_name']}' updated successfully"
    return False, f"Failed to push update to '{defn['form_name']}'"


def push_all_forms(assignments_by_form):
    """
    Push all form updates to HubSpot.
    assignments_by_form: dict of form_key -> {"events": [names], "courses": [names]}
    Returns list of (form_key, success, message).
    """
    results = []
    for form_key, defn in FORM_DEFINITIONS.items():
        data = assignments_by_form.get(form_key, {"events": [], "courses": []})
        event_names = data.get("events", [])
        course_names = data.get("courses", [])
        success, msg = push_form_to_hubspot(form_key, event_names, course_names)
        results.append((form_key, success, msg))
        print(f"  [FORMS] {defn['form_name']}: {'OK' if success else 'FAILED'} - {msg}")
    return results
