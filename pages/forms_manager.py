"""Forms Manager page – HubSpot forms assignment layout and callbacks."""

from dash import html, dcc, callback, Output, Input, State, no_update, ctx, ALL
import hubspot_forms as _hf

from config import COLORS, FONT, card_style, section_label

_FORM_KEYS_ORDERED = list(_hf.FORM_DEFINITIONS.keys())
_FORM_NAMES = {k: d["form_name"] for k, d in _hf.FORM_DEFINITIONS.items()}


def layout():
    """Return forms-manager page layout (children of #forms-page)."""
    return [
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "28px"}, children=[
            html.Div(children=[
                dcc.Link("< Back to Dashboard", href="/", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "textDecoration": "none",
                    "marginBottom": "8px", "display": "block",
                }),
                section_label("FORMS MANAGER"),
                html.H2("HubSpot Forms – Events & Courses", style={
                    "margin": "0", "fontSize": "24px", "fontWeight": "700",
                    "background": "linear-gradient(90deg, #c8a44e, #e0c87a, #b87348)",
                    "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                }),
                html.P("Control which events and courses appear in each HubSpot form.",
                       style={"color": COLORS["text_muted"], "fontSize": "14px", "margin": "4px 0 0"}),
            ]),
            html.Div(style={"display": "flex", "gap": "10px"}, children=[
                html.Button("Fetch Events & Courses", id="forms-fetch-btn", n_clicks=0, style={
                    "backgroundColor": COLORS["accent3"], "color": "#fff", "border": "none",
                    "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
                    "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                }),
                html.Button("Sync from HubSpot", id="forms-sync-hs-btn", n_clicks=0, style={
                    "backgroundColor": "#4A90D9", "color": "#fff", "border": "none",
                    "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
                    "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                }),
                html.Button("Push to HubSpot", id="forms-push-btn", n_clicks=0, style={
                    "backgroundColor": COLORS["accent"], "color": "#fff", "border": "none",
                    "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
                    "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
                }),
            ]),
        ]),
        html.Div(id="forms-feedback", style={"marginBottom": "16px"}),
        dcc.Store(id="forms-refresh", data=0),

        # --- ASSIGNMENTS TABLE ---
        html.Div(style=card_style({}), children=[
            section_label("EVENTS & COURSES ASSIGNMENT"),
            html.P(
                "Toggle checkboxes to control which items appear in each form. "
                "Click 'Fetch Events & Courses' to check for new items, then 'Push to HubSpot' to apply changes.",
                style={"color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px"},
            ),
            html.Div(id="forms-assignment-table", style={"overflowX": "auto"}),
        ]),
    ]


# ── Callbacks ──

@callback(
    Output("forms-assignment-table", "children"),
    Input("url", "pathname"),
    Input("forms-refresh", "data"),
)
def render_forms_assignment_table(pathname, _refresh):
    if pathname != "/forms":
        return no_update
    try:
        import db as _db_mod

        items_df = _db_mod.load_form_items()
        if items_df.empty:
            return html.Div(
                style={"textAlign": "center", "padding": "48px"},
                children=[
                    html.P("No events or courses loaded yet.", style={
                        "color": COLORS["text_muted"], "fontSize": "16px", "marginBottom": "12px",
                    }),
                    html.P('Click "Fetch Events & Courses" to scan the website.', style={
                        "color": COLORS["text_muted"], "fontSize": "13px",
                    }),
                ],
            )

        _db_mod.ensure_assignments_for_all_forms(_FORM_KEYS_ORDERED)
        assignments_df = _db_mod.load_form_assignments()

        assignment_map = {}
        for _, row in assignments_df.iterrows():
            assignment_map[(row["form_key"], row["item_id"])] = row["enabled"]

        header_style = {
            "padding": "12px 16px", "fontWeight": "700", "fontSize": "12px",
            "textTransform": "uppercase", "letterSpacing": "1px",
            "color": COLORS["accent"], "borderBottom": f"2px solid {COLORS['card_border']}",
            "textAlign": "center", "whiteSpace": "nowrap",
        }
        header_cells = [
            html.Th("Type", style={**header_style, "textAlign": "left", "width": "80px"}),
            html.Th("Event / Course", style={**header_style, "textAlign": "left", "minWidth": "250px"}),
            html.Th("Status", style={**header_style, "width": "80px"}),
        ]
        for fk in _FORM_KEYS_ORDERED:
            header_cells.append(html.Th(
                _FORM_NAMES[fk].replace(" Form", ""),
                style={**header_style, "width": "110px"},
            ))
        header = html.Thead(html.Tr(header_cells))

        rows = []
        for _, item in items_df.iterrows():
            item_id = int(item["id"])
            item_type = item["item_type"]
            is_active = item["active"]

            type_badge_color = "#4A90D9" if item_type == "event" else "#9b59b6"
            type_label = "Event" if item_type == "event" else "Course"

            status_color = "#50b560" if is_active else COLORS["text_muted"]
            status_label = "Active" if is_active else "Inactive"

            cells = [
                html.Td(
                    html.Span(type_label, style={
                        "backgroundColor": type_badge_color, "color": "#fff",
                        "padding": "3px 10px", "borderRadius": "12px", "fontSize": "11px",
                        "fontWeight": "600",
                    }),
                    style={"padding": "10px 16px"},
                ),
                html.Td(
                    item["name"],
                    style={
                        "padding": "10px 16px", "fontSize": "13px", "fontWeight": "500",
                        "color": COLORS["text"] if is_active else COLORS["text_muted"],
                    },
                ),
                html.Td(
                    html.Span(status_label, style={"color": status_color, "fontSize": "12px", "fontWeight": "600"}),
                    style={"padding": "10px 16px", "textAlign": "center"},
                ),
            ]

            for fk in _FORM_KEYS_ORDERED:
                defn = _hf.FORM_DEFINITIONS[fk]
                accepts = defn["item_types"]
                form_accepts_item = (
                    (accepts == "both") or
                    (accepts == "events" and item_type == "event") or
                    (accepts == "courses" and item_type == "course")
                )

                if form_accepts_item:
                    is_enabled = assignment_map.get((fk, item_id), False)
                    cells.append(html.Td(
                        dcc.Checklist(
                            id={"type": "form-toggle", "form": fk, "item": item_id},
                            options=[{"label": "", "value": "on"}],
                            value=["on"] if is_enabled else [],
                            style={"display": "flex", "justifyContent": "center"},
                            inputStyle={"cursor": "pointer", "width": "18px", "height": "18px"},
                        ),
                        style={"padding": "10px 16px", "textAlign": "center"},
                    ))
                else:
                    cells.append(html.Td(
                        html.Span("—", style={"color": COLORS["text_muted"], "fontSize": "12px"}),
                        style={"padding": "10px 16px", "textAlign": "center"},
                    ))

            row_bg = "rgba(255,255,255,0.02)" if len(rows) % 2 == 0 else "transparent"
            rows.append(html.Tr(cells, style={
                "backgroundColor": row_bg,
                "borderBottom": f"1px solid {COLORS['card_border']}",
            }))

        body = html.Tbody(rows)

        n_events = len(items_df[items_df["item_type"] == "event"])
        n_courses = len(items_df[items_df["item_type"] == "course"])
        n_active = len(items_df[items_df["active"]])

        summary = html.Div(style={"display": "flex", "gap": "16px", "marginBottom": "16px"}, children=[
            html.Span(f"{n_events} Events", style={
                "backgroundColor": "#4A90D9", "color": "#fff", "padding": "4px 14px",
                "borderRadius": "12px", "fontSize": "12px", "fontWeight": "600",
            }),
            html.Span(f"{n_courses} Courses", style={
                "backgroundColor": "#9b59b6", "color": "#fff", "padding": "4px 14px",
                "borderRadius": "12px", "fontSize": "12px", "fontWeight": "600",
            }),
            html.Span(f"{n_active} Active", style={
                "backgroundColor": "#50b560", "color": "#fff", "padding": "4px 14px",
                "borderRadius": "12px", "fontSize": "12px", "fontWeight": "600",
            }),
        ])

        return html.Div([
            summary,
            html.Table(
                [header, body],
                style={
                    "width": "100%", "borderCollapse": "collapse",
                    "fontSize": "13px",
                },
            ),
        ])
    except Exception as e:
        import traceback
        traceback.print_exc()
        return html.Span(f"Error loading assignments: {e}", style={"color": "#e05555", "fontSize": "13px"})


@callback(
    Output("forms-refresh", "data", allow_duplicate=True),
    Input({"type": "form-toggle", "form": ALL, "item": ALL}, "value"),
    State("forms-refresh", "data"),
    prevent_initial_call=True,
)
def handle_form_toggle(values, current_refresh):
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update

    triggered = ctx.triggered_id
    form_key = triggered["form"]
    item_id = triggered["item"]

    trigger_idx = None
    for i, inp in enumerate(ctx.inputs_list[0]):
        if inp["id"]["form"] == form_key and inp["id"]["item"] == item_id:
            trigger_idx = i
            break

    if trigger_idx is None:
        return no_update

    new_value = values[trigger_idx]
    enabled = "on" in new_value if new_value else False

    try:
        import db as _db_mod
        _db_mod.set_assignment_enabled(form_key, item_id, enabled)
    except Exception as e:
        print(f"  [FORMS] Error toggling assignment: {e}")

    return no_update


@callback(
    Output("forms-feedback", "children"),
    Output("forms-refresh", "data", allow_duplicate=True),
    Input("forms-fetch-btn", "n_clicks"),
    State("forms-refresh", "data"),
    prevent_initial_call=True,
)
def fetch_events_and_courses(n_clicks, current_refresh):
    if not n_clicks:
        return no_update, no_update

    try:
        events = _hf.scrape_events()
        courses = _hf.scrape_courses()

        items = []
        for name in events:
            items.append({"name": name, "item_type": "event"})
        for name in courses:
            items.append({"name": name, "item_type": "course"})

        if not items:
            return html.Span(
                "No events or courses found on the website.",
                style={"color": "#e05555", "fontSize": "13px"},
            ), no_update

        import db as _db_mod
        new_count, updated_count = _db_mod.upsert_form_items(items)

        all_current_names = [i["name"] for i in items]
        _db_mod.deactivate_missing_items(all_current_names)

        _db_mod.ensure_assignments_for_all_forms(_FORM_KEYS_ORDERED)

        synced_from_hs = False
        if not _db_mod.has_any_assignments() or new_count == len(items):
            print("  [FORMS] First fetch detected – reading current state from HubSpot...")
            hs_state = _hf.read_all_forms_current_state()
            _db_mod.sync_assignments_from_hubspot(hs_state)
            synced_from_hs = True

        msg_parts = []
        if new_count > 0:
            msg_parts.append(f"{new_count} new item(s) found")
        if updated_count > 0:
            msg_parts.append(f"{updated_count} existing item(s) updated")
        msg = ". ".join(msg_parts) + f". Total: {len(events)} events, {len(courses)} courses."
        if synced_from_hs:
            msg += " Checkboxes synced from current HubSpot state."

        feedback = html.Div(
            style=card_style({"padding": "12px 18px", "borderLeft": "3px solid #50b560"}),
            children=[
                html.Span("Fetch completed! ", style={"fontWeight": "700", "color": "#50b560"}),
                html.Span(msg, style={"color": COLORS["text_muted"], "fontSize": "13px"}),
            ],
        )
        return feedback, (current_refresh or 0) + 1

    except Exception as e:
        import traceback
        traceback.print_exc()
        return html.Span(f"Error: {e}", style={"color": "#e05555", "fontSize": "13px"}), no_update


@callback(
    Output("forms-feedback", "children", allow_duplicate=True),
    Input("forms-push-btn", "n_clicks"),
    prevent_initial_call=True,
)
def push_forms_to_hubspot(n_clicks):
    if not n_clicks:
        return no_update

    try:
        import db as _db_mod

        assignments_by_form = {}
        for fk in _FORM_KEYS_ORDERED:
            enabled_df = _db_mod.get_enabled_items_for_form(fk)
            events = enabled_df[enabled_df["item_type"] == "event"]["name"].tolist()
            courses = enabled_df[enabled_df["item_type"] == "course"]["name"].tolist()
            assignments_by_form[fk] = {"events": events, "courses": courses}

        results = _hf.push_all_forms(assignments_by_form)

        items = []
        all_ok = True
        for form_key, success, msg in results:
            color = "#50b560" if success else "#e05555"
            icon = "✓" if success else "✗"
            if not success:
                all_ok = False
            items.append(html.Div(
                f"{icon} {msg}",
                style={"color": color, "fontSize": "13px", "marginBottom": "4px"},
            ))

        border_color = "#50b560" if all_ok else "#e05555"
        header_text = "All forms updated successfully!" if all_ok else "Some forms failed to update."
        header_color = "#50b560" if all_ok else "#e05555"

        return html.Div(
            style=card_style({"padding": "12px 18px", "borderLeft": f"3px solid {border_color}"}),
            children=[
                html.Span(header_text, style={
                    "fontWeight": "700", "color": header_color,
                    "display": "block", "marginBottom": "8px",
                }),
                *items,
            ],
        )
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": "#e05555", "fontSize": "13px"})


@callback(
    Output("forms-feedback", "children", allow_duplicate=True),
    Output("forms-refresh", "data", allow_duplicate=True),
    Input("forms-sync-hs-btn", "n_clicks"),
    State("forms-refresh", "data"),
    prevent_initial_call=True,
)
def sync_from_hubspot(n_clicks, current_refresh):
    if not n_clicks:
        return no_update, no_update

    try:
        import db as _db_mod

        items_df = _db_mod.load_form_items()
        if items_df.empty:
            return html.Span(
                'No items in database. Click "Fetch Events & Courses" first.',
                style={"color": "#e05555", "fontSize": "13px"},
            ), no_update

        hs_state = _hf.read_all_forms_current_state()
        count = _db_mod.sync_assignments_from_hubspot(hs_state)

        form_details = []
        for fk in _FORM_KEYS_ORDERED:
            data = hs_state.get(fk, {})
            n_e = len(data.get("events", []))
            n_c = len(data.get("courses", []))
            name = _FORM_NAMES[fk].replace(" Form", "")
            form_details.append(f"{name}: {n_e} events, {n_c} courses")

        feedback = html.Div(
            style=card_style({"padding": "12px 18px", "borderLeft": "3px solid #4A90D9"}),
            children=[
                html.Span("Synced from HubSpot! ", style={"fontWeight": "700", "color": "#4A90D9"}),
                html.Span(f"Updated {count} assignments. ", style={"color": COLORS["text_muted"], "fontSize": "13px"}),
                html.Div(style={"marginTop": "8px"}, children=[
                    html.Div(detail, style={"color": COLORS["text_muted"], "fontSize": "12px", "marginBottom": "2px"})
                    for detail in form_details
                ]),
            ],
        )
        return feedback, (current_refresh or 0) + 1

    except Exception as e:
        import traceback
        traceback.print_exc()
        return html.Span(f"Error: {e}", style={"color": "#e05555", "fontSize": "13px"}), no_update
