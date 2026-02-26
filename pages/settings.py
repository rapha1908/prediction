from config import COLORS, FONT, card_style, section_label, _th_style, _td_style
from dash import html, dcc, callback, Output, Input, State, no_update, ctx, ALL


def layout():
    return [
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "28px"}, children=[
            html.Div(children=[
                dcc.Link("< Back to Dashboard", href="/", style={
                    "color": COLORS["text_muted"], "fontSize": "13px", "textDecoration": "none",
                    "marginBottom": "8px", "display": "block",
                }),
                section_label("SETTINGS"),
                html.H2("Users & Access Control", style={
                    "margin": "0", "fontSize": "24px", "fontWeight": "700",
                    "background": "linear-gradient(90deg, #c8a44e, #e0c87a, #b87348)",
                    "WebkitBackgroundClip": "text", "WebkitTextFillColor": "transparent",
                }),
                html.P("Manage users, roles, and permissions.",
                       style={"color": COLORS["text_muted"], "fontSize": "14px", "margin": "4px 0 0"}),
            ]),
        ]),
        dcc.Store(id="settings-refresh", data=0),
        dcc.Tabs(id="settings-tabs", value="users", style={
            "marginBottom": "24px",
        }, colors={
            "border": COLORS["card_border"],
            "primary": COLORS["accent"],
            "background": COLORS["bg"],
        }, children=[
            dcc.Tab(label="Users", value="users", style={
                "backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                "padding": "10px 24px", "fontFamily": FONT, "fontWeight": "600", "fontSize": "13px",
            }, selected_style={
                "backgroundColor": COLORS["card"], "color": COLORS["accent"],
                "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                "borderRadius": "8px 8px 0 0", "padding": "10px 24px",
                "fontFamily": FONT, "fontWeight": "700", "fontSize": "13px",
            }),
            dcc.Tab(label="Roles & Permissions", value="roles", style={
                "backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                "padding": "10px 24px", "fontFamily": FONT, "fontWeight": "600", "fontSize": "13px",
            }, selected_style={
                "backgroundColor": COLORS["card"], "color": COLORS["accent"],
                "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                "borderRadius": "8px 8px 0 0", "padding": "10px 24px",
                "fontFamily": FONT, "fontWeight": "700", "fontSize": "13px",
            }),
            dcc.Tab(label="My Account", value="account", style={
                "backgroundColor": COLORS["bg"], "color": COLORS["text_muted"],
                "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px 8px 0 0",
                "padding": "10px 24px", "fontFamily": FONT, "fontWeight": "600", "fontSize": "13px",
            }, selected_style={
                "backgroundColor": COLORS["card"], "color": COLORS["accent"],
                "border": f"1px solid {COLORS['card_border']}", "borderBottom": "none",
                "borderRadius": "8px 8px 0 0", "padding": "10px 24px",
                "fontFamily": FONT, "fontWeight": "700", "fontSize": "13px",
            }),
        ]),

        # ---- USERS TAB CONTENT ----
        html.Div(id="settings-users-tab", children=[
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                section_label("ADD NEW USER"),
                html.Div(style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "alignItems": "flex-end"}, children=[
                    html.Div(style={"flex": "1", "minWidth": "150px"}, children=[
                        html.Label("Username", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-username", type="text", placeholder="username",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(style={"flex": "1", "minWidth": "150px"}, children=[
                        html.Label("Display Name", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-displayname", type="text", placeholder="Display Name",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(style={"flex": "1", "minWidth": "150px"}, children=[
                        html.Label("Password", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-password", type="password", placeholder="password",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(style={"flex": "1", "minWidth": "150px"}, children=[
                        html.Label("Role", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Dropdown(id="settings-new-role", placeholder="Select role",
                                     style={"backgroundColor": COLORS["bg"], "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"},
                                     className="settings-dropdown"),
                    ]),
                    html.Button("Add User", id="settings-add-user-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent3"], "color": "#fff", "border": "none",
                        "borderRadius": "8px", "padding": "10px 24px", "fontSize": "13px",
                        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT, "height": "40px",
                    }),
                ]),
                html.Div(id="settings-add-user-feedback", style={"marginTop": "10px"}),
            ]),
            html.Div(style=card_style({}), children=[
                section_label("ALL USERS"),
                html.Div(id="settings-users-table", style={"overflowX": "auto"}),
            ]),
        ]),

        # ---- ROLES TAB CONTENT ----
        html.Div(id="settings-roles-tab", style={"display": "none"}, children=[
            html.Div(style=card_style({"marginBottom": "24px"}), children=[
                section_label("ADD NEW ROLE"),
                html.Div(style={"display": "flex", "gap": "12px", "alignItems": "flex-end"}, children=[
                    html.Div(style={"flex": "1", "minWidth": "200px"}, children=[
                        html.Label("Role Name", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-role-name", type="text", placeholder="e.g. editor",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(style={"flex": "2", "minWidth": "200px"}, children=[
                        html.Label("Description", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-role-desc", type="text", placeholder="Short description",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Button("Add Role", id="settings-add-role-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent3"], "color": "#fff", "border": "none",
                        "borderRadius": "8px", "padding": "10px 24px", "fontSize": "13px",
                        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT, "height": "40px",
                    }),
                ]),
                html.Div(id="settings-add-role-feedback", style={"marginTop": "10px"}),
            ]),
            html.Div(style=card_style({}), children=[
                section_label("ROLES & PERMISSION MATRIX"),
                html.P("Toggle permissions for each role. Changes are saved immediately.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px", "marginBottom": "16px"}),
                html.Div(id="settings-roles-table", style={"overflowX": "auto"}),
            ]),
        ]),

        # ---- MY ACCOUNT TAB CONTENT ----
        html.Div(id="settings-account-tab", style={"display": "none"}, children=[
            html.Div(style=card_style({"maxWidth": "500px"}), children=[
                section_label("CHANGE PASSWORD"),
                html.Div(style={"display": "flex", "flexDirection": "column", "gap": "16px"}, children=[
                    html.Div(children=[
                        html.Label("Current Password", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-current-pw", type="password", placeholder="Enter current password",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(children=[
                        html.Label("New Password", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-new-pw", type="password", placeholder="Enter new password",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Div(children=[
                        html.Label("Confirm New Password", style={"color": COLORS["text_muted"], "fontSize": "11px", "textTransform": "uppercase", "letterSpacing": "1px", "marginBottom": "4px", "display": "block"}),
                        dcc.Input(id="settings-confirm-pw", type="password", placeholder="Confirm new password",
                                  style={"width": "100%", "padding": "10px 14px", "background": COLORS["bg"], "border": f"1px solid {COLORS['card_border']}",
                                         "borderRadius": "8px", "color": COLORS["text"], "fontFamily": FONT, "fontSize": "13px"}),
                    ]),
                    html.Button("Change Password", id="settings-change-pw-btn", n_clicks=0, style={
                        "backgroundColor": COLORS["accent"], "color": "#0b0b14", "border": "none",
                        "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
                        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT, "alignSelf": "flex-start",
                    }),
                    html.Div(id="settings-pw-feedback"),
                ]),
            ]),
        ]),
    ]


@callback(
    Output("current-user-perms", "data"),
    Input("url", "pathname"),
)
def load_user_permissions(_pathname):
    """Fetch current user permissions from the API on every navigation."""
    try:
        import auth
        from flask import has_request_context
        if has_request_context():
            perms = auth.get_current_user_permissions()
            return sorted(perms)
    except Exception:
        pass
    return []


@callback(
    Output("header-stock-link", "style"),
    Output("header-forms-link", "style"),
    Output("header-crosssell-link", "style"),
    Output("header-settings-link", "style"),
    Output("sync-btn", "style"),
    Output("sheets-update-btn", "style"),
    Output("chat-section", "style"),
    Output("report-btn", "style"),
    Input("current-user-perms", "data"),
)
def enforce_permissions(perms):
    """Hide/show header links and features based on user permissions."""
    perms = set(perms or [])

    _link_visible = {
        "color": COLORS["accent"], "fontSize": "12px", "textDecoration": "none",
        "border": f"1px solid {COLORS['accent']}", "borderRadius": "8px",
        "padding": "10px 18px", "whiteSpace": "nowrap", "fontFamily": FONT, "fontWeight": "600",
    }
    _link_hidden = {**_link_visible, "display": "none"}

    _settings_visible = {
        "color": COLORS["text_muted"], "fontSize": "12px", "textDecoration": "none",
        "border": f"1px solid {COLORS['card_border']}", "borderRadius": "8px",
        "padding": "10px 18px", "whiteSpace": "nowrap", "fontFamily": FONT, "fontWeight": "600",
    }
    _settings_hidden = {**_settings_visible, "display": "none"}

    _sync_visible = {
        "backgroundColor": COLORS["accent3"], "color": "#fff",
        "border": "none", "borderRadius": "8px",
        "padding": "10px 24px", "fontSize": "13px",
        "fontWeight": "700", "cursor": "pointer",
        "fontFamily": FONT, "letterSpacing": "0.5px", "whiteSpace": "nowrap",
    }
    _sync_hidden = {**_sync_visible, "display": "none"}

    _chat_visible = card_style({"marginBottom": "28px", "borderTop": f"3px solid {COLORS['accent']}"})
    _chat_hidden = {**_chat_visible, "display": "none"}

    _report_visible = {
        "backgroundColor": "transparent", "color": COLORS["accent"],
        "border": f"1px solid {COLORS['accent']}", "borderRadius": "8px",
        "padding": "8px 20px", "fontSize": "12px", "fontWeight": "600",
        "cursor": "pointer", "fontFamily": FONT, "letterSpacing": "0.5px",
        "whiteSpace": "nowrap", "transition": "all 0.2s ease",
    }
    _report_hidden = {**_report_visible, "display": "none"}

    stock_style = _link_visible if "page:stock" in perms else _link_hidden
    forms_style = _link_visible if "page:forms" in perms else _link_hidden
    crosssell_style = _link_visible if "page:crosssell" in perms else _link_hidden
    settings_style = _settings_visible if "page:settings" in perms else _settings_hidden
    sync_style = _sync_visible if "feature:sync" in perms else _sync_hidden
    _sheets_visible = {
        "backgroundColor": "#34A853", "color": "#fff", "border": "none", "borderRadius": "8px",
        "padding": "10px 20px", "fontSize": "13px", "fontWeight": "700", "cursor": "pointer",
        "fontFamily": FONT, "letterSpacing": "0.5px", "whiteSpace": "nowrap",
    }
    _sheets_hidden = {**_sheets_visible, "display": "none"}
    sheets_style = _sheets_visible if "feature:sync" in perms else _sheets_hidden
    chat_style = _chat_visible if "feature:chat" in perms else _chat_hidden
    report_style = _report_visible if "feature:report" in perms else _report_hidden

    return stock_style, forms_style, crosssell_style, settings_style, sync_style, sheets_style, chat_style, report_style


@callback(
    Output("replenish-btn", "style"),
    Input("current-user-perms", "data"),
)
def enforce_replenish_perm(perms):
    perms = set(perms or [])
    base = {
        "backgroundColor": COLORS["accent3"], "color": "#fff", "border": "none",
        "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
    }
    if "feature:stock_replenish" not in perms:
        base["display"] = "none"
    return base


@callback(
    Output("forms-push-btn", "style"),
    Input("current-user-perms", "data"),
)
def enforce_forms_push_perm(perms):
    perms = set(perms or [])
    base = {
        "backgroundColor": COLORS["accent"], "color": "#fff", "border": "none",
        "borderRadius": "8px", "padding": "12px 28px", "fontSize": "14px",
        "fontWeight": "700", "cursor": "pointer", "fontFamily": FONT,
    }
    if "feature:forms_push" not in perms:
        base["display"] = "none"
    return base


@callback(
    Output("settings-users-tab", "style"),
    Output("settings-roles-tab", "style"),
    Output("settings-account-tab", "style"),
    Input("settings-tabs", "value"),
)
def toggle_settings_tabs(tab):
    show = {"display": "block"}
    hide = {"display": "none"}
    if tab == "roles":
        return hide, show, hide
    if tab == "account":
        return hide, hide, show
    return show, hide, hide


@callback(
    Output("settings-new-role", "options"),
    Input("url", "pathname"),
    Input("settings-refresh", "data"),
)
def load_role_options_for_user_form(pathname, _refresh):
    """Populate the role dropdown in the Add User form."""
    if pathname != "/settings":
        return no_update
    try:
        import db
        roles = db.list_roles()
        return [{"label": r["name"].capitalize(), "value": r["id"]} for r in roles]
    except Exception:
        return []


@callback(
    Output("settings-add-user-feedback", "children"),
    Output("settings-refresh", "data"),
    Input("settings-add-user-btn", "n_clicks"),
    State("settings-new-username", "value"),
    State("settings-new-displayname", "value"),
    State("settings-new-password", "value"),
    State("settings-new-role", "value"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def add_user(n_clicks, username, display_name, password, role_id, refresh):
    if not n_clicks:
        return no_update, no_update
    if not username or not password:
        return html.Span("Username and password are required.",
                         style={"color": COLORS["red"], "fontSize": "13px"}), no_update
    try:
        import db
        from auth import hash_password
        existing = db.load_user_by_username(username.strip().lower())
        if existing:
            return html.Span(f"User '{username}' already exists.",
                             style={"color": COLORS["red"], "fontSize": "13px"}), no_update
        pw_hash = hash_password(password)
        db.create_user(username.strip().lower(), pw_hash, display_name or "", role_id)
        return html.Span(f"User '{username}' created successfully.",
                         style={"color": COLORS["accent3"], "fontSize": "13px"}), (refresh or 0) + 1
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": COLORS["red"], "fontSize": "13px"}), no_update


@callback(
    Output("settings-users-table", "children"),
    Input("url", "pathname"),
    Input("settings-refresh", "data"),
)
def render_users_table(pathname, _refresh):
    """Render the users table with edit/delete actions."""
    if pathname != "/settings":
        return no_update
    try:
        import db
        users = db.list_users()
        roles = db.list_roles()
    except Exception as e:
        return html.P(f"Error loading users: {e}", style={"color": COLORS["red"]})

    if not users:
        return html.P("No users found.", style={"color": COLORS["text_muted"]})

    role_map = {r["id"]: r["name"] for r in roles}

    header = html.Tr([
        html.Th("Username", style=_th_style()),
        html.Th("Display Name", style=_th_style()),
        html.Th("Role", style=_th_style()),
        html.Th("Active", style=_th_style()),
        html.Th("Last Login", style=_th_style()),
        html.Th("Actions", style=_th_style()),
    ])

    rows = []
    for u in users:
        uid = u["id"]
        role_name = role_map.get(u["role_id"], "—") if u["role_id"] else "—"
        last_login = u["last_login"][:16] if u["last_login"] else "Never"
        rows.append(html.Tr([
            html.Td(u["username"], style=_td_style()),
            html.Td(u["display_name"] or "—", style=_td_style()),
            html.Td(
                dcc.Dropdown(
                    id={"type": "user-role-dd", "index": uid},
                    options=[{"label": r["name"].capitalize(), "value": r["id"]} for r in roles],
                    value=u["role_id"],
                    clearable=False,
                    style={"width": "140px", "fontSize": "12px", "backgroundColor": COLORS["bg"]},
                    className="settings-dropdown",
                ),
                style=_td_style(),
            ),
            html.Td(
                html.Div(
                    "Active" if u["is_active"] else "Disabled",
                    style={
                        "color": COLORS["accent3"] if u["is_active"] else COLORS["red"],
                        "fontSize": "12px", "fontWeight": "600",
                        "cursor": "pointer", "textDecoration": "underline",
                    },
                    id={"type": "user-toggle-active", "index": uid},
                    **{"data-active": "1" if u["is_active"] else "0"},
                ),
                style=_td_style(),
            ),
            html.Td(last_login, style=_td_style({"color": COLORS["text_muted"]})),
            html.Td(
                html.Button("Delete", id={"type": "user-delete-btn", "index": uid}, n_clicks=0, style={
                    "backgroundColor": "transparent", "color": COLORS["red"], "border": f"1px solid {COLORS['red']}",
                    "borderRadius": "6px", "padding": "4px 14px", "fontSize": "11px",
                    "fontWeight": "600", "cursor": "pointer", "fontFamily": FONT,
                }),
                style=_td_style(),
            ),
        ]))

    return html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )


@callback(
    Output("settings-refresh", "data", allow_duplicate=True),
    Input({"type": "user-role-dd", "index": ALL}, "value"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def change_user_role(role_values, refresh):
    """Update user role when dropdown changes."""
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update
    uid = ctx.triggered_id["index"]
    new_role_id = ctx.triggered[0]["value"]
    try:
        import db
        db.update_user(uid, role_id=new_role_id)
    except Exception:
        pass
    return (refresh or 0) + 1


@callback(
    Output("settings-refresh", "data", allow_duplicate=True),
    Input({"type": "user-toggle-active", "index": ALL}, "n_clicks"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def toggle_user_active(_n_clicks, refresh):
    """Toggle user active/disabled status."""
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update
    uid = ctx.triggered_id["index"]
    try:
        import db
        user_data = db.list_users()
        for u in user_data:
            if u["id"] == uid:
                db.update_user(uid, is_active=not u["is_active"])
                break
    except Exception:
        pass
    return (refresh or 0) + 1


@callback(
    Output("settings-refresh", "data", allow_duplicate=True),
    Input({"type": "user-delete-btn", "index": ALL}, "n_clicks"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def delete_user(_n_clicks, refresh):
    """Delete a user."""
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update
    if not any(n and n > 0 for n in (_n_clicks or [])):
        return no_update
    uid = ctx.triggered_id["index"]
    try:
        import db
        db.delete_user(uid)
    except Exception:
        pass
    return (refresh or 0) + 1


# --- Roles & Permissions tab ---

@callback(
    Output("settings-add-role-feedback", "children"),
    Output("settings-refresh", "data", allow_duplicate=True),
    Input("settings-add-role-btn", "n_clicks"),
    State("settings-new-role-name", "value"),
    State("settings-new-role-desc", "value"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def add_role(n_clicks, name, desc, refresh):
    if not n_clicks or not name:
        return html.Span("Role name is required.",
                         style={"color": COLORS["red"], "fontSize": "13px"}), no_update
    try:
        import db
        db.create_role(name.strip(), desc or "")
        return html.Span(f"Role '{name}' created.",
                         style={"color": COLORS["accent3"], "fontSize": "13px"}), (refresh or 0) + 1
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": COLORS["red"], "fontSize": "13px"}), no_update


@callback(
    Output("settings-roles-table", "children"),
    Input("url", "pathname"),
    Input("settings-refresh", "data"),
)
def render_roles_table(pathname, _refresh):
    """Render the roles table with permission checkboxes."""
    if pathname != "/settings":
        return no_update
    try:
        import db
        roles = db.list_roles()
        all_perms = db.ALL_PERMISSIONS
    except Exception as e:
        return html.P(f"Error loading roles: {e}", style={"color": COLORS["red"]})

    if not roles:
        return html.P("No roles found.", style={"color": COLORS["text_muted"]})

    # Build header: Role Name | Description | each permission | Delete
    header_cells = [
        html.Th("Role", style=_th_style()),
        html.Th("Description", style=_th_style()),
    ]
    for pkey, plabel, pcat in all_perms:
        prefix = "P" if pcat == "page" else "F"
        header_cells.append(html.Th(
            html.Div([
                html.Span(f"[{prefix}] ", style={"color": COLORS["accent"], "fontSize": "9px"}),
                html.Span(plabel, style={"fontSize": "10px"}),
            ]),
            style=_th_style({"textAlign": "center", "minWidth": "80px"}),
        ))
    header_cells.append(html.Th("", style=_th_style()))

    rows = []
    for role in roles:
        rid = role["id"]
        role_perms = set(role.get("permissions", []))
        cells = [
            html.Td(
                html.Span(role["name"].capitalize(), style={"fontWeight": "700"}),
                style=_td_style(),
            ),
            html.Td(
                html.Span(role["description"] or "—", style={"color": COLORS["text_muted"]}),
                style=_td_style(),
            ),
        ]
        for pkey, _plabel, _pcat in all_perms:
            checked = pkey in role_perms
            cells.append(html.Td(
                dcc.Checklist(
                    id={"type": "role-perm-check", "role": rid, "perm": pkey},
                    options=[{"label": "", "value": "on"}],
                    value=["on"] if checked else [],
                    style={"display": "flex", "justifyContent": "center"},
                    inputStyle={"cursor": "pointer", "width": "16px", "height": "16px"},
                ),
                style=_td_style({"textAlign": "center"}),
            ))
        cells.append(html.Td(
            html.Button("Delete", id={"type": "role-delete-btn", "index": rid}, n_clicks=0, style={
                "backgroundColor": "transparent", "color": COLORS["red"], "border": f"1px solid {COLORS['red']}",
                "borderRadius": "6px", "padding": "4px 14px", "fontSize": "11px",
                "fontWeight": "600", "cursor": "pointer", "fontFamily": FONT,
            }),
            style=_td_style(),
        ))
        rows.append(html.Tr(cells))

    return html.Table(
        [html.Thead(html.Tr(header_cells)), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )


@callback(
    Output("settings-refresh", "data", allow_duplicate=True),
    Input({"type": "role-perm-check", "role": ALL, "perm": ALL}, "value"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def toggle_role_permission(check_values, refresh):
    """Toggle a single permission for a role."""
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update
    rid = ctx.triggered_id["role"]
    perm_key = ctx.triggered_id["perm"]
    enabled = bool(ctx.triggered[0]["value"])
    try:
        import db
        roles = db.list_roles()
        for r in roles:
            if r["id"] == rid:
                current = set(r["permissions"])
                if enabled:
                    current.add(perm_key)
                else:
                    current.discard(perm_key)
                db.set_role_permissions(rid, list(current))
                break
    except Exception:
        pass
    return no_update


@callback(
    Output("settings-refresh", "data", allow_duplicate=True),
    Input({"type": "role-delete-btn", "index": ALL}, "n_clicks"),
    State("settings-refresh", "data"),
    prevent_initial_call=True,
)
def delete_role(_n_clicks, refresh):
    """Delete a role."""
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return no_update
    if not any(n and n > 0 for n in (_n_clicks or [])):
        return no_update
    rid = ctx.triggered_id["index"]
    try:
        import db
        db.delete_role(rid)
    except Exception:
        pass
    return (refresh or 0) + 1


# --- My Account tab ---

@callback(
    Output("settings-pw-feedback", "children"),
    Input("settings-change-pw-btn", "n_clicks"),
    State("settings-current-pw", "value"),
    State("settings-new-pw", "value"),
    State("settings-confirm-pw", "value"),
    prevent_initial_call=True,
)
def change_password(n_clicks, current_pw, new_pw, confirm_pw):
    if not n_clicks:
        return no_update
    if not current_pw or not new_pw:
        return html.Span("All fields are required.",
                         style={"color": COLORS["red"], "fontSize": "13px"})
    if new_pw != confirm_pw:
        return html.Span("New passwords do not match.",
                         style={"color": COLORS["red"], "fontSize": "13px"})
    if len(new_pw) < 4:
        return html.Span("New password must be at least 4 characters.",
                         style={"color": COLORS["red"], "fontSize": "13px"})
    try:
        import db
        import auth
        from flask import has_request_context
        if not has_request_context():
            return html.Span("Could not verify session.", style={"color": COLORS["red"], "fontSize": "13px"})
        user = auth.get_current_user_info()
        if not user:
            return html.Span("User not found.", style={"color": COLORS["red"], "fontSize": "13px"})
        if not auth._check_password(current_pw, user["password_hash"]):
            return html.Span("Current password is incorrect.",
                             style={"color": COLORS["red"], "fontSize": "13px"})
        db.update_user(user["id"], password_hash=auth.hash_password(new_pw))
        return html.Span("Password changed successfully.",
                         style={"color": COLORS["accent3"], "fontSize": "13px"})
    except Exception as e:
        return html.Span(f"Error: {e}", style={"color": COLORS["red"], "fontSize": "13px"})
