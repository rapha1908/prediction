"""
Authentication module using JWT tokens stored in cookies.
Users are stored in PostgreSQL (managed via Settings page).
On first run, users are migrated from the DASHBOARD_USERS env var.
"""

import os
import json
from datetime import datetime, timedelta, timezone

import jwt
import bcrypt
from flask import request, redirect, make_response, jsonify, g
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

JWT_SECRET = os.getenv("JWT_SECRET", "tcche-dashboard-secret-change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "72"))
COOKIE_NAME = "tcche_auth"


# ============================================================
# PASSWORD HELPERS
# ============================================================

def hash_password(plain: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _check_password(plain: str, hashed: str) -> bool:
    """Verify a password against its bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ============================================================
# JWT TOKEN MANAGEMENT
# ============================================================

def create_token(username: str, role_name: str = "") -> str:
    """Create a JWT token including username and role."""
    payload = {
        "sub": username,
        "role": role_name,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(token: str) -> dict | None:
    """Verify a JWT token and return the payload, or None if invalid."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub"):
            return payload
    except jwt.ExpiredSignatureError:
        pass
    except jwt.InvalidTokenError:
        pass
    return None


# ============================================================
# AUTHENTICATION (DB-based)
# ============================================================

def authenticate(username: str, password: str) -> str | None:
    """
    Validate credentials against the DB and return a JWT token if valid.
    Returns None if authentication fails.
    """
    import db

    username = username.strip().lower()
    user = db.load_user_by_username(username)
    if not user or not user["is_active"]:
        return None
    if _check_password(password, user["password_hash"]):
        db.update_last_login(username)
        return create_token(username, user.get("role_name", ""))
    return None


def get_current_user() -> str | None:
    """Get the current authenticated username from the request cookie."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    payload = verify_token(token)
    if not payload:
        return None
    username = payload.get("sub")
    if not username:
        return None
    # Verify user still exists and is active in DB
    import db
    user = db.load_user_by_username(username)
    if not user or not user["is_active"]:
        return None
    return username


def get_current_user_info() -> dict | None:
    """Get full user info dict for the currently authenticated user."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    payload = verify_token(token)
    if not payload:
        return None
    username = payload.get("sub")
    if not username:
        return None
    import db
    return db.load_user_by_username(username)


def get_current_user_permissions() -> set[str]:
    """Get the effective permission set for the current user. Cached per request via Flask g."""
    if hasattr(g, "_user_perms"):
        return g._user_perms
    import db
    user = get_current_user_info()
    if not user:
        g._user_perms = set()
        return g._user_perms
    g._user_perms = db.get_user_permissions(user["id"])
    return g._user_perms


def has_permission(key: str) -> bool:
    """Check if the current user has a specific permission."""
    return key in get_current_user_permissions()


def is_authenticated() -> bool:
    """Check if the current request has a valid auth cookie."""
    return get_current_user() is not None


# ============================================================
# LOGIN PAGE HTML
# ============================================================

LOGIN_PAGE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login – TCCHE Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Outfit', sans-serif;
            background: linear-gradient(135deg, #0b0b14 0%, #13121e 40%, #1a1528 70%, #1e1610 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #f0ebe3;
        }
        .login-card {
            background: #131320;
            border: 1px solid #1f1f32;
            border-radius: 16px;
            padding: 48px 40px;
            width: 100%;
            max-width: 400px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        }
        .brand {
            color: #c8a44e;
            font-size: 11px;
            letter-spacing: 3px;
            text-transform: uppercase;
            font-weight: 600;
            margin-bottom: 8px;
        }
        h1 {
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 6px;
            background: linear-gradient(90deg, #c8a44e, #e0c87a, #b87348);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .subtitle {
            color: #8a847a;
            font-size: 13px;
            margin-bottom: 32px;
        }
        label {
            display: block;
            color: #8a847a;
            font-size: 12px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 6px;
        }
        input[type="text"], input[type="password"] {
            width: 100%;
            padding: 12px 16px;
            background: #0b0b14;
            border: 1px solid #1f1f32;
            border-radius: 8px;
            color: #f0ebe3;
            font-family: 'Outfit', sans-serif;
            font-size: 14px;
            outline: none;
            margin-bottom: 20px;
            transition: border-color 0.2s;
        }
        input:focus {
            border-color: #c8a44e;
        }
        button {
            width: 100%;
            padding: 14px;
            background: linear-gradient(90deg, #c8a44e, #b87348);
            border: none;
            border-radius: 8px;
            color: #0b0b14;
            font-family: 'Outfit', sans-serif;
            font-size: 14px;
            font-weight: 700;
            letter-spacing: 0.5px;
            cursor: pointer;
            transition: opacity 0.2s;
        }
        button:hover { opacity: 0.9; }
        .error {
            background: rgba(212, 74, 74, 0.15);
            border: 1px solid rgba(212, 74, 74, 0.3);
            border-radius: 8px;
            padding: 10px 14px;
            color: #e05555;
            font-size: 13px;
            margin-bottom: 20px;
            display: {error_display};
        }
    </style>
</head>
<body>
    <div class="login-card">
        <p class="brand">TCCHE</p>
        <h1>Sales Dashboard</h1>
        <p class="subtitle">Sign in to access the dashboard</p>
        <div class="error">{error_message}</div>
        <form method="POST" action="/login">
            <label>Username</label>
            <input type="text" name="username" placeholder="Enter your username" autofocus required>
            <label>Password</label>
            <input type="password" name="password" placeholder="Enter your password" required>
            <button type="submit">Sign In</button>
        </form>
    </div>
</body>
</html>
"""


def render_login_page(error: str = "") -> str:
    """Render the login page HTML."""
    html = LOGIN_PAGE_HTML
    html = html.replace("{error_display}", "block" if error else "none")
    html = html.replace("{error_message}", error or "")
    return html


# ============================================================
# FLASK ROUTE SETUP
# ============================================================

def setup_auth(app_server):
    """
    Add authentication routes, middleware, and admin API to the Flask server.
    Call this AFTER creating the Dash app.
    """
    app_server.secret_key = JWT_SECRET

    # --- Seed DB users on first run ---
    import db
    db.seed_default_roles_and_users()

    @app_server.before_request
    def require_auth():
        """Redirect unauthenticated users to login page."""
        allowed = ("/login", "/_dash-", "/assets/", "/_favicon.ico", "/api/")
        if any(request.path.startswith(p) for p in allowed):
            # API routes handle their own auth
            return None
        if not is_authenticated():
            return redirect("/login")
        return None

    @app_server.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "GET":
            if is_authenticated():
                return redirect("/")
            return render_login_page()

        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        token = authenticate(username, password)
        if token:
            response = make_response(redirect("/"))
            response.set_cookie(
                COOKIE_NAME, token,
                httponly=True,
                secure=request.is_secure,
                samesite="Lax",
                max_age=JWT_EXPIRY_HOURS * 3600,
            )
            return response

        return render_login_page("Invalid username or password"), 401

    @app_server.route("/logout")
    def logout():
        response = make_response(redirect("/login"))
        response.delete_cookie(COOKIE_NAME)
        return response

    # ============================================================
    # ADMIN API ENDPOINTS
    # ============================================================

    def _require_api_auth():
        """Return error response if not authenticated, else None."""
        if not is_authenticated():
            return jsonify({"error": "Not authenticated"}), 401
        return None

    def _require_settings_access():
        """Return error response if user lacks page:settings permission."""
        err = _require_api_auth()
        if err:
            return err
        if not has_permission("page:settings"):
            return jsonify({"error": "Access denied"}), 403
        return None

    # --- User info endpoint (for Dash to fetch permissions) ---

    @app_server.route("/api/me", methods=["GET"])
    def api_me():
        err = _require_api_auth()
        if err:
            return err
        user = get_current_user_info()
        if not user:
            return jsonify({"error": "User not found"}), 404
        perms = sorted(get_current_user_permissions())
        return jsonify({
            "id": user["id"],
            "username": user["username"],
            "display_name": user["display_name"],
            "role_id": user["role_id"],
            "role_name": user["role_name"],
            "permissions": perms,
        })

    # --- Users CRUD ---

    @app_server.route("/api/users", methods=["GET"])
    def api_list_users():
        err = _require_settings_access()
        if err:
            return err
        return jsonify(db.list_users())

    @app_server.route("/api/users", methods=["POST"])
    def api_create_user():
        err = _require_settings_access()
        if err:
            return err
        data = request.get_json(force=True)
        username = data.get("username", "").strip().lower()
        password = data.get("password", "")
        display_name = data.get("display_name", "")
        role_id = data.get("role_id")
        if not username or not password:
            return jsonify({"error": "Username and password are required"}), 400
        if len(password) < 4:
            return jsonify({"error": "Password must be at least 4 characters"}), 400
        existing = db.load_user_by_username(username)
        if existing:
            return jsonify({"error": f"User '{username}' already exists"}), 409
        pw_hash = hash_password(password)
        uid = db.create_user(username, pw_hash, display_name, role_id)
        return jsonify({"id": uid, "username": username}), 201

    @app_server.route("/api/users/<int:user_id>", methods=["PUT"])
    def api_update_user(user_id):
        err = _require_settings_access()
        if err:
            return err
        data = request.get_json(force=True)
        kwargs = {}
        if "display_name" in data:
            kwargs["display_name"] = data["display_name"]
        if "role_id" in data:
            kwargs["role_id"] = data["role_id"]
        if "is_active" in data:
            kwargs["is_active"] = bool(data["is_active"])
        if "password" in data and data["password"]:
            kwargs["password_hash"] = hash_password(data["password"])
        if kwargs:
            db.update_user(user_id, **kwargs)
        # Handle per-user overrides
        if "overrides" in data:
            db.set_user_overrides(user_id, data["overrides"])
        return jsonify({"ok": True})

    @app_server.route("/api/users/<int:user_id>", methods=["DELETE"])
    def api_delete_user(user_id):
        err = _require_settings_access()
        if err:
            return err
        current = get_current_user_info()
        if current and current["id"] == user_id:
            return jsonify({"error": "Cannot delete yourself"}), 400
        db.delete_user(user_id)
        return jsonify({"ok": True})

    @app_server.route("/api/users/<int:user_id>/overrides", methods=["GET"])
    def api_get_user_overrides(user_id):
        err = _require_settings_access()
        if err:
            return err
        return jsonify(db.get_user_overrides(user_id))

    # --- Roles CRUD ---

    @app_server.route("/api/roles", methods=["GET"])
    def api_list_roles():
        err = _require_api_auth()
        if err:
            return err
        return jsonify(db.list_roles())

    @app_server.route("/api/roles", methods=["POST"])
    def api_create_role():
        err = _require_settings_access()
        if err:
            return err
        data = request.get_json(force=True)
        name = data.get("name", "").strip()
        if not name:
            return jsonify({"error": "Role name is required"}), 400
        description = data.get("description", "")
        permissions = data.get("permissions", [])
        rid = db.create_role(name, description, permissions)
        return jsonify({"id": rid, "name": name}), 201

    @app_server.route("/api/roles/<int:role_id>", methods=["PUT"])
    def api_update_role(role_id):
        err = _require_settings_access()
        if err:
            return err
        data = request.get_json(force=True)
        if "name" in data or "description" in data:
            db.update_role(
                role_id,
                name=data.get("name"),
                description=data.get("description"),
            )
        if "permissions" in data:
            db.set_role_permissions(role_id, data["permissions"])
        return jsonify({"ok": True})

    @app_server.route("/api/roles/<int:role_id>", methods=["DELETE"])
    def api_delete_role(role_id):
        err = _require_settings_access()
        if err:
            return err
        db.delete_role(role_id)
        return jsonify({"ok": True})

    # --- My Account ---

    @app_server.route("/api/me/password", methods=["PUT"])
    def api_change_password():
        err = _require_api_auth()
        if err:
            return err
        user = get_current_user_info()
        if not user:
            return jsonify({"error": "User not found"}), 404
        data = request.get_json(force=True)
        current_pw = data.get("current_password", "")
        new_pw = data.get("new_password", "")
        if not current_pw or not new_pw:
            return jsonify({"error": "Both current and new password are required"}), 400
        if len(new_pw) < 4:
            return jsonify({"error": "New password must be at least 4 characters"}), 400
        if not _check_password(current_pw, user["password_hash"]):
            return jsonify({"error": "Current password is incorrect"}), 403
        db.update_user(user["id"], password_hash=hash_password(new_pw))
        return jsonify({"ok": True})

    # --- Permissions list ---

    @app_server.route("/api/permissions", methods=["GET"])
    def api_list_permissions():
        err = _require_api_auth()
        if err:
            return err
        return jsonify([
            {"key": k, "label": lbl, "category": cat}
            for k, lbl, cat in db.ALL_PERMISSIONS
        ])
