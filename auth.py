"""
Authentication module using JWT tokens stored in cookies.
Users are defined in DASHBOARD_USERS env var or default config.
"""

import os
import json
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from functools import wraps

import jwt
import bcrypt
from flask import request, redirect, make_response
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

# Secret key for JWT signing (auto-generated if not set)
JWT_SECRET = os.getenv("JWT_SECRET", "tcche-dashboard-secret-change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "72"))  # 3 days default
COOKIE_NAME = "tcche_auth"

# ============================================================
# USER MANAGEMENT
# ============================================================

def _hash_password(plain: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _check_password(plain: str, hashed: str) -> bool:
    """Verify a password against its bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def _load_users() -> dict:
    """
    Load users from DASHBOARD_USERS env var (JSON) or fallback to defaults.

    Format: DASHBOARD_USERS={"admin": "hashed_password", "user2": "hashed_password"}

    For convenience, if the password doesn't start with '$2b$' (bcrypt hash),
    it's treated as plain text and hashed on the fly (useful for initial setup).
    """
    users_json = os.getenv("DASHBOARD_USERS")
    if users_json:
        try:
            raw = json.loads(users_json)
            users = {}
            for username, password in raw.items():
                if password.startswith("$2b$"):
                    users[username] = password
                else:
                    users[username] = _hash_password(password)
            return users
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"  [WARNING] Invalid DASHBOARD_USERS format: {e}")

    # Default users (set these in .env for production!)
    default_pass = os.getenv("DASHBOARD_PASSWORD", "tcche2025")
    return {
        "admin": _hash_password(default_pass),
    }


USERS = _load_users()
print(f"  [OK] Auth: {len(USERS)} user(s) configured: {', '.join(USERS.keys())}")


# ============================================================
# JWT TOKEN MANAGEMENT
# ============================================================

def create_token(username: str) -> str:
    """Create a JWT token for the given user."""
    payload = {
        "sub": username,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(token: str) -> dict | None:
    """Verify a JWT token and return the payload, or None if invalid."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") in USERS:
            return payload
    except jwt.ExpiredSignatureError:
        pass
    except jwt.InvalidTokenError:
        pass
    return None


# ============================================================
# AUTHENTICATION
# ============================================================

def authenticate(username: str, password: str) -> str | None:
    """
    Validate credentials and return a JWT token if valid.
    Returns None if authentication fails.
    """
    username = username.strip().lower()
    hashed = USERS.get(username)
    if hashed and _check_password(password, hashed):
        return create_token(username)
    return None


def get_current_user() -> str | None:
    """Get the current authenticated user from the request cookie."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    payload = verify_token(token)
    return payload.get("sub") if payload else None


def is_authenticated() -> bool:
    """Check if the current request has a valid auth cookie."""
    return get_current_user() is not None


# ============================================================
# LOGIN / LOGOUT PAGE HTML
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
    Add authentication routes and middleware to the Flask server.
    Call this AFTER creating the Dash app.
    """
    app_server.secret_key = JWT_SECRET

    @app_server.before_request
    def require_auth():
        """Redirect unauthenticated users to login page."""
        # Allow access to login route and static assets
        allowed = ("/login", "/_dash-", "/assets/", "/_favicon.ico")
        if any(request.path.startswith(p) for p in allowed):
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

        # POST - handle login form
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
