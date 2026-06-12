import os
import re
import json
import shlex
import subprocess
import threading
from urllib.parse import urlparse

LOGINS_DIR = "/tmp/licher_logins"
LOGINS_DB  = os.path.join(LOGINS_DIR, "logins.json")
COOKIES_DIR = os.path.join(LOGINS_DIR, "cookies")

_lock = threading.Lock()

os.makedirs(COOKIES_DIR, exist_ok=True)


def _load_db():
    if not os.path.exists(LOGINS_DB):
        return {}
    try:
        with open(LOGINS_DB, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_db(db):
    with open(LOGINS_DB, "w") as f:
        json.dump(db, f, indent=2)


def get_domain(url):
    try:
        p = urlparse(url)
        return p.netloc.lower()
    except Exception:
        return url.lower()


def list_logins():
    with _lock:
        db = _load_db()
    out = []
    for domain, info in db.items():
        out.append({
            "domain": domain,
            "login_url": info.get("login_url", ""),
            "username": info.get("username", ""),
            "status": info.get("status", "unknown"),
            "user_field": info.get("user_field", ""),
            "pass_field": info.get("pass_field", ""),
        })
    return out


def remove_login(domain):
    with _lock:
        db = _load_db()
        if domain in db:
            jar = db[domain].get("jar")
            if jar and os.path.exists(jar):
                try:
                    os.remove(jar)
                except Exception:
                    pass
            db.pop(domain, None)
            _save_db(db)
            return True
    return False


def detect_login_fields(login_url):
    """Fetch the login page HTML and guess username/password field names
    from <input> tags with type=text/email/password."""
    try:
        result = subprocess.run(
            ["curl", "-g", "-L", "-s", "--max-time", "15",
             "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
             login_url],
            capture_output=True, text=True, timeout=20
        )
        html = result.stdout
    except Exception:
        return None, None

    user_field = None
    pass_field = None

    # Find all <input ...> tags
    for tag in re.findall(r'<input\b[^>]*>', html, re.IGNORECASE):
        name_m = re.search(r'name=["\']([^"\']+)["\']', tag, re.IGNORECASE)
        type_m = re.search(r'type=["\']([^"\']+)["\']', tag, re.IGNORECASE)
        if not name_m:
            continue
        name = name_m.group(1)
        ftype = (type_m.group(1).lower() if type_m else "text")

        if ftype == "password" and pass_field is None:
            pass_field = name
        elif ftype in ("text", "email") and user_field is None:
            # prefer names that look like login fields
            if re.search(r'user|email|login|name', name, re.IGNORECASE) or user_field is None:
                if user_field is None:
                    user_field = name

    # Fallback common names
    if user_field is None:
        for cand in ["username", "user", "email", "login"]:
            if re.search(r'name=["\']' + cand + r'["\']', html, re.IGNORECASE):
                user_field = cand
                break
    if pass_field is None:
        for cand in ["password", "pass", "passwd"]:
            if re.search(r'name=["\']' + cand + r'["\']', html, re.IGNORECASE):
                pass_field = cand
                break

    return user_field, pass_field


def perform_login(login_url, username, password, user_field=None, pass_field=None):
    """Logs in via curl POST, saves cookie jar, returns dict with status info."""
    domain = get_domain(login_url)

    if not user_field or not pass_field:
        detected_user, detected_pass = detect_login_fields(login_url)
        user_field = user_field or detected_user or "username"
        pass_field = pass_field or detected_pass or "password"

    jar_path = os.path.join(COOKIES_DIR, domain.replace(":", "_") + ".txt")

    safe_login_url = shlex.quote(login_url)
    safe_jar = shlex.quote(jar_path)
    data = f"{user_field}={username}&{pass_field}={password}"
    safe_data = shlex.quote(data)

    cmd = (
        f"curl -g -L -s -o /dev/null -w '%{{http_code}} %{{url_effective}}' "
        f"--connect-timeout 15 "
        f"-c {safe_jar} -b {safe_jar} "
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' "
        f"-H {shlex.quote('Referer: ' + login_url)} "
        f"-H 'Content-Type: application/x-www-form-urlencoded' "
        f"--data {safe_data} "
        f"{safe_login_url}"
    )

    status = "failed"
    detail = ""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        out = result.stdout.strip()
        parts = out.split(" ", 1)
        http_code = parts[0] if parts else ""
        final_url = parts[1] if len(parts) > 1 else ""

        if http_code.startswith("2") or http_code.startswith("3"):
            # if redirected away from the login url itself, likely success
            if final_url and final_url.rstrip("/") != login_url.rstrip("/"):
                status = "success"
                detail = f"HTTP {http_code}, redirected to {final_url}"
            else:
                status = "success"
                detail = f"HTTP {http_code}"
        else:
            status = "failed"
            detail = f"HTTP {http_code}"
    except Exception as e:
        status = "failed"
        detail = str(e)

    with _lock:
        db = _load_db()
        db[domain] = {
            "login_url": login_url,
            "username": username,
            "password": password,
            "user_field": user_field,
            "pass_field": pass_field,
            "jar": jar_path,
            "status": status,
            "detail": detail,
        }
        _save_db(db)

    return {"domain": domain, "status": status, "detail": detail,
            "user_field": user_field, "pass_field": pass_field}


def find_jar_for_url(url):
    """Return cookie jar path for a download URL if a matching saved login exists."""
    domain = get_domain(url)
    with _lock:
        db = _load_db()

    # exact match, then suffix match (e.g. www.site.com vs site.com)
    if domain in db:
        jar = db[domain].get("jar")
        if jar and os.path.exists(jar):
            return jar

    for saved_domain, info in db.items():
        if domain.endswith(saved_domain) or saved_domain.endswith(domain):
            jar = info.get("jar")
            if jar and os.path.exists(jar):
                return jar

    return None
