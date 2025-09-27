# ironclad_auth.py
import os
import time
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

IRONCLAD_CLIENT_ID = os.getenv("IRONCLAD_CLIENT_ID")
IRONCLAD_CLIENT_SECRET = os.getenv("IRONCLAD_CLIENT_SECRET")
IRONCLAD_SCOPES = os.getenv("IRONCLAD_SCOPES")

# If you set a BASE_URL (for API calls), we’ll derive the token host from it
BASE_URL = os.getenv("IRONCLAD_BASE_URL", "https://na1.ironcladapp.com/public/api/v1")

_token_cache = {"access_token": None, "expires_at": 0}

def _host_from_base_url() -> tuple[str, str]:
    """Return (scheme, host) from IRONCLAD_BASE_URL, else sane defaults."""
    try:
        p = urlparse(BASE_URL)
        scheme = p.scheme or "https"
        host = p.netloc or "na1.ironcladapp.com"
        return scheme, host
    except Exception:
        return "https", "na1.ironcladapp.com"

def _candidate_token_urls() -> list[str]:
    scheme, host = _host_from_base_url()
    urls = [
        f"{scheme}://{host}/public/oauth/token",
        f"{scheme}://{host}/oauth/token",
    ]
    # Also try the global host if a regional host fails DNS
    if host != "app.ironcladapp.com":
        urls += [
            f"{scheme}://app.ironcladapp.com/public/oauth/token",
            f"{scheme}://app.ironcladapp.com/oauth/token",
        ]
    return urls

def _request_token(token_url: str) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": IRONCLAD_CLIENT_ID,
        "client_secret": IRONCLAD_CLIENT_SECRET,
        "scope": IRONCLAD_SCOPES,
    }
    resp = requests.post(token_url, data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    access_token = payload["access_token"]
    # cache ~50 minutes by default
    expires_in = int(payload.get("expires_in", 3000))
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = time.time() + max(60, expires_in - 60)
    return access_token

def get_access_token(force_refresh: bool = False) -> str:
    """Get (and cache) a bearer token. Falls back across multiple URLs."""
    if (
        not force_refresh
        and _token_cache["access_token"]
        and time.time() < _token_cache["expires_at"]
    ):
        return _token_cache["access_token"]

    last_err = None
    for url in _candidate_token_urls():
        try:
            return _request_token(url)
        except requests.RequestException as e:
            # Handles HTTPError, ConnectionError, Timeout, etc. — try next URL
            last_err = e
            continue
    # If we exhausted all candidates
    if last_err:
        raise last_err
    raise RuntimeError("No token URL could be constructed")
