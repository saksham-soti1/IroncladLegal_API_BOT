import os
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from ironclad_auth import get_access_token

load_dotenv()

# Default to NA1 if not provided
BASE_URL = os.getenv("IRONCLAD_BASE_URL", "https://na1.ironcladapp.com/public/api/v1")
USER_EMAIL = os.getenv("IRONCLAD_USER_EMAIL")  # for impersonation with client-credentials

# gentle throttling between paged calls
API_SLEEP = float(os.getenv("IRONCLAD_API_SLEEP", "0.15"))


def _headers() -> Dict[str, str]:
    token = get_access_token()
    h = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    # x-as-user-email is required for some endpoints
    if USER_EMAIL:
        h["x-as-user-email"] = USER_EMAIL
    return h


def _get(path: str, params: dict | None = None, max_retries: int = 3) -> Any:
    """
    GET wrapper with tiny retry. Always use the configured BASE_URL (default na1).
    Do not fall back to app.ironcladapp.com, since that host fails in this environment.
    `path` should start with '/' (e.g., '/workflows/...').
    """
    last_err = None
    base = BASE_URL.rstrip("/")
    url = f"{base}{path}"
    backoffs = [0.2, 0.5, 1.0]
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            if attempt < len(backoffs):
                time.sleep(backoffs[attempt])
            continue
    if last_err:
        raise last_err
    raise RuntimeError("Unknown request error")


# ------ Core resources already used by your sync ------

def list_workflows(status: str = "completed", page: int = 0, page_size: int = 5) -> dict:
    """List workflows (contracts). Default: first 5 completed."""
    params = {"page": page, "pageSize": page_size, "perPage": page_size, "status": status}
    return _get("/workflows", params=params)


def get_workflow(workflow_id: str) -> dict:
    """Get full workflow metadata by ID."""
    return _get(f"/workflows/{workflow_id}")


def get_record(record_id: str) -> dict:
    """Fetch a completed record by ID (e.g. from workflow['recordIds'])."""
    return _get(f"/records/{record_id}")


# ------ First-class helpers for participants & comments (auto-pagination) ------

def list_workflow_participants_all(workflow_id: str, page_size: int = 100) -> List[dict]:
    """
    Returns a flat list of ALL participants for a workflow (auto-paginated).
    The API sometimes returns a {'list': [...]} object; sometimes just a list.
    We normalize to a plain list.
    """
    results: List[dict] = []
    page = 0
    while True:
        data = _get(f"/workflows/{workflow_id}/participants", params={"page": page, "pageSize": page_size})
        items = data.get("list") if isinstance(data, dict) and "list" in data else data
        items = items or []
        if not isinstance(items, list):
            break
        results.extend(items)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(API_SLEEP)
    return results


def list_workflow_comments_all(workflow_id: str, page_size: int = 100) -> List[dict]:
    """
    Returns a flat list of ALL comments for a workflow (auto-paginated).
    The API sometimes returns a dict or a list — normalize to list.
    """
    results: List[dict] = []
    page = 0
    while True:
        data = _get(f"/workflows/{workflow_id}/comments", params={"page": page, "pageSize": page_size})
        items = data.get("list") if isinstance(data, dict) and "list" in data else data
        items = items or []
        if not isinstance(items, list):
            break
        results.extend(items)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(API_SLEEP)
    return results


# --- NEW HELPERS for in-progress workflows ---

def list_workflow_approvals(workflow_id: str) -> dict:
    """List approval groups for a workflow (if available)."""
    try:
        return _get(f"/workflows/{workflow_id}/approvals")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 400:
            return {}  # No approval groups returned
        raise

def list_workflow_approval_requests(workflow_id: str, page: int = 0, page_size: int = 100) -> dict:
    """Retrieve approval requests (individual approvals) on a workflow."""
    try:
        return _get(f"/workflows/{workflow_id}/approval-requests", params={"page": page, "pageSize": page_size})
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 400:
            return {}
        raise


def list_workflow_turn_history(workflow_id: str, page: int = 0, page_size: int = 100) -> dict:
    """Retrieve turn history on a workflow (paged)."""
    return _get(f"/workflows/{workflow_id}/turn-history", params={"page": page, "pageSize": page_size})


def get_workflow_sign_status(workflow_id: str) -> dict:
    """Retrieve sign step status for a workflow if it's in signing step."""
    try:
        return _get(f"/workflows/{workflow_id}/sign-status")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 400:
            return {}  # Not in signing yet
        raise


# --- Collect all pages for a given status ('active' for in-progress) ---

def list_all_workflows(status: str = "active", page_size: int = 100) -> dict:
    """
    Return {'count': <int>, 'list': [ ...all workflow stubs... ]} for the given status.
    Uses existing list_workflows(status=..., page=..., page_size=...).
    """
    all_items = []
    total = None
    page = 0
    while True:
        payload = list_workflows(status=status, page=page, page_size=page_size)
        if total is None:
            total = payload.get("count")
        items = payload.get("list", []) or []
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(API_SLEEP)
    return {"count": total if total is not None else len(all_items), "list": all_items}
