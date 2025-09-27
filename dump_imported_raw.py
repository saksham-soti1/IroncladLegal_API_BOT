# dump_imported_raw.py
import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

IRONCLAD_BASE_URL = "https://na1.ironcladapp.com"
IRONCLAD_CLIENT_ID = os.getenv("IRONCLAD_CLIENT_ID")
IRONCLAD_CLIENT_SECRET = os.getenv("IRONCLAD_CLIENT_SECRET")
IRONCLAD_SCOPES = os.getenv("IRONCLAD_SCOPES")
IRONCLAD_USER_EMAIL = os.getenv("IRONCLAD_USER_EMAIL")
IRONCLAD_TOKEN_URL = f"{IRONCLAD_BASE_URL}/oauth/token"

RAW_DIR = Path("data/raw_imported")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# The 3 imported workflow/record IDs you gave me
RECORD_IDS = [
    "d40ff558-0bf7-4ac7-9f3d-b215954a5d9d",  # IC-3876
    "fb58db07-a3d3-4362-9a96-f1947e23db92",  # IC-120
    "3d0e360a-0cf7-4426-8706-630c8332654e",  # IC-1050
]

def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": IRONCLAD_CLIENT_ID,
        "client_secret": IRONCLAD_CLIENT_SECRET,
        "scope": IRONCLAD_SCOPES,
    }
    response = requests.post(IRONCLAD_TOKEN_URL, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_json(path: str, token: str):
    """Fetch JSON from Ironclad API at given path (e.g. /workflows/{id} or /records/{id})."""
    url = f"{IRONCLAD_BASE_URL}/public/api/v1{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "x-as-user-email": IRONCLAD_USER_EMAIL,
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

def dump_imported():
    token = get_access_token()
    for rid in RECORD_IDS:
        print(f"Fetching {rid}...")
        data = None
        try:
            data = fetch_json(f"/workflows/{rid}", token)
            print(f"  ✅ Workflow JSON found for {rid}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  ⚠️ No workflow for {rid}, trying records API...")
                try:
                    data = fetch_json(f"/records/{rid}", token)
                    print(f"  ✅ Record JSON found for {rid}")
                except requests.HTTPError as e2:
                    print(f"  ❌ Failed to fetch {rid} from /records: {e2}")
                    continue
            else:
                print(f"  ❌ Error fetching workflow {rid}: {e}")
                continue

        # Save JSON to disk
        if data:
            out_path = RAW_DIR / f"{rid}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"  💾 Saved JSON to {out_path}")

if __name__ == "__main__":
    dump_imported()
