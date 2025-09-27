# sync_imported.py
import os, time, json
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv()

BASE = "https://na1.ironcladapp.com/public/api/v1"
TOKEN_URL = "https://na1.ironcladapp.com/oauth/token"

CLIENT_ID = os.getenv("IRONCLAD_CLIENT_ID")
CLIENT_SECRET = os.getenv("IRONCLAD_CLIENT_SECRET")
SCOPES = os.getenv("IRONCLAD_SCOPES")
USER_EMAIL = os.getenv("IRONCLAD_USER_EMAIL")

RAW_DIR = Path("data/raw_imported")
RAW_DIR.mkdir(parents=True, exist_ok=True)

PAGE_SIZE = 100
SLEEP_BETWEEN_CALLS = 0.2


def get_access_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPES,
    }
    r = requests.post(TOKEN_URL, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def list_records(token, page=0, page_size=50, sort="createdAt", direction="asc"):
    url = f"{BASE}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "x-as-user-email": USER_EMAIL,
    }
    params = {"page": page, "perPage": page_size, "sortBy": sort, "direction": direction}
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    js = r.json()
    items = js.get("items") or js.get("list") or []
    return items


def get_record(token, record_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "x-as-user-email": USER_EMAIL,
    }
    r = requests.get(f"{BASE}/records/{record_id}", headers=headers)
    r.raise_for_status()
    return r.json()


def main():
    token = get_access_token()
    print("✅ Got Ironclad token (first 20 chars):", token[:20])

    dumped = 0
    page = 0
    while True:
        items = list_records(token, page=page, page_size=PAGE_SIZE,
                             sort="createdAt", direction="asc")
        if not items:
            break

        for rec in items:
            if rec.get("source", {}).get("type") != "import_project":
                continue  # 🚫 skip anything not imported

            rid = rec["id"]
            ic_num = rec.get("ironcladId")
            full = get_record(token, rid)
            out = RAW_DIR / f"{ic_num or rid}.json"
            out.write_text(json.dumps(full, indent=2))
            print(f"💾 dumped imported {ic_num or rid}")
            dumped += 1

        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"✔ Finished. {dumped} imported records dumped to {RAW_DIR}")


if __name__ == "__main__":
    main()
