# sync_contract_texts.py
import os, pathlib, re, requests
from dotenv import load_dotenv
from ironclad_auth import get_access_token

load_dotenv()

BASE_URL = "https://na1.ironcladapp.com"   # confirmed from debug
API_PREFIX = "/public/api/v1"
USER_EMAIL = os.getenv("IRONCLAD_USER_EMAIL")
OUTPUT_DIR = pathlib.Path("data/contracts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEST_LIMIT = None  # 👈 None = process all records

def _headers():
    h = {"Authorization": f"Bearer {get_access_token()}", "Accept": "application/json"}
    if USER_EMAIL:
        h["x-as-user-email"] = USER_EMAIL
    return h

def safe_filename(ic_number: str, orig_name: str, source_tag: str) -> str:
    orig = re.sub(r'[<>:"/\\|?*]', "_", orig_name)
    return f"{ic_number}_{source_tag} - {orig[:100]}"

def main():
    page, page_size, total, downloaded = 0, 100, 0, 0
    workflow_count, import_count = 0, 0

    while TEST_LIMIT is None or total < TEST_LIMIT:
        params = {"page": page, "pageSize": page_size}
        url = f"{BASE_URL}{API_PREFIX}/records"
        r = requests.get(url, headers=_headers(), params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        items = data.get("list") or []
        if not items:
            break

        for rec in items:
            if TEST_LIMIT is not None and total >= TEST_LIMIT:
                break

            ic_number = rec.get("ironcladId") or rec.get("id")

            # 🟢 Tag imports vs workflows
            props = rec.get("properties") or {}
            is_import = "importId" in props
            source_tag = "imported" if is_import else "workflow"
            if is_import:
                import_count += 1
            else:
                workflow_count += 1

            attachments = rec.get("attachments") or {}
            signed = attachments.get("signedCopy")

            doc = signed
            if not doc:
                drafts = attachments.get("drafts") or []
                if drafts:
                    drafts = sorted(
                        drafts,
                        key=lambda d: d.get("lastModified", {}).get("timestamp") or "",
                        reverse=True,
                    )
                    doc = drafts[0]

            if not doc:
                total += 1
                print(f"[{total}] [skip] {ic_number} ({source_tag}): no signed or draft attachment")
                continue

            href = doc.get("href")
            filename = doc.get("filename") or f"{ic_number}.pdf"
            if not href:
                total += 1
                print(f"[{total}] [skip] {ic_number} ({source_tag}): missing href")
                continue

            fname = safe_filename(ic_number, filename, source_tag)
            out_path = OUTPUT_DIR / fname

            if out_path.exists():
                total += 1
                print(f"[{total}] [skip] {ic_number} ({source_tag}): already exists")
                continue

            try:
                dl_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                with requests.get(dl_url, headers=_headers(), stream=True, timeout=120) as resp:
                    resp.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=1024*256):
                            if chunk:
                                f.write(chunk)

                downloaded += 1
                print(f"[{total+1}] [ok] {ic_number} ({source_tag}): saved {out_path}")
            except Exception as e:
                print(f"[{total+1}] [error] {ic_number} ({source_tag}): failed {e}")
            finally:
                total += 1

        page += 1

    print("\n=== SUMMARY ===")
    print(f"Records attempted: {total}")
    print(f"Files downloaded: {downloaded}")
    print(f" - Workflows: {workflow_count}")
    print(f" - Imports:   {import_count}")
    print(f"Files saved under: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
