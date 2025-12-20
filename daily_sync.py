# daily_sync.py
# Accurate daily updater — EXACT BEHAVIOR:
# 1) Snapshot API IDs (active, paused, completed).
# 2) COMPLETED (only NEW IDs): full reload (metadata + docs + text + embeddings).
# 3) ACTIVE (ALL): full refresh (metadata + docs + text + embeddings).
# 4) PAUSED (ALL): full refresh (metadata + docs + text + embeddings).
# 5) IMPORTED (NEW/CHANGED): run import pipeline end-to-end (dump -> load -> text/embeddings).
# 6) Normalize imported statuses (NULL -> 'imported').
# 7) Reconcile transitions for stale active/paused in DB.
# 8) Update ic.sync_run_log.last_run_at.

import os, re, time, pathlib, hashlib, subprocess
from typing import Dict, Any, List, Tuple, Set, Optional

from dotenv import load_dotenv
load_dotenv()

from db import get_conn
from ironclad_auth import get_access_token
from ironclad_api import (
    list_workflows,
    get_workflow,
    get_record,
    list_workflow_participants_all,
    list_workflow_comments_all,
    # Optional: if your ironclad_api exposes a direct document fetcher, we’ll try it.
    # get_workflow_document,  # <- uncomment if available
)
from load_workflows import (
    upsert_workflow,
    insert_documents,
    insert_roles,
    insert_participants,
    insert_comments,
    insert_clauses_from_record,
)
from sync_completed import backfill_completed_approvals
from sync_inprogress import _batched_active_workflows  # uses your paging

# ----- config -----
OUTPUT_DIR = pathlib.Path("data/contracts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")
BATCH_DB_COMMIT = 25
API_RECONNECT_EVERY = 70

# ----- light text + embedding helpers -----
import pdfplumber
from docx import Document as DocxDocument
import tiktoken
from openai import OpenAI

oai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
enc = tiktoken.get_encoding("cl100k_base")
CTRL_REGEX = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

def sanitize(s: str) -> str:
    if not s:
        return ""
    s = CTRL_REGEX.sub(" ", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def sha256(text: str) -> str:
    import hashlib as _h
    return _h.sha256(text.encode("utf-8")).hexdigest()

def extract_text_from_path(path: pathlib.Path) -> Tuple[str, str]:
    if path.suffix.lower() == ".pdf":
        parts = []
        with pdfplumber.open(path) as pdf:
            for p in pdf.pages:
                parts.append(p.extract_text() or "")
        text = "\n\n".join(parts)
        return sanitize(text), sanitize(path.stem)
    if path.suffix.lower() in (".docx", ".doc"):
        doc = DocxDocument(str(path))
        text = "\n".join(p.text for p in doc.paragraphs)
        return sanitize(text), sanitize(path.stem)
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = ""
    return sanitize(text), sanitize(path.stem)

CHARS_PER_CHUNK = 4000
CHARS_OVERLAP = 600

def chunk_text(s: str) -> List[Tuple[int, int, str]]:
    s = sanitize(s)
    n = len(s)
    chunks, i = [], 0
    while i < n:
        j = min(n, i + CHARS_PER_CHUNK)
        piece = s[i:j]
        chunks.append((i, j, piece))
        if j == n: break
        i = max(i + CHARS_PER_CHUNK - CHARS_OVERLAP, j)
    return chunks

def embed_batch(texts: List[str]) -> List[List[float]]:
    payload = [t if (t and t.strip()) else " " for t in texts]
    while True:
        try:
            resp = oai.embeddings.create(model=EMBED_MODEL, input=payload)
            return [d.embedding for d in resp.data]
        except Exception as e:
            print(f"[warn] embed_batch: {e}; retrying in 8s")
            time.sleep(8)

# ----- Ironclad doc download -----
import requests
BASE_URL = "https://na1.ironcladapp.com"

def _headers():
    h = {"Authorization": f"Bearer {get_access_token()}", "Accept": "application/json"}
    if os.getenv("IRONCLAD_USER_EMAIL"):
        h["x-as-user-email"] = os.getenv("IRONCLAD_USER_EMAIL")
    return h

def safe_filename(ic_number: str, orig_name: str, source_tag: str) -> str:
    orig = re.sub(r'[<>:"/\\|?*]', "_", orig_name)
    return f"{ic_number}_{source_tag} - {orig[:100]}"

def _coerce_list(x):
    if not x:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return []

def _collect_candidate_docs(attributes: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Be aggressive: harvest candidates from common attachment places.
    Each candidate: {"href": str, "filename": str, "ts": str}
    """
    cands: List[Dict[str,str]] = []

    # 1) Explicit signed doc
    signed = attributes.get("signed")
    if isinstance(signed, dict):
        href = signed.get("download") or signed.get("href")
        fn = signed.get("filename") or "signed.pdf"
        ts = (signed.get("lastModified") or {}).get("timestamp") or ""
        if href:
            cands.append({"href": href, "filename": fn, "ts": ts})

    # 2) Drafts: pick the newest
    for d in _coerce_list(attributes.get("draft")):
        if isinstance(d, dict):
            href = d.get("download") or d.get("href")
            fn = d.get("filename") or "draft.pdf"
            ts = (d.get("lastModified") or {}).get("timestamp") or ""
            if href:
                cands.append({"href": href, "filename": fn, "ts": ts})

    # 3) Other likely places that some environments use
    for key in ("uploadedFiles", "attachments", "files", "documents"):
        for d in _coerce_list(attributes.get(key)):
            if isinstance(d, dict):
                href = d.get("download") or d.get("href")
                fn = d.get("filename") or d.get("name") or f"{key}.pdf"
                ts = (d.get("lastModified") or {}).get("timestamp") or ""
                if href:
                    cands.append({"href": href, "filename": fn, "ts": ts})

    # Keep unique by href, and prefer later timestamps
    uniq = {}
    for x in cands:
        href = x["href"]
        if href not in uniq or str(x.get("ts","")) > str(uniq[href].get("ts","")):
            uniq[href] = x
    out = list(uniq.values())
    # Sort newest first; ensure signed is preferred when ties
    out.sort(key=lambda z: (str(z.get("ts","")), "signed" not in z.get("filename","").lower()), reverse=True)
    return out

def _download_url_to(readable_id: str, href: str, filename: str, source_tag: str) -> Optional[pathlib.Path]:
    dl = href if href.startswith("http") else f"{BASE_URL}{href}"
    out = OUTPUT_DIR / safe_filename(readable_id, filename, source_tag)
    with requests.get(dl, headers=_headers(), stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(1024 * 256):
                if chunk: f.write(chunk)
    return out

def download_primary_doc(readable_id: str, attributes: Dict[str, Any], source_tag: str = "workflow") -> Optional[pathlib.Path]:
    """
    Aggressive downloader: try signed, newest draft, and other attachment buckets.
    Fallback: optional document API if exposed.
    """
    cands = _collect_candidate_docs(attributes)
    for c in cands:
        try:
            return _download_url_to(readable_id, c["href"], c["filename"], source_tag)
        except Exception as e:
            print(f"   ⚠ download candidate failed ({c.get('filename')}): {e}")

    # --- Optional fallback via API (if available in your ironclad_api) ---
    # try:
    #     doc_payload = get_workflow_document(readable_id)  # if your SDK provides it
    #     href = (doc_payload or {}).get("downloadUrl") or (doc_payload or {}).get("href")
    #     filename = (doc_payload or {}).get("filename") or "document.pdf"
    #     if href:
    #         return _download_url_to(readable_id, href, filename, source_tag)
    # except Exception as e:
    #     print(f"   ⚠ document API fallback failed for {readable_id}: {e}")

    return None

# ----- DB text/embeddings upsert -----
def upsert_text_and_chunks(cur, workflow_id: str, readable_id: str, title_fallback: str, text: str) -> int:
    cur.execute("SELECT title FROM ic.workflows WHERE readable_id = %s", (readable_id,))
    row = cur.fetchone()
    title_to_store = sanitize(row[0]) if row and row[0] else sanitize(title_fallback)

    # Always refresh text for this readable_id
    cur.execute("DELETE FROM ic.contract_chunks WHERE readable_id = %s", (readable_id,))
    cur.execute("DELETE FROM ic.contract_texts  WHERE readable_id = %s", (readable_id,))

    if not text.strip():
        return 0

    text_sha = sha256(text)
    token_count = len(enc.encode(text))
    cur.execute(
        """
        INSERT INTO ic.contract_texts
          (workflow_id, readable_id, title, text, text_sha256, token_count, updated_at, source_status)
        VALUES (%s,%s,%s,%s,%s,%s,NOW(),%s)
        """,
        (workflow_id, readable_id, title_to_store, text, text_sha, token_count, "refreshed"),
    )

    pieces = chunk_text(text)
    if not pieces:
        return 0

    # embed in sub-batches of 100
    for b in range(0, len(pieces), 100):
        batch = pieces[b:b+100]
        vecs = embed_batch([p[2] for p in batch])
        for idx, ((start_c, end_c, body), vec) in enumerate(zip(batch, vecs), start=b):
            cur.execute(
                """
                INSERT INTO ic.contract_chunks
                  (workflow_id, readable_id, chunk_id, start_char, end_char, chunk_text, text_sha256, embedding)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (workflow_id, readable_id, idx, start_c, end_c, body, sha256(body), vec),
            )
    return len(pieces)

# ----- snapshot helpers -----
def api_ids_via_generator(status: str) -> Set[str]:
    ids: Set[str] = set()
    if status == "active":
        for workflows, _ in _batched_active_workflows():
            for wf in workflows or []:
                wid = wf.get("id")
                if wid: ids.add(wid)
        return ids

    page = 0
    while True:
        payload = list_workflows(status=status, page=page)
        items = (payload or {}).get("list") or []
        if not items: break
        for wf in items:
            wid = wf.get("id")
            if wid: ids.add(wid)
        page += 1
    return ids

def db_ids_for_status(status: str) -> Set[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT workflow_id FROM ic.workflows WHERE status = %s", (status,))
        return {r[0] for r in cur.fetchall()}

def set_last_run_now():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE ic.sync_run_log SET last_run_at = NOW() WHERE name='daily'")
        conn.commit()

# ----- core process -----
def _ingest_clause_records(cur, stored_wf_id: str, detail: Dict[str, Any]) -> None:
    for rid in detail.get("recordIds") or []:
        try:
            record = get_record(rid) or {}
            insert_clauses_from_record(cur, stored_wf_id, record)
            time.sleep(0.05)
        except Exception as rec_err:
            print(f"   ⚠ record {rid} failed: {rec_err}")

def _ingest_people_and_comments(cur, stored_wf_id: str) -> None:
    try:
        participants = list_workflow_participants_all(stored_wf_id) or []
        if participants:
            insert_participants(cur, stored_wf_id, {"participants": participants})
        comments = list_workflow_comments_all(stored_wf_id) or []
        if comments:
            insert_comments(cur, stored_wf_id, {"comments": comments})
    except Exception as e:
        print(f"   ⚠ participants/comments fetch failed for {stored_wf_id}: {e}")

def _refresh_text(cur, stored_wf_id: str, attributes: Dict[str, Any]) -> None:
    readable_id = attributes.get("readableId") or stored_wf_id
    path = None
    try:
        path = download_primary_doc(readable_id, attributes, source_tag="workflow")
    except Exception as e:
        print(f"   ⚠ download attempt failed for {stored_wf_id}: {e}")

    if path and path.exists():
        text, title_guess = extract_text_from_path(path)
        n_chunks = upsert_text_and_chunks(cur, stored_wf_id, readable_id, title_guess, text)
        print(f"   ✍ text refreshed for {readable_id} ({n_chunks} chunks)")
    else:
        # We still delete any existing text/chunks to keep “full refresh” semantics honest for active/paused
        cur.execute("DELETE FROM ic.contract_chunks WHERE readable_id = %s", (readable_id,))
        cur.execute("DELETE FROM ic.contract_texts  WHERE readable_id = %s", (readable_id,))
        print(f"   ⚠ no document available to refresh text for {readable_id}")

def full_reload_one(cur, token: str, workflow_id: str):
    detail = get_workflow(workflow_id) or {}
    stored_wf_id, attributes = upsert_workflow(cur, {"workflow": detail})
    insert_documents(cur, stored_wf_id, attributes)
    insert_roles(cur, stored_wf_id, {"workflow": detail})
    _ingest_people_and_comments(cur, stored_wf_id)
    _ingest_clause_records(cur, stored_wf_id, detail)
    backfill_completed_approvals(cur, stored_wf_id)
    _refresh_text(cur, stored_wf_id, attributes)

def process_ids_in_batches(title: str, ids: List[str]):
    if not ids:
        print(f"{title}: nothing to do")
        return
    print(f"{title}: {len(ids)} items")
    seen = 0
    with get_conn() as conn, conn.cursor() as cur:
        for i in range(0, len(ids), API_RECONNECT_EVERY):
            token = get_access_token()
            batch = ids[i:i+API_RECONNECT_EVERY]
            for wid in batch:
                try:
                    full_reload_one(cur, token, wid)
                    seen += 1
                    if seen % 10 == 0:
                        print(f"  ✔ {seen}/{len(ids)} processed")
                except Exception as e:
                    print(f"  ❌ {wid}: {e}")
            conn.commit()
            print(f"  💾 committed up to {seen}")

def main():
    print("🔎 snapshotting API…")
    active_api    = api_ids_via_generator("active")
    paused_api    = api_ids_via_generator("paused")
    completed_api = api_ids_via_generator("completed")
    print(f"  active_api={len(active_api)} paused_api={len(paused_api)} completed_api={len(completed_api)}")

    print("📊 loading DB status sets…")
    active_db     = db_ids_for_status("active")
    paused_db     = db_ids_for_status("paused")
    completed_db  = db_ids_for_status("completed")

    # COMPLETED → only NEW ids
    completed_new = sorted(list(completed_api - completed_db))

    # 1) COMPLETED (new only) — full ingest metadata + text
    process_ids_in_batches("🟢 completed (new)", completed_new)

    # 2) ACTIVE (ALL) — full refresh metadata + text (delete + re-embed every run)
    process_ids_in_batches("🟡 active (refresh all)",  sorted(list(active_api)))

    # 3) PAUSED (ALL) — full refresh metadata + text (delete + re-embed every run)
    process_ids_in_batches("🟠 paused (refresh all)",  sorted(list(paused_api)))

    # 4) IMPORTED (NEW/CHANGED) — run your existing import pipeline end-to-end
    print("📦 imported (new/changed): running import sync -> load -> text refresh")
    try:
        # fetch latest imported projects/records
        subprocess.run(["python", "sync_imported.py"], check=False)
        # upsert into ic.workflows/ic.documents/etc.
        subprocess.run(["python", "load_imported_workflows.py"], check=False)
        # extract text + chunk + embed for any new/updated imported files
        subprocess.run(["python", "sync_contract_texts.py"], check=False)
    except Exception as e:
        print(f"  ⚠ imported pipeline error: {e}")

    # 4b) Normalize legacy NULL -> 'imported' to keep counts clean
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE ic.workflows SET status = 'imported' WHERE status IS NULL")
        conn.commit()

    # 5) Reconcile transitions for actives/paused that disappeared from API sets
    active_db_after = db_ids_for_status("active")
    paused_db_after = db_ids_for_status("paused")
    stale_active = sorted(list(active_db_after - active_api))
    stale_paused = sorted(list(paused_db_after - paused_api))

    if stale_active or stale_paused:
        print(f"🔄 reconciling transitions… stale_active={len(stale_active)} stale_paused={len(stale_paused)}")
        with get_conn() as conn, conn.cursor() as cur:
            for wid in stale_active + stale_paused:
                try:
                    wf = get_workflow(wid) or {}
                    new_status = wf.get("status")
                    if new_status:
                        cur.execute(
                            "UPDATE ic.workflows SET status=%s, last_updated_at=NOW() WHERE workflow_id=%s",
                            (new_status, wid),
                        )
                except Exception as e:
                    print(f"  ⚠ reconcile {wid}: {e}")
            conn.commit()

    set_last_run_now()
    print("✅ daily sync complete")

if __name__ == "__main__":
    main()
