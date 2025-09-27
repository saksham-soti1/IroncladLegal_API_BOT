# extract_and_store_texts.py
import os, re, hashlib, pathlib
from typing import Optional, Tuple
from db import get_conn

CONTRACT_DIR = pathlib.Path("data/contracts")
TEST_LIMIT = None      # None = process all files
BATCH_SIZE = 400       # reconnect every 400 files
START_INDEX = 1700     # 👈 set this to skip directly to Nth file

# ---------- text extraction ----------

def extract_text(path: pathlib.Path) -> str:
    ext = path.suffix.lower()
    if ext == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(path) as pdf:
                parts = [p.extract_text() or "" for p in pdf.pages]
            return "\n".join(parts)
        except Exception as e:
            print(f"[error] PDF parse failed for {path.name}: {e}")
            return ""
    elif ext == ".docx":
        try:
            from docx import Document
            doc = Document(path)
            return "\n".join(par.text for par in doc.paragraphs)
        except Exception as e:
            print(f"[error] DOCX parse failed for {path.name}: {e}")
            return ""
    else:
        return ""

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\x00", "")  # strip NUL chars
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def count_tokens(text: str) -> int:
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except Exception:
        return len(text.split())

# ---------- filename parsing ----------

def parse_filename(path: pathlib.Path) -> Tuple[str, str, str]:
    base = path.stem
    if " - " in base:
        left, inferred_title = base.split(" - ", 1)
    else:
        left, inferred_title = base, base
    parts = left.split("_", 1)
    if len(parts) == 2:
        readable_id, source_tag = parts
    else:
        readable_id, source_tag = left, "unknown"
    return readable_id, source_tag, inferred_title

# ---------- DB helpers ----------

def lookup_workflow(cur, readable_id: str) -> Tuple[Optional[str], Optional[str]]:
    cur.execute(
        "SELECT workflow_id, title FROM ic.workflows WHERE readable_id = %s",
        (readable_id,)
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)

def already_loaded(cur, readable_id: str, sha: str) -> bool:
    cur.execute(
        "SELECT 1 FROM ic.contract_texts WHERE readable_id = %s AND text_sha256 = %s",
        (readable_id, sha)
    )
    return cur.fetchone() is not None

def insert_contract_text(cur, workflow_id, readable_id, title, text, sha, token_count, source_status):
    cur.execute(
        """
        INSERT INTO ic.contract_texts
          (workflow_id, readable_id, title, text, text_sha256, token_count, source_status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (workflow_id, readable_id, title, text, sha, token_count, source_status)
    )

# ---------- main ----------

def main():
    files = sorted([p for p in CONTRACT_DIR.glob("*") if p.is_file()])
    if not files:
        print(f"No files found in {CONTRACT_DIR}")
        return

    limit = len(files) if TEST_LIMIT is None else TEST_LIMIT
    files_to_process = files[START_INDEX:limit] if START_INDEX else files[:limit]

    processed = inserted = skipped_empty = 0
    batch_counter = 0

    conn = get_conn()
    cur = conn.cursor()

    try:
        for i, path in enumerate(files_to_process, start=START_INDEX+1 if START_INDEX else 1):
            readable_id, source_status, inferred_title = parse_filename(path)
            wf_id, wf_title = lookup_workflow(cur, readable_id)
            title_to_store = wf_title or inferred_title

            raw = extract_text(path)
            text = clean_text(raw)
            if not text:
                print(f"[{i}] [skip] {path.name}: no extractable text")
                skipped_empty += 1
                processed += 1
            else:
                sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
                if already_loaded(cur, readable_id, sha):
                    print(f"[{i}] [skip] {path.name}: already loaded")
                    processed += 1
                else:
                    tokens = count_tokens(text)
                    insert_contract_text(cur, wf_id, readable_id, title_to_store, text, sha, tokens, source_status)
                    conn.commit()
                    inserted += 1
                    processed += 1
                    print(f"[{i}] [ok] {path.name}: inserted (id={readable_id}, wf_id={wf_id}, status={source_status}, tokens={tokens})")

            batch_counter += 1
            if batch_counter >= BATCH_SIZE:
                # reconnect to DB
                cur.close()
                conn.close()
                conn = get_conn()
                cur = conn.cursor()
                batch_counter = 0
                print(f"--- reconnected DB after {i} files ---")

    finally:
        cur.close()
        conn.close()

    print("\n=== LOAD SUMMARY ===")
    print(f"processed: {processed}")
    print(f"inserted:  {inserted}")
    print(f"skipped empty: {skipped_empty}")

if __name__ == "__main__":
    main()
