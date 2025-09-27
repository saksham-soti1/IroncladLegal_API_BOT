# chunk_contracts.py
import re
from db import get_conn

CHARS_PER_CHUNK = 4000
CHARS_OVERLAP = 600

def fetch_missing(limit=10):
    sql = """
    SELECT readable_id, workflow_id, text, text_sha256
    FROM ic.contract_texts t
    WHERE NOT EXISTS (
      SELECT 1 FROM ic.contract_chunks c
      WHERE c.readable_id = t.readable_id
    )
    ORDER BY updated_at DESC
    LIMIT %s;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def chunk_text(s: str):
    s = s.replace("\x00", "")  # strip any NULs
    n = len(s)
    i, chunks = 0, []
    while i < n:
        j = min(n, i + CHARS_PER_CHUNK)
        piece = s[i:j]
        chunks.append((i, j, piece))
        if j == n:
            break
        i = max(i + CHARS_PER_CHUNK - CHARS_OVERLAP, j)
    return chunks

def insert_chunks(readable_id, workflow_id, sha, pieces):
    with get_conn() as conn, conn.cursor() as cur:
        # delete existing chunks for this doc first (safe + idempotent)
        cur.execute("DELETE FROM ic.contract_chunks WHERE readable_id = %s", (readable_id,))
        for idx, (start_c, end_c, body) in enumerate(pieces):
            cur.execute("""
              INSERT INTO ic.contract_chunks
              (workflow_id, readable_id, chunk_id, start_char, end_char, chunk_text, text_sha256)
              VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (workflow_id, readable_id, idx, start_c, end_c, body, sha))
        conn.commit()

def main():
    rows = fetch_missing(limit=5)  # test with 5 docs first
    if not rows:
        print("No docs need chunking.")
        return

    processed = 0
    for readable_id, workflow_id, text, sha in rows:
        if not text:
            print(f"[skip] {readable_id}: empty text")
            continue

        pieces = chunk_text(text)
        if not pieces:
            print(f"[skip] {readable_id}: chunker returned 0 pieces")
            continue

        insert_chunks(readable_id, workflow_id, sha, pieces)
        processed += 1
        print(f"[ok] {readable_id}: {len(pieces)} chunks inserted")

    print(f"\nDone. Docs chunked: {processed}")
    print("Verify with:")
    print("  SELECT readable_id, count(*) AS chunks")
    print("  FROM ic.contract_chunks GROUP BY readable_id ORDER BY chunks DESC LIMIT 10;")

if __name__ == "__main__":
    main()
