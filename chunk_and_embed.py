# chunk_and_embed.py
import os, re, time
from db import get_conn
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

CHARS_PER_CHUNK = 4000
CHARS_OVERLAP = 600
MODEL = "text-embedding-3-small"  # 1536 dims
BATCH_SIZE = 100                  # chunks per API call
DOCS_PER_SESSION = 200            # reconnect after this many docs

def fetch_missing(limit=None):
    sql = """
    SELECT readable_id, workflow_id, text, text_sha256
    FROM ic.contract_texts t
    WHERE NOT EXISTS (
      SELECT 1 FROM ic.contract_chunks c
      WHERE c.readable_id = t.readable_id
    )
    ORDER BY updated_at DESC
    """
    if limit:
        sql += " LIMIT %s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,) if limit else None)
        return cur.fetchall()

def chunk_text(s: str):
    s = (s or "").replace("\x00", "")
    n = len(s)
    chunks, i = [], 0
    while i < n:
        j = min(n, i + CHARS_PER_CHUNK)
        piece = s[i:j]
        chunks.append((i, j, piece))
        if j == n:
            break
        i = max(i + CHARS_PER_CHUNK - CHARS_OVERLAP, j)
    return chunks

def embed_batch(texts):
    while True:
        try:
            resp = client.embeddings.create(model=MODEL, input=texts)
            return [d.embedding for d in resp.data]
        except Exception as e:
            print(f"[warn] embed_batch error {e}, retrying in 10s")
            time.sleep(10)

def insert_chunks(conn, readable_id, workflow_id, sha, pieces):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ic.contract_chunks WHERE readable_id = %s", (readable_id,))
        for b in range(0, len(pieces), BATCH_SIZE):
            batch = pieces[b:b+BATCH_SIZE]
            embeddings = embed_batch([body for _,_,body in batch])
            for (idx,(start_c,end_c,body)), vec in zip(enumerate(batch, b), embeddings):
                cur.execute("""
                  INSERT INTO ic.contract_chunks
                  (workflow_id, readable_id, chunk_id,
                   start_char, end_char, chunk_text,
                   text_sha256, embedding)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (workflow_id, readable_id, idx,
                      start_c, end_c, body, sha, vec))
        conn.commit()

def main():
    rows = fetch_missing(limit=None)
    if not rows:
        print("No docs need chunking/embedding.")
        return

    conn = get_conn()
    processed = 0

    try:
        for i,(readable_id, workflow_id, text, sha) in enumerate(rows, start=1):
            if not text:
                print(f"[skip] {readable_id}: no text")
                continue
            pieces = chunk_text(text)
            if not pieces:
                print(f"[skip] {readable_id}: 0 chunks")
                continue
            insert_chunks(conn, readable_id, workflow_id, sha, pieces)
            processed += 1
            print(f"[{i}] [ok] {readable_id}: {len(pieces)} chunks embedded & inserted")

            if processed % DOCS_PER_SESSION == 0:
                # close & reopen connection to avoid Azure timeout
                conn.close()
                conn = get_conn()
                print(f"--- reconnected DB after {processed} docs ---")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
