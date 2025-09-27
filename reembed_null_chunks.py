# reembed_null_chunks.py
import os, time
from db import get_conn
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = "text-embedding-3-small"

def fetch_nulls(limit=50):
    sql = """
    SELECT readable_id, chunk_id, chunk_text
    FROM ic.contract_chunks
    WHERE embedding IS NULL
    ORDER BY readable_id, chunk_id
    LIMIT %s;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()

def embed_batch(texts):
    while True:
        try:
            resp = client.embeddings.create(model=MODEL, input=texts)
            return [d.embedding for d in resp.data]
        except Exception as e:
            print(f"[warn] embed_batch error {e}, retrying in 10s")
            time.sleep(10)

def update_embeddings(rows):
    with get_conn() as conn, conn.cursor() as cur:
        for batch_start in range(0, len(rows), 50):  # safe batch
            batch = rows[batch_start:batch_start+50]
            embeddings = embed_batch([r[2] for r in batch])
            for (readable_id, chunk_id, _), vec in zip(batch, embeddings):
                cur.execute("""
                  UPDATE ic.contract_chunks
                  SET embedding = %s
                  WHERE readable_id = %s AND chunk_id = %s
                """, (vec, readable_id, chunk_id))
        conn.commit()

def main():
    rows = fetch_nulls(limit=100)
    if not rows:
        print("No null embeddings found.")
        return
    print(f"re-embedding {len(rows)} chunks...")
    update_embeddings(rows)
    print("done.")

if __name__ == "__main__":
    main()
