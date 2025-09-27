import os
import time
from typing import Dict, Any

from db import get_conn
from ironclad_auth import get_access_token
from ironclad_api import (
    list_workflows,
    get_workflow,
    get_record,
    list_workflow_participants_all,
    list_workflow_comments_all,
)
from load_workflows import (
    upsert_workflow,
    insert_documents,
    insert_roles,
    insert_participants,
    insert_comments,
    insert_clauses_from_record,
)

PAGE_SIZE = int(os.getenv("SYNC_PAGE_SIZE", "100"))   # tune if you hit rate limits
SLEEP_BETWEEN_CALLS = float(os.getenv("SYNC_CALL_SLEEP", "0.10"))  # seconds
COMMIT_EVERY = int(os.getenv("SYNC_COMMIT_EVERY", "50"))           # commit batch size


def batched_completed_workflows(page_size=PAGE_SIZE):
    """
    Generator that yields lists of workflows (not raw page dicts).
    """
    page = 0
    total_count = None
    while True:
        payload = list_workflows(page=page, page_size=page_size)
        if total_count is None:
            total_count = payload.get("count")
            print(f"📊 Total workflows reported by API: {total_count}")

        workflows = payload.get("list", [])
        if not workflows:
            break
        yield workflows, total_count
        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)


def load_one_workflow(cur, wf_stub: Dict[str, Any]) -> str:
    """
    Fully loads a single workflow IF it is not already in Postgres.
    Skips workflows already present in ic.workflows.
    Returns the workflow_id (whether skipped or loaded).
    """
    wf_id = wf_stub.get("id")

    # ✅ Skip if workflow already exists
    cur.execute("SELECT 1 FROM ic.workflows WHERE workflow_id = %s", (wf_id,))
    if cur.fetchone():
        return wf_id  # skip silently, no print

    # fetch full workflow detail
    detail = get_workflow(wf_id) or {}

    # store header; returns (workflow_id, attributes_from_header)
    stored_wf_id, attributes = upsert_workflow(cur, {"workflow": detail})

    # child data (docs, roles)
    insert_documents(cur, stored_wf_id, attributes)
    insert_roles(cur, stored_wf_id, {"workflow": detail})

    # participants (all pages)
    try:
        participants = list_workflow_participants_all(wf_id)
        if participants:
            insert_participants(cur, stored_wf_id, {"participants": participants})
    except Exception as e:
        print(f"   ⚠️ participants fetch failed for {wf_id}: {e}")

    # comments (all pages)
    try:
        comments = list_workflow_comments_all(wf_id)
        if comments:
            insert_comments(cur, stored_wf_id, {"comments": comments})
    except Exception as e:
        print(f"   ⚠️ comments fetch failed for {wf_id}: {e}")

    # clauses
    for rid in detail.get("recordIds") or []:
        try:
            record = get_record(rid) or {}
            insert_clauses_from_record(cur, stored_wf_id, record)
            time.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as rec_err:
            print(f"   ⚠️ record {rid} failed: {rec_err}")

    print(f"  ✔ Loaded workflow {stored_wf_id}")
    return stored_wf_id


def main():
    token = get_access_token()
    print("✅ Connected to Postgres")
    print(f"✅ Got Ironclad token (first 20 chars): {token[:20]}")

    total_loaded = total_seen = failures = 0
    total_count = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 🔄 Only load new completed workflows (skip existing)
            for page_num, (workflows, total_count) in enumerate(batched_completed_workflows(), start=1):
                print(f"\n📄 Page {page_num} — {len(workflows)} workflows")
                for wf in workflows:
                    total_seen += 1
                    wf_id = wf.get("id") or "<no-id>"
                    title = (wf.get("title") or "").strip()
                    try:
                        stored_id = load_one_workflow(cur, wf)
                        # load_one_workflow skips if workflow already exists
                        if stored_id == wf_id:
                            continue
                        total_loaded += 1
                        if total_loaded % 25 == 0:
                            print(f"  ✔ {stored_id} | {title[:80]} "
                                  f"({total_loaded}/{total_count} ~ {total_loaded/total_count:.1%})")
                    except Exception as e:
                        failures += 1
                        print(f"  ❌ {wf_id} | {title[:120]}  -> {e}")

                    if total_loaded % COMMIT_EVERY == 0:
                        conn.commit()
                        print(f"💾 Committed {total_loaded} so far...")

                time.sleep(SLEEP_BETWEEN_CALLS)

            conn.commit()

    print("\n🎉 Sync complete.")
    print(f"   Seen:     {total_seen}")
    print(f"   Loaded:   {total_loaded}")
    print(f"   Failures: {failures}")
    if total_count:
        print(f"   Coverage: {total_loaded}/{total_count} ({total_loaded/total_count:.1%})")


if __name__ == "__main__":
    main()
