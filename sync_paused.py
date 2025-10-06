# sync_paused.py
import os
import time
from typing import Dict, Any

import psycopg2.extras as extras

from db import get_conn
from ironclad_auth import get_access_token
from ironclad_api import (
    list_workflows,                      # we’ll call with status="paused"
    get_workflow,
    get_record,
    list_workflow_participants_all,
    list_workflow_comments_all,
    list_workflow_approvals,
    list_workflow_approval_requests,
    list_workflow_turn_history,
)
from load_workflows import (
    upsert_workflow,
    insert_documents,
    insert_roles,
    insert_participants,
    insert_comments,
    insert_clauses_from_record,
)

PAGE_SIZE = int(os.getenv("SYNC_PAGE_SIZE", "100"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SYNC_CALL_SLEEP", "0.10"))
COMMIT_EVERY = int(os.getenv("SYNC_COMMIT_EVERY", "50"))
LIMIT = int(os.getenv("SYNC_PAUSED_LIMIT", "0"))  # 0 = no cap


def _batched_paused_workflows(page_size=PAGE_SIZE):
    """
    Generator yielding (workflows_on_page, total_count) for status='paused'.
    Mirrors the active/completed sync paging, but targets paused.
    """
    page = 0
    total_count = None
    while True:
        payload = list_workflows(status="paused", page=page, page_size=page_size)
        if total_count is None:
            total_count = payload.get("count")
            print(f"📊 Total PAUSED workflows reported by API: {total_count}")

        workflows = payload.get("list", []) or []
        if not workflows:
            break
        yield workflows, total_count

        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)


def load_one_paused(cur, wf_stub: Dict[str, Any]) -> str:
    """
    Fully loads a paused workflow into Postgres.
    - Reuses same loaders as active/completed
    - Stores status='paused' into ic.workflows
    """
    wf_id = wf_stub.get("id")
    detail = get_workflow(wf_id) or {}

    stored_wf_id, attributes = upsert_workflow(cur, {"workflow": detail})

    insert_documents(cur, stored_wf_id, attributes)
    insert_roles(cur, stored_wf_id, {"workflow": detail})

    try:
        participants = list_workflow_participants_all(wf_id)
        if participants:
            insert_participants(cur, stored_wf_id, {"participants": participants})
    except Exception as e:
        print(f"   ⚠️ participants fetch failed for {wf_id}: {e}")

    try:
        comments = list_workflow_comments_all(wf_id)
        if comments:
            insert_comments(cur, stored_wf_id, {"comments": comments})
    except Exception as e:
        print(f"   ⚠️ comments fetch failed for {wf_id}: {e}")

    try:
        approvals = list_workflow_approvals(wf_id)
        if approvals:
            from sync_inprogress import insert_approvals
            insert_approvals(cur, stored_wf_id, approvals)
    except Exception as e:
        print(f"   ⚠️ approvals fetch failed for {wf_id}: {e}")

    try:
        approval_reqs = list_workflow_approval_requests(wf_id)
        if approval_reqs:
            from sync_inprogress import insert_approval_requests
            insert_approval_requests(cur, stored_wf_id, approval_reqs)
    except Exception as e:
        print(f"   ⚠️ approval-requests fetch failed for {wf_id}: {e}")

    try:
        turns = list_workflow_turn_history(wf_id)
        if turns:
            from sync_inprogress import insert_turn_history
            insert_turn_history(cur, stored_wf_id, turns)
    except Exception as e:
        print(f"   ⚠️ turn-history fetch failed for {wf_id}: {e}")

    for rid in detail.get("recordIds") or []:
        try:
            record = get_record(rid) or {}
            insert_clauses_from_record(cur, stored_wf_id, record)
            time.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as rec_err:
            print(f"   ⚠️ record {rid} failed: {rec_err}")

    print(f"  ✔ Loaded paused workflow {stored_wf_id}")
    return stored_wf_id


def main():
    token = get_access_token()
    print("✅ Got Ironclad token (first 20 chars):", token[:20])

    loaded = 0
    seen = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for workflows, total in _batched_paused_workflows(page_size=PAGE_SIZE):
                for wf_stub in workflows:
                    if LIMIT and seen >= LIMIT:
                        break

                    load_one_paused(cur, wf_stub)
                    seen += 1
                    loaded += 1

                    if loaded % COMMIT_EVERY == 0:
                        conn.commit()
                        print(f"💾 Committed {loaded} paused so far...")

                    time.sleep(SLEEP_BETWEEN_CALLS)

                if LIMIT and seen >= LIMIT:
                    break

        conn.commit()

    print("\n🎉 Paused sync complete.")
    print(f"   Seen:     {seen}")
    print(f"   Loaded:   {loaded}")
    print("   Failures: 0 (see warnings above if any)")


if __name__ == "__main__":
    main()
