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
    # 👇 added: approvals-related endpoints
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


# ---------- approvals upsert helpers (idempotent) ----------

def insert_approvals(cur, workflow_id: str, approvals: Dict[str, Any]):
    """
    Upsert approvals summary + ensure roles/assignees exist.
    Table shapes assumed from create_schema.py:
      - ic.approvals(workflow_id, group_order, role_id, role_name, reviewer_type, status)
      - ic.roles(workflow_id, role_id, display_name)
      - ic.role_assignees(workflow_id, role_id, user_id, user_name, email)
    """
    if not approvals:
        return

    # reviewer groups
    for grp in (approvals.get("approvalGroups") or []):
        group_order = grp.get("order")
        for r in (grp.get("reviewers") or []):
            role_id = r.get("role")
            role_name = r.get("displayName")
            reviewer_type = r.get("reviewerType")
            status = r.get("status")
            cur.execute("""
                INSERT INTO ic.approvals
                    (workflow_id, group_order, role_id, role_name, reviewer_type, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (workflow_id, group_order, role_id) DO UPDATE SET
                    role_name = EXCLUDED.role_name,
                    reviewer_type = EXCLUDED.reviewer_type,
                    status = EXCLUDED.status
            """, (workflow_id, group_order, role_id, role_name, reviewer_type, status))

    # roles + assignees (so people names/emails resolve in SQL joins)
    for role in (approvals.get("roles") or []):
        role_id = role.get("id")
        role_name = role.get("displayName")
        cur.execute("""
            INSERT INTO ic.roles (workflow_id, role_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (workflow_id, role_id) DO UPDATE SET
                display_name = EXCLUDED.display_name
        """, (workflow_id, role_id, role_name))
        for a in (role.get("assignees") or []):
            cur.execute("""
                INSERT INTO ic.role_assignees (workflow_id, role_id, user_id, user_name, email)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (workflow_id, role_id, email) DO NOTHING
            """, (workflow_id, role_id, a.get("userId"), a.get("userName"), a.get("email")))


def insert_approval_requests(cur, workflow_id: str, approval_reqs: Dict[str, Any]):
    """
    Upsert approval request events (time-based approvals).
    ic.approval_requests columns (from create_schema.py expectation):
      (workflow_id, start_time, end_time, status, actor_type, actor_id,
       role_id, role_name, duration_ms, aggregate_duration_ms, approval_type)
    """
    if not approval_reqs:
        return

    for r in (approval_reqs.get("list") or []):
        cur.execute("""
            INSERT INTO ic.approval_requests
              (workflow_id, start_time, end_time, status, actor_type, actor_id,
               role_id, role_name, duration_ms, aggregate_duration_ms, approval_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id, role_id, start_time) DO UPDATE SET
              end_time = EXCLUDED.end_time,
              status = EXCLUDED.status,
              actor_type = EXCLUDED.actor_type,
              actor_id = EXCLUDED.actor_id,
              role_name = EXCLUDED.role_name,
              duration_ms = EXCLUDED.duration_ms,
              aggregate_duration_ms = EXCLUDED.aggregate_duration_ms,
              approval_type = EXCLUDED.approval_type
        """, (
            workflow_id,
            r.get("startTime"), r.get("endTime"),
            r.get("status"),
            r.get("actorType"), r.get("actorId"),
            r.get("role"), r.get("roleName"),
            r.get("duration"), r.get("aggregateDuration"),
            r.get("approvalType"),
        ))


def insert_turn_history(cur, workflow_id: str, turns: Dict[str, Any]):
    """
    Upsert document turn history (optionally useful for timing/ownership analytics).
    ic.turn_history columns (from create_schema.py expectation):
      (workflow_id, turn_number, turn_party, turn_location, turn_user_id,
       turn_user_email, turn_start_time, turn_end_time)
    """
    if not turns:
        return

    for t in (turns.get("list") or []):
        cur.execute("""
            INSERT INTO ic.turn_history
              (workflow_id, turn_number, turn_party, turn_location, turn_user_id,
               turn_user_email, turn_start_time, turn_end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id, turn_number) DO UPDATE SET
              turn_party     = EXCLUDED.turn_party,
              turn_location  = EXCLUDED.turn_location,
              turn_user_id   = EXCLUDED.turn_user_id,
              turn_user_email= EXCLUDED.turn_user_email,
              turn_start_time= EXCLUDED.turn_start_time,
              turn_end_time  = EXCLUDED.turn_end_time
        """, (
            workflow_id,
            t.get("turnNumber"),
            t.get("turnParty"),
            t.get("turnLocation"),
            t.get("turnUserId"),
            t.get("turnUserEmail"),
            t.get("turnStartTime"),
            t.get("turnEndTime"),
        ))


def backfill_completed_approvals(cur, wf_id: str):
    """
    Fetch + upsert approvals/approval_requests/turn_history for a workflow
    EVEN IF it already exists in ic.workflows. This is the key to backfilling.
    """
    # approvals summary + assignees
    try:
        approvals = list_workflow_approvals(wf_id)
        if approvals:
            insert_approvals(cur, wf_id, approvals)
    except Exception as e:
        print(f"   ⚠ approvals fetch failed for {wf_id}: {e}")

    # approval request events (time-based)
    try:
        reqs = list_workflow_approval_requests(wf_id)
        if reqs:
            insert_approval_requests(cur, wf_id, reqs)
    except Exception as e:
        print(f"   ⚠ approval-requests fetch failed for {wf_id}: {e}")

    # turn history
    try:
        turns = list_workflow_turn_history(wf_id)
        if turns:
            insert_turn_history(cur, wf_id, turns)
    except Exception as e:
        print(f"   ⚠ turn-history fetch failed for {wf_id}: {e}")


# ---------- main loader ----------

def load_one_workflow(cur, wf_stub: Dict[str, Any]) -> str:
    """
    Fully loads a single completed workflow.
    Behavior:
      - If it doesn't exist, insert everything (header, docs, roles, participants, comments, clauses) + approvals backfill.
      - If it exists with the same status, we STILL backfill approvals/approval_requests/turn_history (idempotent).
      - If it exists but status changed (e.g., active → completed), update status + backfill approvals.
    Returns the workflow_id.
    """
    wf_id = wf_stub.get("id")
    new_status = wf_stub.get("status")

    # Check if workflow already exists
    cur.execute("SELECT status FROM ic.workflows WHERE workflow_id = %s", (wf_id,))
    row = cur.fetchone()
    if row:
        existing_status = row[0]
        if existing_status != new_status:
            cur.execute("""
                UPDATE ic.workflows
                SET status = %s,
                    last_updated_at = NOW()
                WHERE workflow_id = %s
            """, (new_status, wf_id))
            print(f"  🔄 Updated workflow {wf_id} status {existing_status} → {new_status}")

        # 👇 Always backfill approvals for completed (even for already-known workflows)
        backfill_completed_approvals(cur, wf_id)
        return wf_id  # we didn't reinsert header/docs/etc.

    # -------- new workflow path --------
    # fetch full workflow detail (only if new)
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

    # 👇 backfill approvals stack for this newly inserted completed workflow
    backfill_completed_approvals(cur, stored_wf_id)

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
            # 🔄 Load/refresh completed workflows
            for page_num, (workflows, total_count) in enumerate(batched_completed_workflows(), start=1):
                print(f"\n📄 Page {page_num} — {len(workflows)} workflows")
                for wf in workflows:
                    total_seen += 1
                    wf_id = wf.get("id") or "<no-id>"
                    title = (wf.get("title") or "").strip()
                    try:
                        stored_id = load_one_workflow(cur, wf)
                        # Count new inserts only
                        if stored_id != wf_id:  # inserted, not just status update/backfill
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
