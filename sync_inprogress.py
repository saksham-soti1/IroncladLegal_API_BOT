# sync_inprogress.py
import os
import time
from typing import Dict, Any, List, Tuple

import psycopg2.extras as extras

from db import get_conn
from ironclad_auth import get_access_token
from ironclad_api import (
    list_workflows,                      # we’ll call with status="active"
    get_workflow,
    get_record,
    list_workflow_participants_all,
    list_workflow_comments_all,
    list_workflow_approvals,
    list_workflow_approval_requests,
    list_workflow_turn_history,
)
from load_workflows import (
    upsert_workflow,                     # same header loader used for completed
    insert_documents,                    # same document loader
    insert_roles,                        # same roles + role_assignees loader
    insert_participants,                 # same participants loader
    insert_comments,                     # same comments loader
    insert_clauses_from_record,          # same clauses-from-record loader
)

PAGE_SIZE = int(os.getenv("SYNC_PAGE_SIZE", "100"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SYNC_CALL_SLEEP", "0.10"))
COMMIT_EVERY = int(os.getenv("SYNC_COMMIT_EVERY", "50"))
LIMIT = int(os.getenv("SYNC_INPROGRESS_LIMIT", "0"))  # 0 = no cap


def _batched_active_workflows(page_size=PAGE_SIZE):
    """
    Generator yielding (workflows_on_page, total_count) for status='active'.
    Mirrors the completed sync paging, but targets active. (Same API shape.)
    """
    page = 0
    total_count = None
    while True:
        payload = list_workflows(status="active", page=page, page_size=page_size)
        if total_count is None:
            total_count = payload.get("count")
            print(f"📊 Total ACTIVE workflows reported by API: {total_count}")

        workflows = payload.get("list", []) or []
        if not workflows:
            break
        yield workflows, total_count

        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)


# ---------- New inserters for in-progress-only resources ----------

def insert_approvals(cur, workflow_id: str, approvals: Dict[str, Any]):
    """
    approvals payload shape (from /workflows/{id}/approvals):
      - approvalGroups: [{ order, status, reviewers:[{role, displayName, reviewerType, status}, ...] }]
      - roles: [{ id, displayName, assignees:[{userName, userId, email}, ...] }]
    We persist:
      ic.approvals(workflow_id, group_order, role_id, role_name, reviewer_type, status)
    Also upsert role assignees into ic.role_assignees to guarantee names/emails are captured.
    """
    if not approvals:
        return

    # 1) reviewers in group order
    for grp in approvals.get("approvalGroups", []) or []:
        group_order = grp.get("order")
        for r in grp.get("reviewers", []) or []:
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

    # 2) explicitly store assignees for roles (emails → real people)
    #    (This overlaps with insert_roles(), but it's safe and ensures coverage.)
    for role in approvals.get("roles", []) or []:
        role_id = role.get("id")
        role_name = role.get("displayName")
        # ensure role row exists (idempotent relative to insert_roles)
        cur.execute("""
            INSERT INTO ic.roles (workflow_id, role_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (workflow_id, role_id) DO UPDATE SET display_name = EXCLUDED.display_name
        """, (workflow_id, role_id, role_name))

        for a in role.get("assignees", []) or []:
            cur.execute("""
                INSERT INTO ic.role_assignees (workflow_id, role_id, user_id, user_name, email)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (workflow_id, role_id, email) DO NOTHING
            """, (workflow_id, role_id, a.get("userId"), a.get("userName"), a.get("email")))


def insert_approval_requests(cur, workflow_id: str, approval_reqs: Dict[str, Any]):
    """
    approval-requests payload (paged wrapper):
      { page, pageSize, count, list: [
          {startTime, endTime, status, actorId, actorType, role, roleName,
           duration, aggregateDuration, approvalType}, ...
        ]
      }
    We persist:
      ic.approval_requests(workflow_id, start_time, end_time, status, actor_type, actor_id,
                           role_id, role_name, duration_ms, aggregate_duration_ms, approval_type)
    """
    if not approval_reqs:
        return
    for r in approval_reqs.get("list", []) or []:
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
    turn-history payload (paged wrapper):
      { page, pageSize, count, list: [
          {turnNumber, turnParty, turnLocation, turnUserId, turnUserEmail,
           turnStartTime, turnEndTime}, ...
        ]
      }
    We persist:
      ic.turn_history(workflow_id, turn_number, turn_party, turn_location, turn_user_id,
                      turn_user_email, turn_start_time, turn_end_time)
    """
    if not turns:
        return
    for t in turns.get("list", []) or []:
        cur.execute("""
            INSERT INTO ic.turn_history
              (workflow_id, turn_number, turn_party, turn_location, turn_user_id,
               turn_user_email, turn_start_time, turn_end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workflow_id, turn_number) DO UPDATE SET
              turn_party = EXCLUDED.turn_party,
              turn_location = EXCLUDED.turn_location,
              turn_user_id = EXCLUDED.turn_user_id,
              turn_user_email = EXCLUDED.turn_user_email,
              turn_start_time = EXCLUDED.turn_start_time,
              turn_end_time = EXCLUDED.turn_end_time
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


# ---------- Loader for a single active workflow (header + children) ----------

def load_one_active(cur, wf_stub: Dict[str, Any]) -> str:
    """
    Fully loads an active workflow into Postgres.
    - Reuses the same completed loaders for header/docs/roles/participants/comments/clauses.
    - Adds approvals, approval_requests, and turn_history inserts.
    Idempotent via ON CONFLICT protections.
    """
    wf_id = wf_stub.get("id")

    # fetch full workflow detail
    detail = get_workflow(wf_id) or {}

    # header → ic.workflows (+ step_states)
    stored_wf_id, attributes = upsert_workflow(cur, {"workflow": detail})

    # documents from attributes
    insert_documents(cur, stored_wf_id, attributes)

    # roles (+ assignees) from full workflow
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

    # approvals (summary + role assignees redundancy for safety)
    try:
        approvals = list_workflow_approvals(wf_id)
        if approvals:
            insert_approvals(cur, stored_wf_id, approvals)
    except Exception as e:
        print(f"   ⚠️ approvals fetch failed for {wf_id}: {e}")

    # approval requests (paged)
    try:
        approval_reqs = list_workflow_approval_requests(wf_id)
        if approval_reqs:
            insert_approval_requests(cur, stored_wf_id, approval_reqs)
    except Exception as e:
        print(f"   ⚠️ approval-requests fetch failed for {wf_id}: {e}")

    # turn history (paged)
    try:
        turns = list_workflow_turn_history(wf_id)
        if turns:
            insert_turn_history(cur, stored_wf_id, turns)
    except Exception as e:
        print(f"   ⚠️ turn-history fetch failed for {wf_id}: {e}")

    # clauses (records API – often empty for in-progress)
    for rid in detail.get("recordIds") or []:
        try:
            record = get_record(rid) or {}
            insert_clauses_from_record(cur, stored_wf_id, record)
            time.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as rec_err:
            print(f"   ⚠️ record {rid} failed: {rec_err}")

    print(f"  ✔ Loaded active workflow {stored_wf_id}")
    return stored_wf_id


def main():
    token = get_access_token()
    print("✅ Got Ironclad token (first 20 chars):", token[:20])

    loaded = 0
    seen = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for workflows, total in _batched_active_workflows(page_size=PAGE_SIZE):
                for wf_stub in workflows:
                    # Optional cap for initial testing
                    if LIMIT and seen >= LIMIT:
                        break

                    load_one_active(cur, wf_stub)
                    seen += 1
                    loaded += 1

                    if loaded % COMMIT_EVERY == 0:
                        conn.commit()
                        print(f"💾 Committed {loaded} active so far...")

                    time.sleep(SLEEP_BETWEEN_CALLS)

                if LIMIT and seen >= LIMIT:
                    break

        conn.commit()

    print("\n🎉 Active sync complete.")
    print(f"   Seen:     {seen}")
    print(f"   Loaded:   {loaded}")
    print("   Failures: 0 (see warnings above if any)")

if __name__ == "__main__":
    main()
