import json
import sys
import glob
from pathlib import Path
from typing import Any, Dict, Optional, List

import psycopg2.extras as extras
from db import get_conn


def get(d: Dict, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def first_non_null(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def pick_money(attributes: Dict, *keys) -> (Optional[float], Optional[str]):
    """
    Reads Ironclad monetaryAmount fields like:
      {"currency":"USD","amount":123.45}
    """
    for k in keys:
        obj = attributes.get(k)
        if isinstance(obj, dict) and "amount" in obj:
            return obj.get("amount"), obj.get("currency")
    return None, None


def upsert_workflow(cur, wf: Dict):
    # Accept either {"workflow": {...}} or a direct workflow object from API
    if "workflow" in wf:
        wf_header = wf.get("workflow", {}) or {}
    else:
        wf_header = wf  # direct workflow detail JSON

    attributes = wf_header.get("attributes", {}) or {}
    schema_def = wf_header.get("schema", {}) or {}

    wf_id        = wf_header.get("id")
    title        = wf_header.get("title")
    template     = wf_header.get("template")
    status       = wf_header.get("status")
    step         = wf_header.get("step")
    is_complete  = wf_header.get("isComplete")
    is_cancelled = wf_header.get("isCancelled")
    created      = wf_header.get("created")
    last_updated = wf_header.get("lastUpdated")

    # common fields
    record_type       = first_non_null(attributes.get("recordType"), attributes.get("documentType"))
    legal_entity      = attributes.get("legalEntity")
    department        = first_non_null(attributes.get("vaxcyteDepartment"), attributes.get("department"))
    owner_name        = attributes.get("ownerName")
    paper_source      = attributes.get("paperSource")
    document_type     = attributes.get("documentType")
    counterparty_name = attributes.get("counterpartyName")   # ✅ new field

    agreement_date = attributes.get("agreementDate")
    execution_date = attributes.get("executionDate")

    po_number       = first_non_null(attributes.get("pONumber"), attributes.get("requisitionPoNumberDisplay"))
    requisition_num = attributes.get("requisitionNumber")

    est_amt, est_ccy = pick_money(attributes, "estimatedCost", "estimatedCtaCosts")
    cv_amt,  cv_ccy  = pick_money(attributes, "contractValue", "estimatedCost")

    readable_id = attributes.get("readableId")
    ironclad_id = attributes.get("ironcladId")

    cur.execute("""
        INSERT INTO ic.workflows (
          workflow_id, readable_id, ironclad_id, title, template, status, step,
          is_complete, is_cancelled, created_at, last_updated_at,
          record_type, legal_entity, department, owner_name, paper_source, document_type,
          agreement_date, execution_date, po_number, requisition_number,
          estimated_cost_amount, estimated_cost_currency,
          contract_value_amount, contract_value_currency,
          counterparty_name,                 -- ✅ new column
          attributes, field_schema, raw_workflow
        )
        VALUES (
          %(workflow_id)s, %(readable_id)s, %(ironclad_id)s, %(title)s, %(template)s, %(status)s, %(step)s,
          %(is_complete)s, %(is_cancelled)s, %(created)s, %(last_updated)s,
          %(record_type)s, %(legal_entity)s, %(department)s, %(owner_name)s, %(paper_source)s, %(document_type)s,
          %(agreement_date)s, %(execution_date)s, %(po_number)s, %(requisition_number)s,
          %(estimated_cost_amount)s, %(estimated_cost_currency)s,
          %(contract_value_amount)s, %(contract_value_currency)s,
          %(counterparty_name)s,             -- ✅
          %(attributes)s, %(field_schema)s, %(raw_workflow)s
        )
        ON CONFLICT (workflow_id) DO UPDATE SET
          title=EXCLUDED.title,
          status=EXCLUDED.status,
          step=EXCLUDED.step,
          is_complete=EXCLUDED.is_complete,
          is_cancelled=EXCLUDED.is_cancelled,
          last_updated_at=EXCLUDED.last_updated_at,
          record_type=EXCLUDED.record_type,
          legal_entity=EXCLUDED.legal_entity,
          department=EXCLUDED.department,
          owner_name=EXCLUDED.owner_name,
          paper_source=EXCLUDED.paper_source,
          document_type=EXCLUDED.document_type,
          agreement_date=EXCLUDED.agreement_date,
          execution_date=EXCLUDED.execution_date,
          po_number=EXCLUDED.po_number,
          requisition_number=EXCLUDED.requisition_number,
          estimated_cost_amount=EXCLUDED.estimated_cost_amount,
          estimated_cost_currency=EXCLUDED.estimated_cost_currency,
          contract_value_amount=EXCLUDED.contract_value_amount,
          contract_value_currency=EXCLUDED.contract_value_currency,
          counterparty_name=EXCLUDED.counterparty_name,   -- ✅ update path
          attributes=EXCLUDED.attributes,
          field_schema=EXCLUDED.field_schema,
          raw_workflow=EXCLUDED.raw_workflow
    """, {
        "workflow_id": wf_id,
        "readable_id": readable_id,
        "ironclad_id": ironclad_id,
        "title": title,
        "template": template,
        "status": status,
        "step": step,
        "is_complete": is_complete,
        "is_cancelled": is_cancelled,
        "created": created,
        "last_updated": last_updated,
        "record_type": record_type,
        "legal_entity": legal_entity,
        "department": department,
        "owner_name": owner_name,
        "paper_source": paper_source,
        "document_type": document_type,
        "agreement_date": agreement_date,
        "execution_date": execution_date,
        "po_number": po_number,
        "requisition_number": requisition_num,
        "estimated_cost_amount": est_amt,
        "estimated_cost_currency": est_ccy,
        "contract_value_amount": cv_amt,
        "contract_value_currency": cv_ccy,
        "counterparty_name": counterparty_name,   # ✅ param
        "attributes": extras.Json(attributes),
        "field_schema": extras.Json(schema_def),
        "raw_workflow": extras.Json(wf_header),
    })

    # step states
    for step_name in ("approvals", "signatures"):
        state = get(wf_header, step_name, "state")
        if state:
            cur.execute("""
              INSERT INTO ic.step_states (workflow_id, step_name, state)
              VALUES (%s, %s, %s)
              ON CONFLICT (workflow_id, step_name) DO UPDATE SET state=EXCLUDED.state
            """, (wf_id, step_name, state))

    return wf_id, attributes


def insert_documents(cur, workflow_id: str, attributes: Dict[str, Any]):
    for d in ensure_list(attributes.get("draft")):
        _insert_doc(cur, workflow_id, "draft", d)
    if isinstance(attributes.get("signed"), dict):
        _insert_doc(cur, workflow_id, "signed", attributes["signed"])
    for d in ensure_list(attributes.get("sentSignaturePacket")):
        _insert_doc(cur, workflow_id, "sentSignaturePacket", d)
    if isinstance(attributes.get("partiallySigned"), dict):
        _insert_doc(cur, workflow_id, "partiallySigned", attributes["partiallySigned"])


def _insert_doc(cur, workflow_id: str, doc_type: str, doc: Dict[str, Any]):
    cur.execute("""
      INSERT INTO ic.documents
        (workflow_id, doc_type, version, version_number, filename, storage_key, download_path,
         last_modified_at, last_modified_author)
      VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        workflow_id,
        doc_type,
        doc.get("version"),
        doc.get("versionNumber"),
        doc.get("filename"),
        doc.get("key"),
        doc.get("download"),
        get(doc, "lastModified", "timestamp"),
        extras.Json(get(doc, "lastModified", "author")) if get(doc, "lastModified") else None
    ))


def insert_roles(cur, workflow_id: str, wf: Dict[str, Any]):
    for role in wf.get("workflow", {}).get("roles", []):
        role_id = role.get("id")
        disp    = role.get("displayName")
        cur.execute("""
          INSERT INTO ic.roles (workflow_id, role_id, display_name)
          VALUES (%s, %s, %s)
          ON CONFLICT (workflow_id, role_id) DO UPDATE SET display_name=EXCLUDED.display_name
        """, (workflow_id, role_id, disp))
        for a in role.get("assignees", []):
            cur.execute("""
              INSERT INTO ic.role_assignees (workflow_id, role_id, user_id, user_name, email)
              VALUES (%s, %s, %s, %s, %s)
              ON CONFLICT (workflow_id, role_id, email) DO NOTHING
            """, (workflow_id, role_id, a.get("userId"), a.get("userName"), a.get("email")))


def _normalize_items(container: Any, key: str) -> List[dict]:
    if container is None: return []
    if isinstance(container, list): return container
    if isinstance(container, dict) and "list" in container: return container["list"]
    return []


def insert_participants(cur, workflow_id: str, wf: Dict[str, Any]):
    raw = wf.get("participants")
    items = _normalize_items(raw, "participants")
    for item in items:
        cur.execute("""
          INSERT INTO ic.participants (workflow_id, user_id, email)
          VALUES (%s, %s, %s)
          ON CONFLICT (workflow_id, user_id, email) DO NOTHING
        """, (workflow_id, item.get("userId"), item.get("email")))


def insert_comments(cur, workflow_id: str, wf: Dict[str, Any]):
    raw = wf.get("comments")
    items = _normalize_items(raw, "comments")
    for c in items:
        cur.execute("""
          INSERT INTO ic.comments
            (comment_id, workflow_id, author, author_email, author_user_id, ts, message,
             is_external, mentioned, replied_to, reactions)
          VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT (comment_id) DO UPDATE SET
             author = EXCLUDED.author,
             author_email = EXCLUDED.author_email,
             author_user_id = EXCLUDED.author_user_id,
             ts = EXCLUDED.ts,
             message = EXCLUDED.message,
             is_external = EXCLUDED.is_external,
             mentioned = EXCLUDED.mentioned,
             replied_to = EXCLUDED.replied_to,
             reactions = EXCLUDED.reactions
        """, (
            c.get("id"), workflow_id,
            extras.Json(c.get("author")),
            get(c, "author", "email"),
            get(c, "author", "userId"),
            c.get("timestamp"),
            c.get("commentMessage"),
            c.get("isExternal"),
            extras.Json(c.get("mentionedUserDetails")),
            extras.Json(c.get("repliedTo")),
            extras.Json(c.get("reactions")),
        ))


def insert_clauses_from_record(cur, workflow_id: str, record: Dict):
    props = record.get("properties", {})
    for k, v in props.items():
        if k.lower().startswith("clause"):
            cur.execute("""
              INSERT INTO ic.clauses (workflow_id, clause_name, clause_value)
              VALUES (%s, %s, %s)
              ON CONFLICT (workflow_id, clause_name)
              DO UPDATE SET clause_value=EXCLUDED.clause_value
            """, (workflow_id, k, extras.Json(v)))


def load_one(cur, path: Path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    wf_id, attributes = upsert_workflow(cur, data)
    insert_documents(cur, wf_id, attributes)
    insert_roles(cur, wf_id, data)
    insert_participants(cur, wf_id, data)
    insert_comments(cur, wf_id, data)
    return wf_id


if __name__ == "__main__":
    files = []
    for arg in sys.argv[1:]:
        files.extend(glob.glob(arg))
    if not files:
        print("No input files provided. Example: python load_workflows.py data/raw/*.json")
        sys.exit(1)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for fp in files:
                wf_id = load_one(cur, Path(fp))
                print(f"✔ Loaded {fp} -> workflow {wf_id}")
        conn.commit()
    print("✅ Done.")
