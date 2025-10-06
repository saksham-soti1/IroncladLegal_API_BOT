# load_imported_workflows.py
import os, json
from pathlib import Path
from db import get_conn

RAW_DIR = Path("data/raw_imported")

def upsert_imported(cur, rec: dict):
    props = rec.get("properties", {})
    wf_id = rec.get("id")  # record id (UUID)
    readable_id = rec.get("ironcladId")  # e.g. IC-5906
    title = rec.get("name")
    record_type = rec.get("type")
    last_updated_at = rec.get("lastUpdated")

    # --- Core metadata from properties (only columns that exist in schema) ---
    counterparty = props.get("counterpartyName", {}).get("value")
    department = props.get("department", {}).get("value")
    legal_entity = props.get("legalEntity", {}).get("value")
    owner_name = props.get("contractOwner", {}).get("value")

    agreement_date = props.get("agreementDate", {}).get("value")
    execution_date = props.get("standard_executedDate", {}).get("value")
    expiration_date = props.get("expirationDate", {}).get("value")

    contract_value_amount = None
    contract_value_currency = None
    if "contractValue" in props:
        val = props["contractValue"].get("value", {})
        contract_value_amount = val.get("amount")
        contract_value_currency = val.get("currency")

    # --- Keep all metadata in attributes JSON ---
    merged_attrs = dict(props)

    # --- Insert/update workflow row ---
    cur.execute("""
        INSERT INTO ic.workflows (
            workflow_id, readable_id, title, record_type,
            counterparty_name, department, legal_entity, owner_name,
            agreement_date, execution_date, expiration_date,
            attributes, field_schema, raw_workflow,
            contract_value_amount, contract_value_currency,
            last_updated_at
        )
        VALUES (%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,
                %s)
        ON CONFLICT (workflow_id) DO UPDATE SET
            title = EXCLUDED.title,
            record_type = EXCLUDED.record_type,
            counterparty_name = EXCLUDED.counterparty_name,
            department = EXCLUDED.department,
            legal_entity = EXCLUDED.legal_entity,
            owner_name = EXCLUDED.owner_name,
            agreement_date = EXCLUDED.agreement_date,
            execution_date = EXCLUDED.execution_date,
            expiration_date = EXCLUDED.expiration_date,
            contract_value_amount = EXCLUDED.contract_value_amount,
            contract_value_currency = EXCLUDED.contract_value_currency,
            attributes = EXCLUDED.attributes,
            field_schema = EXCLUDED.field_schema,
            raw_workflow = EXCLUDED.raw_workflow,
            last_updated_at = EXCLUDED.last_updated_at
    """, (
        wf_id, readable_id, title, record_type,
        counterparty, department, legal_entity, owner_name,
        agreement_date, execution_date, expiration_date,
        json.dumps(merged_attrs), json.dumps({}), json.dumps(rec),
        contract_value_amount, contract_value_currency,
        last_updated_at
    ))

    # --- Insert clauses ---
    for key, val in props.items():
        if key.startswith("clause_") and isinstance(val, dict):
            clause_val = val.get("value", {})
            clause_name = key
            clause_text = clause_val.get("clauseText")
            if clause_text:
                cur.execute("""
                    INSERT INTO ic.clauses (workflow_id, clause_name, clause_value)
                    VALUES (%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (wf_id, clause_name, json.dumps(clause_val)))

        # --- Insert attachments metadata into ic.documents (auto id, no collisions) ---
    attachments = rec.get("attachments", {})
    for key, att in attachments.items():
        if not isinstance(att, dict):
            continue
        filename = att.get("filename")
        storage_key = att.get("href")

        # skip if already present (so the loader is idempotent)
        cur.execute("""
            SELECT 1
            FROM ic.documents
            WHERE workflow_id = %s
              AND doc_type = %s
              AND COALESCE(filename, '') = COALESCE(%s, '')
            LIMIT 1
        """, (wf_id, key, filename))
        if cur.fetchone():
            continue

        # let BIGSERIAL doc_id auto-generate
        cur.execute("""
            INSERT INTO ic.documents (workflow_id, doc_type, filename, storage_key)
            VALUES (%s, %s, %s, %s)
        """, (wf_id, key, filename, storage_key))



def main():
    with get_conn() as conn, conn.cursor() as cur:
        count = 0
        for path in RAW_DIR.glob("*.json"):
            data = json.loads(path.read_text())
            if data.get("source", {}).get("type") == "import_project":
                upsert_imported(cur, data)
                print(f"✔ loaded imported {data.get('ironcladId')}")
                count += 1
        conn.commit()
        print(f"✅ Finished loading {count} imported records")


if __name__ == "__main__":
    main()
