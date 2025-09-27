from db import get_conn

DDL = """
CREATE SCHEMA IF NOT EXISTS ic;

-- Main workflow header (broad core + full raw for no-loss)
CREATE TABLE IF NOT EXISTS ic.workflows (
  workflow_id            TEXT PRIMARY KEY,         -- workflow.id
  readable_id            TEXT,                     -- attributes.readableId (e.g., IC-6734)
  ironclad_id            TEXT,                     -- attributes.ironcladId if present
  title                  TEXT,
  template               TEXT,
  status                 TEXT,
  step                   TEXT,
  is_complete            BOOLEAN,
  is_cancelled           BOOLEAN,
  created_at             TIMESTAMPTZ,
  last_updated_at        TIMESTAMPTZ,
  -- frequently used props (nullable)
  record_type            TEXT,
  legal_entity           TEXT,
  department             TEXT,
  owner_name             TEXT,
  paper_source           TEXT,
  document_type          TEXT,
  agreement_date         TIMESTAMPTZ,
  execution_date         TIMESTAMPTZ,
  po_number              TEXT,
  requisition_number     TEXT,
  estimated_cost_amount  NUMERIC,
  estimated_cost_currency TEXT,
  contract_value_amount  NUMERIC,
  contract_value_currency TEXT,
  sign_step_completed_at TIMESTAMPTZ,              -- will populate when available
  workflow_completed_at  TIMESTAMPTZ,              -- will populate when available
  -- everything else for lossless storage
  attributes             JSONB NOT NULL,
  field_schema           JSONB NOT NULL,           -- “schema” block from API (field definitions)
  raw_workflow           JSONB NOT NULL            -- the full workflow JSON (header only)
);

-- Documents associated with a workflow (draft/signed/packets)
CREATE TABLE IF NOT EXISTS ic.documents (
  doc_id           BIGSERIAL PRIMARY KEY,
  workflow_id      TEXT REFERENCES ic.workflows(workflow_id) ON DELETE CASCADE,
  doc_type         TEXT,                    -- 'draft' | 'signed' | 'sentSignaturePacket' | 'partiallySigned' | etc
  version          TEXT,
  version_number   INT,
  filename         TEXT,
  storage_key      TEXT,
  download_path    TEXT,
  last_modified_at TIMESTAMPTZ,
  last_modified_author JSONB
);

-- Roles and their assignees (approvers/signers/owner/etc.)
CREATE TABLE IF NOT EXISTS ic.roles (
  workflow_id  TEXT,
  role_id      TEXT,
  display_name TEXT,
  PRIMARY KEY (workflow_id, role_id)
);

-- Clauses (contract clauses/properties tied to workflows)
CREATE TABLE IF NOT EXISTS ic.clauses (
  workflow_id TEXT REFERENCES ic.workflows(workflow_id) ON DELETE CASCADE,
  clause_name TEXT,
  clause_value JSONB,
  PRIMARY KEY (workflow_id, clause_name)
);


-- Role assignees
CREATE TABLE IF NOT EXISTS ic.role_assignees (
  workflow_id  TEXT,
  role_id      TEXT,
  user_id      TEXT,
  user_name    TEXT,
  email        TEXT,
  PRIMARY KEY (workflow_id, role_id, email)
);

-- Participants
CREATE TABLE IF NOT EXISTS ic.participants (
  workflow_id TEXT,
  user_id     TEXT,
  email       TEXT,
  PRIMARY KEY (workflow_id, user_id, email)
);

-- Comments (discussion)
CREATE TABLE IF NOT EXISTS ic.comments (
  comment_id   TEXT PRIMARY KEY,  -- comment.id
  workflow_id  TEXT REFERENCES ic.workflows(workflow_id) ON DELETE CASCADE,
  author       JSONB,
  author_email TEXT,
  author_user_id TEXT,
  ts           TIMESTAMPTZ,
  message      TEXT,
  is_external  BOOLEAN,
  mentioned    JSONB,
  replied_to   JSONB,
  reactions    JSONB
);

-- Step states (e.g., approvals/signatures)
CREATE TABLE IF NOT EXISTS ic.step_states (
  workflow_id TEXT,
  step_name   TEXT,   -- 'approvals' | 'signatures' | etc
  state       TEXT,   -- 'completed', etc
  PRIMARY KEY (workflow_id, step_name)
);


-- Turn history (workflow steps with timing)
CREATE TABLE IF NOT EXISTS ic.turn_history (
  workflow_id   TEXT,
  seq_num       INT,
  step_name     TEXT,
  started_at    TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  raw           JSONB,
  PRIMARY KEY (workflow_id, seq_num)
);

-- === Contract full text (per workflow) ===
CREATE TABLE IF NOT EXISTS ic.contract_texts (
  workflow_id   TEXT PRIMARY KEY REFERENCES ic.workflows(workflow_id) ON DELETE CASCADE,
  readable_id   TEXT,          -- e.g., "IC-6439" (denormalized for convenience)
  title         TEXT,          -- optional denorm from workflows
  text          TEXT NOT NULL, -- full extracted text
  text_sha256   TEXT NOT NULL, -- to detect changes / avoid reprocessing
  token_count   INTEGER,       -- optional: for budgeting/debug
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Fast phrase/substring search over the full text
CREATE INDEX IF NOT EXISTS idx_contract_texts_trgm
  ON ic.contract_texts USING gin (text gin_trgm_ops);

-- Handy lookup by readable ID
CREATE INDEX IF NOT EXISTS idx_contract_texts_readable
  ON ic.contract_texts (readable_id);


-- === Contract chunks (for semantic search and RAG) ===
CREATE TABLE IF NOT EXISTS ic.contract_chunks (
  workflow_id   TEXT REFERENCES ic.workflows(workflow_id) ON DELETE CASCADE,
  chunk_id      BIGSERIAL PRIMARY KEY,
  section_hint  TEXT,          -- optional: heading or clause name
  start_page    INT,
  end_page      INT,
  start_char    INT,
  end_char      INT,
  chunk_text    TEXT NOT NULL,
  embedding     vector(1536),  -- uses pgvector extension (Azure: "vector")
  text_sha256   TEXT NOT NULL
);

-- Trigram index for fast keyword lookups inside chunks
CREATE INDEX IF NOT EXISTS idx_contract_chunks_trgm
  ON ic.contract_chunks USING gin (chunk_text gin_trgm_ops);

-- Vector index for ANN search (semantic similarity)
CREATE INDEX IF NOT EXISTS idx_contract_chunks_vec
  ON ic.contract_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- useful indexes
CREATE INDEX IF NOT EXISTS idx_workflows_title ON ic.workflows USING gin (to_tsvector('english', title));
CREATE INDEX IF NOT EXISTS idx_workflows_attributes ON ic.workflows USING gin (attributes);
CREATE INDEX IF NOT EXISTS idx_comments_workflow_ts ON ic.comments (workflow_id, ts);
"""

if __name__ == "__main__":
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    print("✅ Schema created/verified.")
