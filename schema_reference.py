SCHEMA_DESCRIPTION = """
Database schema (schema = ic)

-- Core workflow metadata (used by the GPT SQL path; do not invent columns)
Table: workflows
- workflow_id (TEXT, PK)
- readable_id (TEXT)
- ironclad_id (TEXT)
- title (TEXT)
- template (TEXT)

- status (TEXT)
  ✅ Completed workflows → 'completed'
  ✅ In-progress workflows → 'active'
  ❌ Do not filter by 'In Progress' (not a stored value)

- step (TEXT)  -- only present for in-progress
  ✅ Allowed values (case-sensitive): 'Create', 'Review', 'Sign', 'Archive'
  ✅ Natural language → canonical:
      "in create", "creation"          → 'Create'
      "in review", "under review"      → 'Review'
      "in sign", "signing"             → 'Sign'
      "in archive", "archived"         → 'Archive'
  ⚠️ Always filter with exact DB values.

- is_complete (BOOLEAN)
- is_cancelled (BOOLEAN)

- created_at (TIMESTAMPTZ)      -- workflow creation timestamp
- last_updated_at (TIMESTAMPTZ)
- record_type (TEXT)            -- contract type (NDA, MSA, SOW, etc.)
  ✅ Use record_type for “How many NDAs/MSAs/SOWs?”
  ❌ Do not use document_type for contract type classification.

- legal_entity (TEXT)
  ✅ When grouping/displaying, wrap with COALESCE(legal_entity, 'Unspecified Legal Entity').

- department (TEXT)
  ✅ When grouping or displaying, always normalize via department_map + department_canonical.
  ✅ Use COALESCE(dm.canonical_value, c1.canonical_value, c2.canonical_value, 'Department not specified') AS department_clean.
  ✅ Join logic:
      LEFT JOIN ic.department_map dm
        ON UPPER(TRIM(w.department)) = UPPER(dm.raw_value)
      LEFT JOIN ic.department_canonical c1
        ON UPPER(TRIM(w.department)) = UPPER(c1.canonical_value)
      LEFT JOIN ic.department_canonical c2
        ON UPPER(TRIM(w.owner_name)) = UPPER(c2.canonical_value)
  ✅ Always GROUP BY department_clean.
  ⚠️ Prevents duplicates (e.g., 'CLINICAL' vs 'Clinical') and collapses bad OCR values into canonicals or 'Department not specified'.

Recommended SQL patterns (metadata)
- Spend by department (actuals only, with normalization):
    SELECT
      COALESCE(
        dm.canonical_value,
        c1.canonical_value,
        c2.canonical_value,
        'Department not specified'
      ) AS department_clean,
      COUNT(*) AS contracts,
      SUM(w.contract_value_amount) AS total_value
    FROM ic.workflows w
    LEFT JOIN ic.department_map dm
      ON UPPER(TRIM(w.department)) = UPPER(dm.raw_value)
    LEFT JOIN ic.department_canonical c1
      ON UPPER(TRIM(w.department)) = UPPER(c1.canonical_value)
    LEFT JOIN ic.department_canonical c2
      ON UPPER(TRIM(w.owner_name)) = UPPER(c2.canonical_value)
    WHERE w.contract_value_amount IS NOT NULL
    GROUP BY department_clean
    ORDER BY total_value DESC NULLS LAST;


- owner_name (TEXT)
- paper_source (TEXT)
- document_type (TEXT)          -- not the contract type category
- agreement_date (TIMESTAMPTZ)
- execution_date (TIMESTAMPTZ)  -- use for executed/signed filters and year breakdowns
- expiration_date (TIMESTAMPTZ) -- use for expiry questions

- po_number (TEXT)
- requisition_number (TEXT)

- contract_value_amount (NUMERIC)
- contract_value_currency (TEXT)

- estimated_cost_amount (NUMERIC)
- estimated_cost_currency (TEXT)

- counterparty_name (TEXT, nullable)
  ✅ Primary field for vendor/counterparty filters and counts.
  ✅ When filtering by vendor/counterparty, prefer counterparty_name first;
     fallback to COALESCE(legal_entity,'') ILIKE or title ILIKE.

- attributes (JSONB)
  ↳ Contains additional UI metadata, including Priority.
  ✅ Observed priority values: 'High Priority', 'Medium/Low Priority', NULL
  ✅ Filter via LOWER(attributes->>'priority').
     Examples:
       WHERE LOWER(attributes->>'priority') = 'high priority'
       WHERE LOWER(attributes->>'priority') = 'medium/low priority'


Approvals:
- Approvals are stored in ic.approvals.
- Link to ic.role_assignees ON workflow_id + role_id to resolve the actual user_name/email.
- status values include 'approved', 'pending', etc.
- To ask “How many contracts has [NAME] approved?”:
    • Filter ra.user_name ILIKE '%Name%'
    • AND a.status='approved'
- To ask about pending approvals by person:
    • Filter ra.user_name ILIKE '%Name%'
    • AND a.status='pending'
- Workflow scope:
    • Completed workflows: w.status='completed'
    • In-progress workflows: w.status='active'


Quarter logic (calendar-aligned):
- Q1 = Jan–Mar
- Q2 = Apr–Jun
- Q3 = Jul–Sep
- Q4 = Oct–Dec

Relative quarters (always based on CURRENT_DATE):
- "Last quarter":
    execution_date >= date_trunc('quarter', CURRENT_DATE) - INTERVAL '3 months'
    AND execution_date <  date_trunc('quarter', CURRENT_DATE)
- "This quarter":
    execution_date >= date_trunc('quarter', CURRENT_DATE)
    AND execution_date <  date_trunc('quarter', CURRENT_DATE) + INTERVAL '3 months'
- "Next quarter":
    execution_date >= date_trunc('quarter', CURRENT_DATE) + INTERVAL '3 months'
    AND execution_date <  date_trunc('quarter', CURRENT_DATE) + INTERVAL '6 months'

Explicit quarters (when user says Q1/Q2/Q3/Q4 YYYY):
- Use EXTRACT(YEAR FROM execution_date)=YYYY
  AND EXTRACT(QUARTER FROM execution_date)=N
  (Q1=1, Q2=2, Q3=3, Q4=4)

Important:
- Do not approximate with “last 3 months.”
- Always anchor to CURRENT_DATE and align with calendar quarters.

Financial rules
- “Spend/total spend” → use contract_value_amount only.
- “Estimated cost”    → use estimated_cost_amount only.
- ❌ Do not COALESCE actual + estimated unless explicitly asked.
- Currency handling:
  If summing across multiple currencies, either group by currency or state that totals mix currencies.

Recommended SQL patterns (metadata)
- Spend by department (actuals only):
    SELECT
      COALESCE(department, 'Department not specified') AS department,
      COUNT(*)                                        AS contracts,
      SUM(contract_value_amount)                      AS total_value
    FROM ic.workflows
    WHERE contract_value_amount IS NOT NULL
    GROUP BY COALESCE(department, 'Department not specified')
    ORDER BY total_value DESC NULLS LAST;

- Estimated cost by department:
    SELECT
      COALESCE(department, 'Department not specified') AS department,
      COUNT(*)                                        AS contracts,
      SUM(estimated_cost_amount)                      AS total_estimated
    FROM ic.workflows
    WHERE estimated_cost_amount IS NOT NULL
    GROUP BY COALESCE(department, 'Department not specified')
    ORDER BY total_estimated DESC NULLS LAST;

- Contracts expiring this year:
    SELECT COUNT(*) AS contract_count
    FROM ic.workflows
    WHERE expiration_date >= date_trunc('year', CURRENT_DATE)
      AND expiration_date <  date_trunc('year', CURRENT_DATE) + INTERVAL '1 year';

- Priority breakdown:
    SELECT LOWER(attributes->>'priority') AS priority, COUNT(*) AS contracts
    FROM ic.workflows
    GROUP BY 1
    ORDER BY contracts DESC;

-- Vendor / Counterparty guidance
Preferred filter order:
  1) w.counterparty_name ILIKE '%<vendor>%'
  2) OR COALESCE(w.legal_entity,'') ILIKE '%<vendor>%'
  3) OR w.title ILIKE '%<vendor>%'

Example (count + sample IDs):
    SELECT COUNT(*) AS contracts_with_vendor,
           ARRAY(
             SELECT w.readable_id
             FROM ic.workflows w
             WHERE (
               COALESCE(w.counterparty_name,'') ILIKE '%Lonza%'
               OR COALESCE(w.legal_entity,'')  ILIKE '%Lonza%'
               OR w.title ILIKE '%Lonza%'
             )
             ORDER BY w.readable_id
             LIMIT 5
           ) AS example_ids
    FROM ic.workflows w
    WHERE (
      COALESCE(w.counterparty_name,'') ILIKE '%Lonza%'
      OR COALESCE(w.legal_entity,'')  ILIKE '%Lonza%'
      OR w.title ILIKE '%Lonza%'
    );

-- Documents & participants (for joins/lists)
Table: documents
- doc_id (BIGSERIAL, PK)
- workflow_id (TEXT, FK → workflows)
- doc_type (TEXT)
- version (TEXT)
- version_number (INT)
- filename (TEXT)
- storage_key (TEXT)
- download_path (TEXT)
- last_modified_at (TIMESTAMPTZ)
- last_modified_author (JSONB)

Table: roles
- workflow_id (TEXT, FK → workflows)
- role_id (TEXT)
- display_name (TEXT)

Table: role_assignees
- workflow_id (TEXT, FK → workflows)
- role_id (TEXT)
- user_id (TEXT)
- user_name (TEXT)
- email (TEXT)

Table: participants
- workflow_id (TEXT, FK → workflows)
- user_id (TEXT)
- email (TEXT)

Table: comments
- comment_id (TEXT, PK)
- workflow_id (TEXT, FK → workflows)
- author (JSONB)  -- includes displayName, email, userId
- author_email (TEXT)        -- convenience copy of email
- author_user_id (TEXT)      -- convenience copy of userId
- ts (TIMESTAMPTZ)
- message (TEXT)
- is_external (BOOLEAN)

Name matching guidance (comments)
- Prefer matching on email when provided.
- Otherwise case-insensitive prefix match on displayName from author JSON:
    WHERE LOWER(author->>'displayName') LIKE LOWER('<prefix>%')
- Liberal matching:
    WHERE LOWER(author->>'displayName') LIKE LOWER('<prefix>%')
       OR LOWER(author_email)           LIKE LOWER('<prefix>%')

Comment timing guidance (spans)
- Compute spans per workflow via MIN(ts) and MAX(ts).
- Exclude workflows with only 1 comment (HAVING COUNT(*) > 1) to avoid zero spans.
- Examples:
    SELECT AVG(last_ts - first_ts) AS avg_comment_span
    FROM (
        SELECT workflow_id, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS comment_count
        FROM ic.comments
        GROUP BY workflow_id
        HAVING COUNT(*) > 1
    ) spans;

    SELECT MIN(last_ts - first_ts) AS shortest_span
    FROM (
        SELECT workflow_id, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS comment_count
        FROM ic.comments
        GROUP BY workflow_id
        HAVING COUNT(*) > 1
    ) spans;

    SELECT MAX(last_ts - first_ts) AS longest_span
    FROM (
        SELECT workflow_id, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS comment_count
        FROM ic.comments
        GROUP BY workflow_id
        HAVING COUNT(*) > 1
    ) spans;

-- Canonical clauses (authoritative extraction table)
Table: clauses
- workflow_id (TEXT, FK → workflows)
- clause_name (TEXT)   -- canonical slug (e.g., clause_termination-for-convenience)
- clause_value (JSONB) -- extracted clause text/value

Routing guidance:
- If the user says "clause"/"clauses", answer via SQL on ic.clauses.
  • Count workflows with a clause → COUNT(DISTINCT workflow_id).
  • When listing, join back to workflows for readable_id/title.
- If "clause" is NOT mentioned, use text search over ic.contract_chunks (see below).

Examples:
  SELECT COUNT(DISTINCT workflow_id)
  FROM ic.clauses
  WHERE clause_name ILIKE 'clause_%termination%';

  SELECT c.workflow_id, w.readable_id, c.clause_name
  FROM ic.clauses c
  JOIN ic.workflows w ON w.workflow_id = c.workflow_id
  WHERE c.clause_name ILIKE 'clause_%indemn%';


-- Imported workflows (Ironclad “imports”)
Imported contracts are identified by the presence of "importId" in attributes.

Examples:
- Count all imported contracts:
    SELECT COUNT(*) 
    FROM ic.workflows
    WHERE attributes ? 'importId';

- List imported contracts with governing law California:
    SELECT readable_id, attributes->'governingLaw'->>'value' AS governing_law
    FROM ic.workflows
    WHERE attributes ? 'importId'
      AND attributes->'governingLaw'->>'value' ILIKE '%California%';

- Show imported contracts with a one-year agreement term:
    SELECT readable_id, attributes->'agreementTerm'->>'value' AS agreement_term
    FROM ic.workflows
    WHERE attributes ? 'importId'
      AND attributes->'agreementTerm'->>'value' ILIKE '%one year%';

-- Imported contract date logic
- Imported contracts do not use created_at. Instead, use:
    (attributes->'smartImportProperty_predictionDate'->>'value')::timestamptz

- To count or filter imported contracts by month:
    SELECT DATE_TRUNC('month', (attributes->'smartImportProperty_predictionDate'->>'value')::timestamptz) AS month,
           COUNT(*)
    FROM ic.workflows
    WHERE attributes ? 'importId'
      AND DATE_TRUNC('month', (attributes->'smartImportProperty_predictionDate'->>'value')::timestamptz)
          = DATE_TRUNC('month', DATE '2025-08-01')
    GROUP BY month

⚠️ Do NOT use HAVING with the alias "month".  
Always repeat the DATE_TRUNC(...) expression in the WHERE clause when filtering by a specific month.

-- Text search corpus + embeddings (for mention/snippet/semantic)
Table: contract_texts
- readable_id (TEXT, PK)
- workflow_id (TEXT, nullable)
- title (TEXT)
- text (TEXT)
- text_sha256 (TEXT)
- token_count (INT)
- source_status (TEXT)
- updated_at (TIMESTAMPTZ)

Table: contract_chunks
- readable_id (TEXT)
- workflow_id (TEXT, nullable)
- chunk_id (BIGINT)                -- 0..N-1 per document
- start_char (INT)
- end_char (INT)
- chunk_text (TEXT)
- embedding (vector(1536))         -- pgvector (cosine)
- text_sha256 (TEXT)

Indexes
- GIN trigram over chunk_text for fast ILIKE '%term%' search.
- IVFFLAT vector_cosine_ops over embedding for semantic retrieval.

Deterministic patterns (counts from text)
- Count contracts that mention a term:
    WITH m AS (
      SELECT DISTINCT readable_id
      FROM ic.contract_chunks
      WHERE chunk_text ILIKE '%<term>%'
    )
    SELECT COUNT(*) FROM m;

- Boolean counts (AND / OR / NOT):
    WITH m AS (
      SELECT DISTINCT readable_id
      FROM ic.contract_chunks
      WHERE chunk_text ILIKE '%<A>%' AND NOT (chunk_text ILIKE '%<B>%')
    )
    SELECT COUNT(*) FROM m;

- Snippets (examples where a term appears):
    SELECT readable_id, chunk_id, LEFT(chunk_text, 300)
    FROM ic.contract_chunks
    WHERE chunk_text ILIKE '%<term>%'
    LIMIT 10;

Proximity snippets (regex)
- Two terms within ~N chars (case-insensitive, dotall):
    SELECT readable_id, chunk_id, LEFT(chunk_text, 300)
    FROM ic.contract_chunks
    WHERE chunk_text ~ '(?is)(term1.{0,120}term2|term2.{0,120}term1)'
    LIMIT 10;

Semantic patterns (pgvector)
- Query embedding → nearest chunks, then group by readable_id:
    WITH top_chunks AS (
      SELECT readable_id, chunk_id, chunk_text,
             (embedding <=> '<[dims floats]>'::vector) AS cosine_distance
      FROM ic.contract_chunks
      ORDER BY embedding <=> '<[dims floats]>'::vector
      LIMIT 40
    )
    SELECT readable_id,
           MIN(cosine_distance) AS best_distance,
           ARRAY_AGG(LEFT(chunk_text, 200))[:3] AS example_snippets
    FROM top_chunks
    GROUP BY readable_id
    ORDER BY best_distance ASC
    LIMIT 10;

Notes:
- When showing top-N IDs inside answers, Postgres does not allow `LIMIT` inside `ARRAY_AGG`.
- Always use a subselect array instead, like:

    ARRAY(
      SELECT readable_id
      FROM matches
      ORDER BY readable_id
      LIMIT 5
    )

- Do not attempt: ARRAY_AGG(readable_id ORDER BY readable_id LIMIT 5)  ← this will error.
"""
