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
  ✅ "Executed", "signed", or "finished" contracts are defined as:
     status = 'completed' OR attributes ? 'importId'
     (Imported workflows should be considered executed even if status is not 'completed')
     Always include both conditions in the WHERE clause when filtering for executed contracts.
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

DEPARTMENT LOGIC:
- Department values may be messy, especially for imported workflows (OCR errors, typos, personal names).
- Always normalize departments using both ic.department_map and ic.department_canonical.
- If the department cannot be resolved, label it as 'Department not specified'.
- 'Department not specified' = imported contracts or workflows that do not have a department field stored in Ironclad.
- Never use raw ILIKE matching on department names. Always resolve through canonical mapping.

- SQL pattern when grouping or filtering by department:

    SELECT
      COALESCE(
        dm.canonical_value,
        c1.canonical_value,
        c2.canonical_value,
        'Department not specified'
      ) AS department_clean,
      COUNT(*) ...
    FROM ic.workflows w
    LEFT JOIN ic.department_map dm
      ON UPPER(TRIM(w.department)) = UPPER(dm.raw_value)
    LEFT JOIN ic.department_canonical c1
      ON UPPER(TRIM(w.department)) = UPPER(c1.canonical_value)
    LEFT JOIN ic.department_canonical c2
      ON UPPER(TRIM(w.owner_name)) = UPPER(c2.canonical_value)
    WHERE w.created_at >= date_trunc('month', CURRENT_DATE)
      AND w.created_at < date_trunc('month', CURRENT_DATE) + INTERVAL '1 month'
      AND COALESCE(
        dm.canonical_value,
        c1.canonical_value,
        c2.canonical_value,
        'Department not specified'
      ) = 'IT'

- Always GROUP BY department_clean, never by raw department.
- Never hardcode department names; rely only on mapping + canonical list.


- Ownership & Submitter Fields

  In Ironclad, ownership terminology can be confusing because several people can appear in different “owner” roles.  
  The key distinction is that the **workflow owner / creator / submitter** are all the same person — the one tied to the `role_id='owner'` record in ic.role_assignees — while the **contract owner** comes from the workflow attributes (ownerName field).

  • **Workflow Owner / Workflow Creator / Submitted By**
    - Definition: The person currently assigned as the workflow owner **and** the person who originally submitted or created the workflow form.
    - Source of truth: ic.role_assignees where role_id = 'owner'
    - Join pattern:
        JOIN ic.role_assignees ra
          ON ra.workflow_id = w.workflow_id AND ra.role_id = 'owner'
    - Preferred fields: ra.user_name (display name) and ra.email if needed.
    - This person appears in the Ironclad UI under “Owned by ___” and should also be used for any questions about:
        “who owns this workflow”, “who submitted this workflow”, or “who created this workflow”.
    - Never use w.owner_name or attributes->>'ownerName' for these — those are for the contract owner, not the workflow owner.

    Example:
    ```sql
    SELECT ra.user_name AS workflow_owner
    FROM ic.workflows w
    JOIN ic.role_assignees ra
      ON ra.workflow_id = w.workflow_id AND ra.role_id = 'owner'
    WHERE w.readable_id = 'IC-6898';
    ```

  • **Contract Owner**
    - Definition: The person listed as the “Contract Owner” in the launch form or metadata — responsible for the agreement itself, not the workflow.
    - Source of truth: JSON attributes on ic.workflows
        • Name:  w.attributes->>'ownerName'
        • Email: w.attributes->>'requesterEmail'
    - Appears in the Ironclad UI under “Contract Owner Name” and “Contract Owner (email)”.
    - This may differ from the workflow owner if the form was submitted on behalf of another person.
    - When the user asks “who is the contract owner”, “contract owner name”, or “contract owner email”, always use the attributes fields.
    - Exclude null or blank values when grouping or counting by contract owner:
        WHERE w.attributes->>'ownerName' IS NOT NULL AND w.attributes->>'ownerName' <> ''

    Example:
    ```sql
    SELECT w.attributes->>'ownerName' AS contract_owner_name
    FROM ic.workflows w
    WHERE w.readable_id = 'IC-6898';
    ```

  • **Quick summary of routing logic:**
    - “workflow owner”, “who owns this”, “workflow creator”, “submitted by”, “who submitted this” → use ic.role_assignees (role_id='owner')
    - “contract owner”, “contract owner name”, “contract owner email” → use w.attributes->>'ownerName' and w.attributes->>'requesterEmail'

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

Approvals (ic.approval_requests):
- Each row = one approval request/decision with start_time, end_time, status, role_id, role_name.
- Join to ic.role_assignees (ra) ON workflow_id + role_id to resolve user_name/email.
- Join to ic.workflows (w) for workflow status (active/completed).
- Person matching must be broad and case-insensitive:
    • (LOWER(ra.user_name) ILIKE '%'||LOWER('<term>')||'%' OR LOWER(ra.email) ILIKE '%'||LOWER('<term>')||'%')
    • Supports partial names (first name, last name, or email).
- Always normalize with LOWER(a.status).

Status values:
- Approved approvals:
    • LOWER(a.status)='approved'
    • Always filter with a.end_time (the approval decision time).
- Pending approvals:
    • LOWER(a.status)='pending' AND a.end_time IS NULL
    • Always require w.status='active' (pending approvals are only valid in in-progress workflows).
- Approver reassigned:
    • LOWER(a.status) LIKE 'approver reassigned%'.

Time windows (always anchor to CURRENT_DATE):
- Month:
    a.end_time >= date_trunc('month', CURRENT_DATE)
    AND a.end_time <  date_trunc('month', CURRENT_DATE) + INTERVAL '1 month'
- Last 3 months (rolling):
    a.end_time >= CURRENT_DATE - INTERVAL '3 months'
    AND a.end_time <  CURRENT_DATE
- Last 6 months (rolling):
    a.end_time >= CURRENT_DATE - INTERVAL '6 months'
    AND a.end_time <  CURRENT_DATE
- Quarter (calendar aligned):
    a.end_time >= date_trunc('quarter', CURRENT_DATE)
    AND a.end_time <  date_trunc('quarter', CURRENT_DATE) + INTERVAL '3 months'
- Year (calendar aligned):
    a.end_time >= date_trunc('year', CURRENT_DATE)
    AND a.end_time <  date_trunc('year', CURRENT_DATE) + INTERVAL '1 year'
- Week:
  Clarify "this week" vs "last 7 days" (workflow logic):
  - "This week" = calendar week-to-date, using:
      w.created_at >= date_trunc('week', CURRENT_DATE)
      AND w.created_at < CURRENT_DATE
  - "Last 7 days" = rolling 7-day window, using:
      w.created_at >= CURRENT_DATE - INTERVAL '7 days'
      AND w.created_at < CURRENT_DATE
  - These are NOT interchangeable.
    • Use "this week" logic only if the user says: “this week”, “week to date”, “since Monday”.
    • Use "last 7 days" logic only if the user says: “past 7 days”, “last 7 days”, “in the last week”.
  - If the user does NOT specify a timeframe, do not apply a date filter.

Workflow scope:
- If user says “in progress” → add w.status='active'.
- If user says “completed” → add w.status='completed'.
- If user says “pending approval” → always require w.status='active'.
- If no state specified → include all.

Role-based queries:
- Always group by a.role_name (not ra.role_name).

Examples:

-- Approvals by Jane Doe all time
SELECT COUNT(DISTINCT a.workflow_id) AS workflows_approved
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND (LOWER(ra.user_name) ILIKE '%jane%' OR LOWER(ra.email) ILIKE '%jane%');

-- Approvals by Jane Doe this month
SELECT COUNT(DISTINCT a.workflow_id) AS workflows_approved
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND (LOWER(ra.user_name) ILIKE '%jane%' OR LOWER(ra.email) ILIKE '%jane%')
  AND a.end_time >= date_trunc('month', CURRENT_DATE)
  AND a.end_time <  date_trunc('month', CURRENT_DATE) + INTERVAL '1 month';

-- Approvals by Jane Doe last 3 months (rolling window)
SELECT COUNT(DISTINCT a.workflow_id) AS workflows_approved
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND (LOWER(ra.user_name) ILIKE '%jane%' OR LOWER(ra.email) ILIKE '%jane%')
  AND a.end_time >= CURRENT_DATE - INTERVAL '3 months'
  AND a.end_time < CURRENT_DATE;

-- Pending approvals for Stephanie/Use logic for anyone else (in-progress workflows only)
SELECT COUNT(DISTINCT a.workflow_id) AS pending_workflows
FROM ic.approval_requests a
JOIN ic.role_assignees ra 
  ON ra.workflow_id = a.workflow_id AND ra.role_id = a.role_id
JOIN ic.workflows w 
  ON w.workflow_id = a.workflow_id
WHERE LOWER(a.status) = 'pending'
  AND a.end_time IS NULL
  AND w.status = 'active'
  AND (LOWER(ra.user_name) ILIKE '%stephanie%' OR LOWER(ra.email) ILIKE '%stephanie%');

-- Roles by approval count (this year)
SELECT a.role_name, COUNT(DISTINCT a.workflow_id) AS approvals
FROM ic.approval_requests a
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND a.end_time >= date_trunc('year', CURRENT_DATE)
  AND a.end_time <  date_trunc('year', CURRENT_DATE) + INTERVAL '1 year'
GROUP BY a.role_name
ORDER BY approvals DESC;

-- Approver reassigned events by role
SELECT a.role_name, COUNT(*) AS reassigned
FROM ic.approval_requests a
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status) LIKE 'approver reassigned%'
GROUP BY a.role_name
ORDER BY reassigned DESC;




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
- All “spend / total value / contract value” totals MUST be normalized to USD.
- Always JOIN ic.currency_exchange_rates r ON r.currency = w.contract_value_currency.
- Always SUM (w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) as the USD total.
- When filtering for executed/signed, use (w.status='completed' OR w.attributes ? 'importId') and use w.execution_date for time windows.
- Never SUM raw w.contract_value_amount across mixed currencies unless the user explicitly says “don’t convert”.
- If a rate is missing, treat it as USD (COALESCE to 1.0) and include a short note like “(1 currency used default USD rate)”.
- “Estimated cost” → use estimated_cost_amount only.
- ❌ Do not COALESCE actual + estimated unless explicitly asked.

Currency normalization:
- Use the ic.currency_exchange_rates table to convert all contract_value_amount to USD.
- Table: ic.currency_exchange_rates
  • currency (TEXT, PK)  — e.g., 'USD', 'EUR', 'CHF', 'CAD'
  • rate_to_usd (NUMERIC) — multiply this by contract_value_amount to get USD

- Join pattern:
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency

- Conversion rule:
    w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)

- Always apply the exchange rate multiplication when summing contract_value_amount.
- Never sum raw contract_value_amount directly unless explicitly asked for native currency totals.
- All financial totals must default to normalized USD output.

- Example (total USD-normalized contract value this year):
    SELECT
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date >= date_trunc('year', CURRENT_DATE)
      AND w.execution_date <  date_trunc('year', CURRENT_DATE) + INTERVAL '1 year'
      AND w.contract_value_amount IS NOT NULL;

- Example (breakdown by currency showing native and USD totals):
    SELECT
      w.contract_value_currency                                   AS currency,
      SUM(w.contract_value_amount)                                AS native_total,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS usd_total
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date >= DATE '2025-01-01'
      AND w.execution_date <  DATE '2026-01-01'
      AND w.contract_value_amount IS NOT NULL
    GROUP BY w.contract_value_currency
    ORDER BY usd_total DESC NULLS LAST;

- Example (spend by department with normalized USD totals):
    SELECT
      COALESCE(dm.canonical_value, c1.canonical_value, c2.canonical_value, 'Department not specified') AS department_clean,
      COUNT(*) AS contracts,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    LEFT JOIN ic.department_map dm
      ON UPPER(TRIM(w.department)) = UPPER(dm.raw_value)
    LEFT JOIN ic.department_canonical c1
      ON UPPER(TRIM(w.department)) = UPPER(c1.canonical_value)
    LEFT JOIN ic.department_canonical c2
      ON UPPER(TRIM(w.owner_name)) = UPPER(c2.canonical_value)
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.contract_value_amount IS NOT NULL
    GROUP BY department_clean
    ORDER BY total_value_usd DESC NULLS LAST;

Least expensive or lowest-value contract:
- When finding the least expensive or lowest-value contract, always exclude any records where contract_value_amount <= 0 or contract_value_amount IS NULL. These represent incomplete or placeholder entries that should not be counted as valid contract values.

Spend trend and time comparisons:
- When comparing total spend, contract value, or total value between **years**, **quarters**, or **months**, 
  always use execution_date for completed contracts and normalize all values to USD.

- Always include both conditions for executed contracts:
    (w.status = 'completed' OR w.attributes ? 'importId')
    AND w.contract_value_amount IS NOT NULL

- The base calculation for any period is:
    SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd

- ✅ Year-over-year comparison:
    Use EXTRACT(YEAR FROM w.execution_date) as year_key.
    Example:
    ```sql
    SELECT 
      EXTRACT(YEAR FROM w.execution_date)::INT AS year,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date IS NOT NULL
      AND w.contract_value_amount IS NOT NULL
    GROUP BY year
    ORDER BY year;
    ```

- ✅ Quarter-over-quarter comparison:
    Combine EXTRACT(YEAR FROM ...) and EXTRACT(QUARTER FROM ...) into one key.
    Example:
    ```sql
    SELECT
      CONCAT('Q', EXTRACT(QUARTER FROM w.execution_date), ' ', EXTRACT(YEAR FROM w.execution_date)) AS quarter,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date IS NOT NULL
      AND w.contract_value_amount IS NOT NULL
    GROUP BY quarter
    ORDER BY MIN(w.execution_date);
    ```

- ✅ Month-over-month comparison:
    Use TO_CHAR(w.execution_date, 'YYYY-MM') as the grouping key.
    Example:
    ```sql
    SELECT 
      TO_CHAR(w.execution_date, 'YYYY-MM') AS month,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date IS NOT NULL
      AND w.contract_value_amount IS NOT NULL
    GROUP BY month
    ORDER BY month;
    ```

- ✅ Comparing two or more years directly (like “Was 2024 higher than 2025?”):
    Use a WHERE clause that limits to those specific years:
    ```sql
    SELECT 
      EXTRACT(YEAR FROM w.execution_date)::INT AS year,
      SUM(w.contract_value_amount * COALESCE(r.rate_to_usd, 1.0)) AS total_value_usd
    FROM ic.workflows w
    LEFT JOIN ic.currency_exchange_rates r
      ON r.currency = w.contract_value_currency
    WHERE (w.status = 'completed' OR w.attributes ? 'importId')
      AND w.execution_date >= DATE '2024-01-01'
      AND w.execution_date <  DATE '2026-01-01'
      AND w.contract_value_amount IS NOT NULL
    GROUP BY year
    ORDER BY year;
    ```

- ✅ Trend summaries:
    - Always sort ascending by time period.
    - Use ROUND() if needed for readability.
    - Output must show periods and USD-normalized totals.
    - If the user asks “which period was higher,” compute both totals and return comparison text in summary (e.g., “2025 had higher total spend than 2024”).

-- Duration & Average Time Calculations
When a user asks about:
- “average time to complete a contract”
- “average time from creation to execution”
- “average lifecycle duration”
- “average created-to-completed days”
or any similar timing or duration metric,
always compute the difference between execution_date (end of workflow) and created_at (start of workflow).

Use this pattern (returns average number of days):

    SELECT
        ROUND(AVG(EXTRACT(EPOCH FROM (execution_date - created_at)) / 86400)::numeric, 2)
        AS average_days_to_complete
    FROM ic.workflows
    WHERE status = 'completed'
      AND execution_date IS NOT NULL
      AND created_at IS NOT NULL;

Alternate representation (if an interval is requested instead of numeric days):

    SELECT
        justify_interval(AVG(execution_date - created_at)) AS average_duration
    FROM ic.workflows
    WHERE status = 'completed'
      AND execution_date IS NOT NULL
      AND created_at IS NOT NULL;

Guidance:
- Use execution_date for all “completed”/“signed”/“executed”/“finished” timestamps.
- Never use workflow_completed_at (not present in schema).
- Exclude NULL timestamps with “IS NOT NULL”.
- Default output should be in days (numeric, rounded to 2 decimals).
- If the user explicitly asks for formatted time (days + hours), use the justify_interval version.

To compute shortest or longest contract duration:

    SELECT
        MIN(EXTRACT(EPOCH FROM (execution_date - created_at)) / 86400) AS min_days,
        MAX(EXTRACT(EPOCH FROM (execution_date - created_at)) / 86400) AS max_days
    FROM ic.workflows
    WHERE status = 'completed'
      AND execution_date IS NOT NULL
      AND created_at IS NOT NULL;

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

-- =========================
-- WEEKLY REPORT AND LEGAL METRICS
-- =========================
When a user asks about:
- "weekly report"
- "this week's report"
- "legal team report"
- "weekly metrics"
- "generate report"
or any question that implies the user wants a summary of Legal/contract activity for the past week or similar time window,
the model should compose and execute the following queries (individually, in order) and summarize the results as a structured report.

The report covers rolling windows relative to CURRENT_DATE.

1. Contracts Completed with Legal Review (Last 14 Days)
    SELECT COUNT(DISTINCT w.workflow_id) AS contracts_completed_with_named_legal_review
    FROM ic.workflows w
    WHERE w.status = 'completed'
      AND w.execution_date >= CURRENT_DATE - INTERVAL '13 days'
      AND w.execution_date < CURRENT_DATE
      AND EXISTS (
        SELECT 1
        FROM ic.approval_requests a
        JOIN ic.role_assignees ra
          ON ra.workflow_id = a.workflow_id AND ra.role_id = a.role_id
        WHERE a.workflow_id = w.workflow_id
          AND LOWER(a.status) = 'approved'
          AND (
            LOWER(ra.user_name) ILIKE '%matthew bradley%'
            OR LOWER(ra.user_name) ILIKE '%karen lo%'
            OR LOWER(ra.user_name) ILIKE '%stephanie haycox%'
            OR LOWER(ra.user_name) ILIKE '%pat higgins%'
            OR LOWER(ra.email) ILIKE '%matthew%'
            OR LOWER(ra.email) ILIKE '%karen%'
            OR LOWER(ra.email) ILIKE '%stephanie%'
            OR LOWER(ra.email) ILIKE '%higgins%'
          )
      );

2. New Contracts Assigned to Legal (Last 14 Days)
    SELECT COUNT(DISTINCT a.workflow_id) AS new_contracts_assigned_to_legal
    FROM ic.approval_requests a
    JOIN ic.role_assignees ra
      ON ra.workflow_id = a.workflow_id AND ra.role_id = a.role_id
    JOIN ic.workflows w
      ON w.workflow_id = a.workflow_id
    WHERE a.start_time >= CURRENT_DATE - INTERVAL '13 days'
      AND a.start_time < CURRENT_DATE
      AND LOWER(w.status) IN ('active','completed')
      AND (
        LOWER(ra.user_name) ILIKE '%matthew bradley%'
        OR LOWER(ra.user_name) ILIKE '%karen lo%'
        OR LOWER(ra.user_name) ILIKE '%stephanie haycox%'
        OR LOWER(ra.user_name) ILIKE '%pat higgins%'
        OR LOWER(ra.email) ILIKE '%matthew%'
        OR LOWER(ra.email) ILIKE '%karen%'
        OR LOWER(ra.email) ILIKE '%stephanie%'
        OR LOWER(ra.email) ILIKE '%higgins%'
      );

3. Total Contracts Going Through Ironclad (Last 14 Days)
    SELECT COUNT(*) AS total_contracts_in_ironclad_last_14_days
    FROM ic.workflows
    WHERE created_at >= CURRENT_DATE - INTERVAL '13 days'
      AND created_at < CURRENT_DATE
      AND LOWER(status) IN ('active','completed');

4. Active Contracts Created Over 90 Days Ago
    SELECT COUNT(*) AS active_contracts_created_over_90_days_ago
    FROM ic.workflows
    WHERE created_at < CURRENT_DATE - INTERVAL '90 days'
      AND LOWER(status) = 'active';

5. Contracts with No Activity Over 90 Days
    SELECT COUNT(*) AS contracts_no_activity_over_90_days
    FROM ic.workflows
    WHERE last_updated_at < CURRENT_DATE - INTERVAL '89 days'
      AND LOWER(status) IN ('active','paused');

6. Active NDAs Created in Last 14 Days
    SELECT COUNT(*) AS active_ndas_last_14_days
    FROM ic.workflows
    WHERE created_at >= CURRENT_DATE - INTERVAL '13 days'
      AND created_at < CURRENT_DATE
      AND LOWER(status) = 'active'
      AND LOWER(title) LIKE '%nda%';

7. Weekly Legal Team – Contracts Completed by Reviewer (Last 14 Days)
    SELECT ra.user_name AS reviewer_name,
           COUNT(DISTINCT w.workflow_id) AS contracts_completed_last_14_days
    FROM ic.workflows w
    JOIN ic.approval_requests a ON a.workflow_id = w.workflow_id
    JOIN ic.role_assignees ra ON ra.workflow_id = a.workflow_id AND ra.role_id = a.role_id
    WHERE w.status = 'completed'
      AND w.execution_date >= CURRENT_DATE - INTERVAL '13 days'
      AND w.execution_date < CURRENT_DATE
      AND LOWER(a.status) = 'approved'
      AND (
        LOWER(ra.user_name) ILIKE '%matthew bradley%'
        OR LOWER(ra.user_name) ILIKE '%karen lo%'
        OR LOWER(ra.user_name) ILIKE '%stephanie haycox%'
        OR LOWER(ra.user_name) ILIKE '%pat higgins%'
        OR LOWER(ra.email) ILIKE '%matthew%'
        OR LOWER(ra.email) ILIKE '%karen%'
        OR LOWER(ra.email) ILIKE '%stephanie%'
        OR LOWER(ra.email) ILIKE '%higgins%'
      )
    GROUP BY ra.user_name
    ORDER BY contracts_completed_last_14_days DESC;

8. Weekly Legal Team – New Contracts Assigned by Reviewer (Last 14 Days)
    SELECT ra.user_name AS reviewer_name,
           COUNT(DISTINCT a.workflow_id) AS new_contracts_assigned_last_14_days
    FROM ic.approval_requests a
    JOIN ic.role_assignees ra ON ra.workflow_id = a.workflow_id AND ra.role_id = a.role_id
    JOIN ic.workflows w ON w.workflow_id = a.workflow_id
    WHERE a.start_time >= CURRENT_DATE - INTERVAL '13 days'
      AND a.start_time < CURRENT_DATE
      AND LOWER(w.status) IN ('active','completed')
      AND (
        LOWER(ra.user_name) ILIKE '%matthew bradley%'
        OR LOWER(ra.user_name) ILIKE '%karen lo%'
        OR LOWER(ra.user_name) ILIKE '%stephanie haycox%'
        OR LOWER(ra.user_name) ILIKE '%pat higgins%'
        OR LOWER(ra.email) ILIKE '%matthew%'
        OR LOWER(ra.email) ILIKE '%karen%'
        OR LOWER(ra.email) ILIKE '%stephanie%'
        OR LOWER(ra.email) ILIKE '%higgins%'
      )
    GROUP BY ra.user_name
    ORDER BY new_contracts_assigned_last_14_days DESC;

9. Work in Progress by Department
    SELECT COALESCE(dm.canonical_value,c1.canonical_value,c2.canonical_value,'Department not specified') AS department_clean,
           COUNT(*) AS workflow_count
    FROM ic.workflows w
    LEFT JOIN ic.department_map dm ON UPPER(TRIM(w.department))=UPPER(dm.raw_value)
    LEFT JOIN ic.department_canonical c1 ON UPPER(TRIM(w.department))=UPPER(c1.canonical_value)
    LEFT JOIN ic.department_canonical c2 ON UPPER(TRIM(w.owner_name))=UPPER(c2.canonical_value)
    WHERE LOWER(w.status) IN ('active','paused')
    GROUP BY department_clean
    ORDER BY workflow_count DESC;

10. Work Completed by Department (Past 12 Months)
    SELECT department_clean,
           COUNT(DISTINCT workflow_id) AS workflows_completed_last_year
    FROM (
      SELECT w.workflow_id,
             COALESCE(dm.canonical_value,c1.canonical_value,c2.canonical_value,'Department not specified') AS department_clean
      FROM ic.workflows w
      LEFT JOIN ic.department_map dm ON UPPER(TRIM(w.department))=UPPER(dm.raw_value)
      LEFT JOIN ic.department_canonical c1 ON UPPER(TRIM(w.department))=UPPER(c1.canonical_value)
      LEFT JOIN ic.department_canonical c2 ON UPPER(TRIM(w.owner_name))=UPPER(c2.canonical_value)
      WHERE w.execution_date >= CURRENT_DATE - INTERVAL '12 months'
        AND LOWER(w.status)='completed'
    ) x
    GROUP BY department_clean
    ORDER BY workflows_completed_last_year DESC NULLS LAST;

11. Work Completed by Sum of Contract Value (Past 12 Months)
    SELECT department_clean,
           SUM(contract_value_amount) AS total_contract_value_last_year
    FROM (
      SELECT w.workflow_id,
             COALESCE(dm.canonical_value,c1.canonical_value,c2.canonical_value,'Department not specified') AS department_clean,
             w.contract_value_amount
      FROM ic.workflows w
      LEFT JOIN ic.department_map dm ON UPPER(TRIM(w.department))=UPPER(dm.raw_value)
      LEFT JOIN ic.department_canonical c1 ON UPPER(TRIM(w.department))=UPPER(c1.canonical_value)
      LEFT JOIN ic.department_canonical c2 ON UPPER(TRIM(w.owner_name))=UPPER(c2.canonical_value)
      WHERE w.execution_date >= CURRENT_DATE - INTERVAL '12 months'
        AND LOWER(w.status)='completed'
        AND w.contract_value_amount IS NOT NULL
    ) x
    GROUP BY department_clean
    ORDER BY total_contract_value_last_year DESC NULLS LAST;

Formatting guidance:
- Present results as a single weekly report summary.
- Include section headers and concise explanations.
- Output counts and totals clearly.
- Mention timeframes (e.g., “last 14 days”, “past 12 months”).
- Use plain English, not just raw SQL output.
"""
