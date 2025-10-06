import os, re, json, textwrap
from typing import Any, Dict, List, Tuple, Optional
from decimal import Decimal
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from openai import OpenAI

from db import get_conn
from schema_introspect import get_live_schema
from schema_reference import SCHEMA_DESCRIPTION

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# -----------------------------
# Utils
# -----------------------------
def safe_json(obj):
    if isinstance(obj, list): return [safe_json(x) for x in obj]
    if isinstance(obj, tuple): return tuple(safe_json(x) for x in obj)
    if isinstance(obj, dict): return {k: safe_json(v) for k, v in obj.items()}
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    if isinstance(obj, timedelta): return str(obj)
    return obj

def run_sql(sql: str, params: Optional[Tuple[Any,...]]=None, max_rows:int=400):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = 12000;")
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows)
            return cols, rows

# -----------------------------
# Embeddings
# -----------------------------
EMBED_MODEL = os.getenv("EMBED_MODEL","text-embedding-3-small")

def embed_query(text:str)->List[float]:
    out = client.embeddings.create(model=EMBED_MODEL,input=text)
    return out.data[0].embedding

def vector_literal(vec:List[float])->str:
    return "'[" + ",".join(f"{x:.6f}" for x in vec) + "]'::vector"

# -----------------------------
# SQL generation & validation
# -----------------------------
def build_sql_system_prompt()->str:
    live = get_live_schema()
    live_json = json.dumps(live, indent=2, sort_keys=True)
    rules = """
You are a legal contracts analytics assistant. You must output ONLY PostgreSQL SQL inside a single ```sql ... ``` code fence.
No prose, no markdown headings, no explanations outside the fence.

HARD RULES:
CONTRACTS / AGREEMENTS / WORKFLOWS:
- The terms "contract(s)", "agreement(s)", and "workflow(s)" all mean the same thing in this system.
- Always query from ic.workflows (joined with related tables if needed).
- Do not treat "contracts" as a separate table — they are all stored in ic.workflows.
- For counts like "how many contracts", "how many agreements", or "how many workflows", always count rows from ic.workflows.
- For listing, return workflow-level information (w.readable_id, w.title, w.status, department, etc.).


- SELECT-only. Never emit CREATE/INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/GRANT/REVOKE/MERGE/VACUUM/COPY/SET/SHOW. Users questions will never be about any of create/insert/update/delete/drop/alter/truncate/grant/revoke/merge/vacuum/copy/set/show, always interpret them as SQL queries.
- Treat natural words "create/review/sign/archive" as workflow steps.
- Status values: 'completed' (finished) and 'active' (in-progress).
- Use only existing columns. Do not invent names.

CLAUSES VS TEXT SEARCH:
- If the user explicitly says "clause"/"clauses", query ic.clauses (workflows joined via workflow_id). Count DISTINCT workflow_id for counts.
- If the user does NOT say "clause", treat it as a text search/mention task (outside SQL path).

VENDOR/COUNTERPARTY:
- Prefer ic.workflows.counterparty_name when filtering by vendor/counterparty.
- If NULL, fallback to COALESCE(legal_entity,'') ILIKE or title ILIKE.

- Imported contracts: always filter with "attributes ? 'importId'".
  Do NOT guess based on title or record_type.
  For time-based questions (e.g. "imports by month"), always use
    (attributes->'smartImportProperty_predictionDate'->>'value')::timestamptz
  instead of created_at or agreementDate/standard_executedDate.
  When filtering imported contracts by a given month, never use HAVING with the alias.
  Instead, repeat the DATE_TRUNC(...) expression directly inside the WHERE clause.


QUARTER WINDOWS:
- Always determine quarter ranges using CURRENT_DATE and calendar quarters.
- "Last quarter" = the previous full calendar quarter:
    execution_date >= date_trunc('quarter', CURRENT_DATE) - INTERVAL '3 months'
    AND execution_date <  date_trunc('quarter', CURRENT_DATE)
- "This quarter" = the current full calendar quarter:
    execution_date >= date_trunc('quarter', CURRENT_DATE)
    AND execution_date <  date_trunc('quarter', CURRENT_DATE) + INTERVAL '3 months'
- "Next quarter" = the next full calendar quarter:
    execution_date >= date_trunc('quarter', CURRENT_DATE) + INTERVAL '3 months'
    AND execution_date <  date_trunc('quarter', CURRENT_DATE) + INTERVAL '6 months'
- For explicit quarters like "Q1 2024" or "Q3 2023":
    Use EXTRACT(YEAR FROM execution_date)=YYYY
    AND EXTRACT(QUARTER FROM execution_date)=N
    (Q1=1, Q2=2, Q3=3, Q4=4).
- Never approximate with “last 3 months.” Always anchor to CURRENT_DATE and use calendar quarter boundaries.
- Never use INTERVAL '1 quarter' (invalid). Use INTERVAL '3 months' instead.
- The current year is 2025. 

APPROVALS:
- Use ic.approval_requests (alias a). Each row = one approval request/decision.
- Join with ic.role_assignees (ra) ON (workflow_id, role_id) to resolve user_name/email.
- Join with ic.workflows (w) for workflow status.
- Person matching MUST be broad and case-insensitive:
    • (LOWER(ra.user_name) ILIKE '%'||LOWER('<term>')||'%' OR LOWER(ra.email) ILIKE '%'||LOWER('<term>')||'%')

STATUS HANDLING:
- Approved approvals:
    • LOWER(a.status)='approved'
    • Always filter with a.end_time (the decision time).
- Pending approvals:
    • LOWER(a.status)='pending' AND a.end_time IS NULL
    • Always require w.status='active' (pending approvals only exist on in-progress workflows).
- Approver reassigned:
    • LOWER(a.status) LIKE 'approver reassigned%'.

TIME WINDOWS (always anchor to CURRENT_DATE):
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
    a.end_time >= CURRENT_DATE - INTERVAL '7 days'
    AND a.end_time < CURRENT_DATE
- If no timeframe is given → do not filter on dates.

WORKFLOW SCOPE:
- If user says “in progress” → add w.status='active'.
- If user says “completed” → add w.status='completed'.
- If the user says “pending approval” → require w.status='active'.
- If no state specified → include all.

OUTPUT SHAPE (ABSOLUTE RULES):
- Return **exactly one** SQL statement.
- If the user asks “how many / count”, return a single scalar COUNT in one SELECT.
- If the user asks “list / show / which”, return a single SELECT of rows (no COUNT, no extra statements).
  Prefer columns:
    w.workflow_id, w.readable_id, w.title
  Optionally include: a.role_name, a.start_time, a.end_time
  Order by a.end_time DESC (for approved) or a.start_time DESC (for pending), and LIMIT 100.

EXAMPLES:

-- Count: approved by Adam (all time)
SELECT COUNT(DISTINCT a.workflow_id) AS workflows_approved
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND (LOWER(ra.user_name) ILIKE '%adam%' OR LOWER(ra.email) ILIKE '%adam%');

-- List: approved by Adam in the last 3 months (rows, not count)
SELECT w.workflow_id, w.readable_id, w.title, a.role_name, a.end_time
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='approved'
  AND (LOWER(ra.user_name) ILIKE '%adam%' OR LOWER(ra.email) ILIKE '%adam%')
  AND a.end_time >= CURRENT_DATE - INTERVAL '3 months'
  AND a.end_time <  CURRENT_DATE
ORDER BY a.end_time DESC
LIMIT 100;

-- Count: pending approvals for Stephanie (active workflows only)
SELECT COUNT(DISTINCT a.workflow_id) AS pending_workflows
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='pending'
  AND a.end_time IS NULL
  AND w.status='active'
  AND (LOWER(ra.user_name) ILIKE '%stephanie%' OR LOWER(ra.email) ILIKE '%stephanie%');

-- List: pending approvals for Stephanie (rows, not count)
SELECT w.workflow_id, w.readable_id, w.title, a.role_name, a.start_time
FROM ic.approval_requests a
JOIN ic.role_assignees ra ON ra.workflow_id=a.workflow_id AND ra.role_id=a.role_id
JOIN ic.workflows w ON w.workflow_id=a.workflow_id
WHERE LOWER(a.status)='pending'
  AND a.end_time IS NULL
  AND w.status='active'
  AND (LOWER(ra.user_name) ILIKE '%stephanie%' OR LOWER(ra.email) ILIKE '%stephanie%')
ORDER BY a.start_time DESC
LIMIT 100;


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



CONSTANTS:
- Do NOT use parameter placeholders like %s. Inline constants as proper SQL string literals (escape ' by doubling).

OUTPUT FORMAT RULES:
- Always return your final SQL inside a single ```sql ... ``` fenced block.
- For normal analytical questions: return exactly ONE SELECT query.

- For weekly or multi-metric report style questions (like “generate the weekly report”, “weekly metrics”, “legal team report”):
    • Output a MULTI-STATEMENT SQL bundle containing several SELECT statements.
    • Each SELECT must be preceded by a comment line beginning with -- followed by its section title.
    • All statements and comments must be inside the same ```sql``` fenced block.
    • Separate statements with semicolons.
    • Never include text, markdown, or explanations outside the fence.
    • Use the canonical order and titles defined in the curated schema description (1–11).

Example:

```sql
-- Contracts Completed with Legal Review (Last 14 Days)
SELECT ...
;
-- New Contracts Assigned to Legal (Last 14 Days)
SELECT ...
;
-- ...

"""
    return f"""{rules}

=== Curated Schema Description ===
{SCHEMA_DESCRIPTION.strip()}

=== Live Schema ===
{live_json}
"""

SQL_FENCE_RE = re.compile(r"```sql\s*(.*?)```", re.IGNORECASE|re.DOTALL)
PROHIBITED = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|MERGE|VACUUM|COPY|\\copy|SET\s+|SHOW\s+)\b", re.IGNORECASE)

def extract_sql(text:str)->str:
    m = SQL_FENCE_RE.search(text or "")
    return m.group(1).strip().rstrip(";") if m else (text or "").strip().rstrip(";")

def validate_sql_safe(sql: str) -> None:
    """
    Validate that only safe read-only statements are produced.
    Allows multiple SELECT statements separated by semicolons.
    """
    body = sql.strip()
    # remove trailing semicolon
    if body.endswith(";"):
        body = body[:-1].strip()
    # still forbid any non-SELECT verbs
    if PROHIBITED.search(body):
        raise ValueError("Only SELECT statements are allowed.")
    # ensure first statement starts with SELECT
    if not re.search(r"\bselect\b", body, re.IGNORECASE):
        raise ValueError("Must contain at least one SELECT statement.")



def ask_for_sql(q:str)->str:
    sys = build_sql_system_prompt()
    resp = client.chat.completions.create(
        model="gpt-4o-mini",temperature=0,
        messages=[{"role":"system","content":sys},{"role":"user","content":q}]
    )
    return extract_sql(resp.choices[0].message.content or "")

# -----------------------------
# Summarizer
# -----------------------------
def build_summarizer_prompt() -> str:
    """
    Summarizer explicitly guided by the schema_reference weekly-report structure.
    Produces a fixed-order, human-readable weekly report using only retrieved numeric data.
    """
    return (
        "You are a precise but readable legal-analytics summarizer.\n"
        "You will receive a JSON payload containing one or more SQL sections with fields:\n"
        "  title, columns, rows_preview, metric, and sql.\n\n"
        "ABSOLUTE RULES:\n"
        "• NEVER invent, infer, or guess numbers or SQL.\n"
        "• Use ONLY data present in the payload.\n"
        "• Preserve the canonical weekly-report order (sections 1–11).\n\n"
        "FORMATTING:\n"
        "• Report header: 'Weekly Legal & Contract Report'.\n"
        "• For each section (1–11):\n"
        "     - Write the section title as a heading.\n"
        "     - For Sections 1–6: summarize in a short English sentence like\n"
        "           '7 contracts were completed with legal review in the last 14 days.'\n"
        "       Use plural/singular correctly and avoid showing raw field names.\n"
        "     - For Sections 7–10: present tabular or bullet outputs just as returned (reviewer or department lists).\n"
        "     - For Section 11: treat values as monetary; format with a leading $ and thousands commas (e.g. $1,234,567).\n"
        "     - If a section has no rows, state 'No data returned for this section.'\n"
        "• Keep grammar clear, consistent tense (past 14 days / past 12 months etc.).\n"
        "• Never change or infer numeric values; use only values in payload.metrics or row counts.\n"
        "• If a metric count or row count is available, use it exactly as provided.\n"
        "• Finish with one concise overall summary saying something like 'This is a summary of the weekly report'"
        "But do not make anything up. \n"
    )



def build_general_summarizer_prompt() -> str:
    """
    Summarizer for single-query SQL results.
    Balances accuracy and readability — grounded, but phrased naturally.
    """
    return (
        "You are a precise but fluent legal contracts analyst.\n"
        "You will receive a JSON payload with SQL query results (columns, rows, and possibly numeric fields).\n"
        "Your job is to write a short, natural-language summary that correctly reflects the data.\n\n"
        "GROUNDING RULES:\n"
        "• Every number, name, or title must come directly from the provided data — never invent values.\n"
        "• If the payload includes 'true_numeric_result', that is the correct count/total; use it verbatim.\n"
        "• If grouped results (like departments or reviewers) are shown, describe them factually and optionally sum their numeric values.\n"
        "• If multiple rows contain repeated IDs or titles, count each unique one only once.\n"
        "• If numeric fields represent monetary amounts (e.g., contract_value_amount, total_value), include them with proper currency formatting (e.g., $50,000,000 not 50000000.0).\n"
        "• If only one contract is shown, describe it specifically (include title, ID, and amount).\n"
        "• If multiple results appear, describe trends (e.g., 'X departments executed Y contracts...').\n"
        "• Do not restate table structure — describe meaning.\n"
        "• If there are zero rows, say 'No matching results were found.'\n"
        "• Keep your answer factual but readable — 1–3 sentences max.\n"
    )



def build_contract_summarizer_prompt() -> str:
    """
    Dedicated summarizer for 'Summarize IC-####' questions.
    Prevents the weekly report format from appearing for contract summaries.
    """
    return (
        "You are a legal contract summarizer.\n"
        "You will receive text chunks from a single contract in JSON format.\n"
        "Write a concise summary titled 'Summary for <readable_id>'.\n"
        "Include key sections such as parties, term, termination, obligations, "
        "confidentiality, payment, governing law, and other notable terms.\n"
        "Use bullet points when appropriate.\n"
        "Do NOT use weekly report or section numbering.\n"
        "Stick strictly to the provided text; never invent details."
    )


def stream_contract_summary_from_text(payload: Dict[str, Any]):
    """
    Streams a contract summary using the dedicated contract summarizer prompt.
    """
    stream = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": build_contract_summarizer_prompt()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        stream=True,
    )
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content



def stream_summary_from_payload(payload:Dict[str,Any]):
    stream=client.chat.completions.create(
        model="gpt-4o-mini",temperature=0,
        messages=[
            {"role":"system","content":build_summarizer_prompt()},
            {"role":"user","content":json.dumps(payload,ensure_ascii=False)},
        ],stream=True
    )
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content

# -----------------------------
# Follow-up Detection + Merge (NEW)
# -----------------------------
FOLLOWUP_DETECT_PROMPT = """
You are a classifier that determines if a user's new question is a follow-up to their last question in a conversation.
You must respond ONLY with JSON in the format: {"followup": true} or {"followup": false}

Rules:
- A follow-up = ONLY if the new question is incomplete or ambiguous without the previous one.
- If the new question introduces its own subject (contracts, workflows, clauses, department, timeframe, etc.), treat it as standalone (followup=false).
- Similar topics (same department, same timeframe) do NOT make it a follow-up if the question is otherwise complete.
- If uncertain, return {"followup": false}.

Examples of true follow-ups:
Last: "How many workflows are pending Stephanie’s approval?"
Now: "List them"
→ {"followup": true}

Last: "How many workflows are pending Stephanie’s approval?"
Now: "What about Pat?"
→ {"followup": true}

Last: "Show me the NDAs signed in Q2 2024"
Now: "What about MSAs?"
→ {"followup": true}

Last: "How many contracts were executed last quarter?"
Now: "List them"
→ {"followup": true}

Examples of NOT follow-ups (standalone questions):
Last: "How many workflows has IT created this month?"
Now: "How many contracts has IT created in the last 30 days?"
→ {"followup": false}

Last: "Summarize IC-1001"
Now: "Summarize IC-1002"
→ {"followup": false}

Last: "Which contracts mention indemnification?"
Now: "Which contracts mention liability?"
→ {"followup": false}
"""


FOLLOWUP_MERGE_PROMPT = """
You are a question rewriter. Given the previous user question (Last) and the current follow-up (Now),
rewrite them into ONE clear standalone question that does not rely on prior context.

CRITICAL:
- Preserve the TASK TYPE from the Now message.
  • If Now asks to "list/show/which/return rows", the merged question MUST ask to list rows (not count).
  • If Now asks "how many/count", the merged question MUST ask for a count (not a list).
  • If Now asks to "compare/summarize", preserve that action.

- Keep all important filters from Last and Now (names, dates, workflow states, contract types, vendors, etc.).
- Do NOT invent new information.
- Output ONLY the rewritten question as plain text (no explanations).
"""


def is_followup(last_q: str, current_q: str) -> bool:
    if not last_q or not current_q: 
        return False
    msgs = [
        {"role":"system","content":FOLLOWUP_DETECT_PROMPT},
        {"role":"user","content":f"Last: {last_q}\nNow: {current_q}"}
    ]
    resp = client.chat.completions.create(model="gpt-4o-mini", temperature=0, messages=msgs)
    content = (resp.choices[0].message.content or "{}").strip()
    try:
        js = json.loads(content)
        return bool(js.get("followup", False))
    except Exception:
        return False

def merge_followup(last_q: str, current_q: str) -> str:
    msgs = [
        {"role":"system","content":FOLLOWUP_MERGE_PROMPT},
        {"role":"user","content":f"Last: {last_q}\nNow: {current_q}"}
    ]
    resp = client.chat.completions.create(model="gpt-4o-mini", temperature=0, messages=msgs)
    merged = (resp.choices[0].message.content or current_q).strip()
    return merged if merged else current_q

# -----------------------------
# Intent classification
# -----------------------------
IC_ID_RE = re.compile(r"\bIC-\d+\b", re.IGNORECASE)

INTENT_SYSTEM_PROMPT = """
You are an intent classifier for a legal contracts bot.
Output STRICT JSON only with schema:
{
  "intent": "text_mention_count | text_snippets | summarize_contract | compare_contracts | semantic_find | similar_to_contract | sql_generic",
  "terms": [string],
  "logic": {"operator":"AND|OR","exclude":[string]},
  "near": {"enabled":true|false,"window":120},
  "readable_ids":[string],
  "query_text":string|null,
  "vendor_term":string|null,
  "notes":string|null
}

Routing guidance:
- summarize IC-#### → summarize_contract
- compare IC-#### vs IC-#### → compare_contracts
- "how many mention …" / "show snippets …" (when user does NOT say 'clause') → text_mention_count or text_snippets
- "clause"/"clauses" present → sql_generic (query ic.clauses)
- "similar to IC-####" → similar_to_contract
- "find/which contracts about …" → semantic_find
- else → sql_generic

Examples:
User: summarize IC-1001
{"intent":"summarize_contract","readable_ids":["IC-1001"],"terms":[],"logic":{"operator":"AND","exclude":[]}}

User: compare IC-1001 vs IC-1002
{"intent":"compare_contracts","readable_ids":["IC-1001","IC-1002"],"terms":[],"logic":{"operator":"AND","exclude":[]}}

User: how many workflows have termination clauses?
{"intent":"sql_generic","terms":["termination"],"logic":{"operator":"AND","exclude":[]}}

User: how many contracts do we have with lonza
{"intent":"sql_generic","vendor_term":"lonza","terms":[],"logic":{"operator":"AND","exclude":[]}}

User: show snippets where indemnification is near cap
{"intent":"text_snippets","terms":["indemnification","cap"],"near":{"enabled":true,"window":120}}

User: which contracts talk about "GxP"
{"intent":"semantic_find","query_text":"GxP"}

User: how many NDAs were executed in 2024
{"intent":"sql_generic"}
"""

def _extract_first_json(txt:str)->str:
    t=(txt or "").strip()
    if t.startswith("{") and t.endswith("}"): return t
    depth=0; start=None
    for i,ch in enumerate(t):
        if ch=="{":
            if depth==0: start=i
            depth+=1
        elif ch=="}":
            depth-=1
            if depth==0 and start is not None:
                return t[start:i+1]
    return "{}"

def classify_intent(q:str)->Dict[str,Any]:
    ids=[m.group(0).upper() for m in IC_ID_RE.finditer(q)]
    quoted=re.findall(r"['\"]([^'\"]+)['\"]",q)
    hints={"readable_ids_detected":ids,"quoted_terms_detected":quoted}
    msgs=[
        {"role":"system","content":INTENT_SYSTEM_PROMPT},
        {"role":"system","content":"HINTS: "+json.dumps(hints)},
        {"role":"user","content":q},
    ]
    resp=client.chat.completions.create(model="gpt-4o-mini",temperature=0,messages=msgs)
    content=resp.choices[0].message.content or "{}"
    try: js=json.loads(_extract_first_json(content))
    except: js={}
    js.setdefault("intent","sql_generic")
    js.setdefault("terms",[])
    js.setdefault("logic",{"operator":"AND","exclude":[]})
    js.setdefault("near",{"enabled":False,"window":120})
    js.setdefault("readable_ids",[])
    js.setdefault("query_text",None)
    js.setdefault("vendor_term",None)
    js.setdefault("notes",None)
    # Belt-and-suspenders: if the user says clause/clauses, force SQL path
    if "clause" in q.lower():
        js["intent"] = "sql_generic"
    return js

# -----------------------------
# Answer assembly
# -----------------------------
def _answer_with_rows(question, sql, cols, rows, intent):
    """
    Route output formatting correctly.
    - Weekly report → use weekly-report summarizer (multi-section layout)
    - All other single-query results → use general summarizer (short direct answer)
    """
    # Detect true numeric value if this is an aggregate COUNT(*) or SUM() result
    numeric_value = None
    if len(rows) == 1 and len(cols) == 1:
        val = rows[0][0]
        if isinstance(val, (int, float, Decimal)):
            numeric_value = float(val)

    payload = {
        "question": question,
        "sql": sql,
        "columns": cols,
        "rows_preview": safe_json(rows[:50]),
        "row_count_returned": len(rows),
        "total_rows": len(rows),
        "true_numeric_result": numeric_value,  # NEW field: the real count if available
        "intent": intent,
    }


    # ✅ Detect if this is a weekly report SQL bundle (multi-section keywords)
    weekly_keywords = [
        "Contracts Completed with Legal Review",
        "Weekly Legal Team",
        "Work Completed by Department",
    ]
    is_weekly_report = any(k.lower() in sql.lower() for k in weekly_keywords)

    if is_weekly_report:
        # Weekly-report summarizer (multi-section layout)
        stream = stream_summary_from_payload(payload)
    else:
        # General summarizer (normal single-query output)
        stream_resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": build_general_summarizer_prompt()},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            stream=True,
        )
        stream = (
            chunk.choices[0].delta.content
            for chunk in stream_resp
            if chunk.choices and chunk.choices[0].delta.content
        )

    return {
        "sql": sql,
        "columns": cols,
        "rows": rows,
        "stream": stream,
        "intent_json": intent,
    }


def _answer_with_text(question, meta, texts, intent):
    """
    Handles contract summaries (Summarize IC-####) so they don't use the weekly report layout.
    """
    payload = {"question": question, "meta": meta, "text_blobs": texts, "intent": intent}
    return {
        "sql": meta.get("sql", ""),
        "columns": [],
        "rows": [],
        "stream": stream_contract_summary_from_text(payload),
        "intent_json": intent,
    }


# -----------------------------
# Deterministic helpers
# -----------------------------
def _ilike_clause_frag(alias,terms,op):
    if not terms: return "TRUE",[]
    frags=[];params=[]
    for t in terms:
        frags.append(f"{alias}.chunk_text ILIKE %s")
        params.append(f"%{t}%")
    # join WITH spaces around the operator (fixes the 'ANDc' bug)
    return ("(" + f" {op} ".join(frags) + ")"), params

def _not_frag(alias,terms):
    if not terms: return "",[]
    frags=[];params=[]
    for t in terms:
        frags.append(f"NOT ({alias}.chunk_text ILIKE %s)")
        params.append(f"%{t}%")
    return " AND " + " AND ".join(frags), params

# -----------------------------
# Main router
# -----------------------------


def _count_unquoted_percent_s(sql: str) -> int:
    """
    Counts %s placeholders that are OUTSIDE single-quoted string literals.
    Treats doubled quotes ('') as an escaped single quote.
    """
    count = 0
    in_single = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            if in_single:
                # handle doubled single quote '' inside string literal
                if i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                in_single = False
                i += 1
                continue
            else:
                in_single = True
                i += 1
                continue
        # Only count %s when NOT inside a string literal
        if not in_single and ch == "%" and i + 1 < n and sql[i + 1] == "s":
            count += 1
            i += 2
            continue
        i += 1
    return count

def _extract_sections(sql_block: str):
    """
    Split a multi-statement SQL block into sections with optional titles.
    A title is taken from the nearest preceding '-- ...' comment directly
    above the SELECT. Returns list of dicts: {"title": str|None, "sql": str}
    """
    parts = [p for p in sql_block.split(";") if p.strip()]
    sections = []
    src = sql_block.splitlines()
    # Build quick lookup from statement text to title by scanning lines
    i = 0
    buf = []
    current_title = None
    for line in src:
        if line.strip().startswith("--"):
            current_title = line.strip().lstrip("-").strip()
        buf.append(line)
        if ";" in line:
            stmt = "\n".join(buf).strip()
            # pull only the last SELECT ... before the semicolon
            # but keep leading comments
            sections.append({"title": current_title, "sql": stmt.rstrip(";").strip()})
            buf = []
            current_title = None
    # fallback when the last statement has no trailing semicolon
    leftover = "\n".join(buf).strip()
    if leftover:
        sections.append({"title": current_title, "sql": leftover})
    # final cleanup: only keep statements that contain 'select'
    out = []
    for s in sections:
        if re.search(r"\bselect\b", s["sql"], re.IGNORECASE):
            out.append({"title": s["title"], "sql": s["sql"]})
    return out


def _derive_metric(cols, rows):
    """
    If the result looks like a single-row aggregate, return (name, value).
    Otherwise return (None, None).
    """
    if not cols or not rows:
        return (None, None)
    if len(rows) == 1:
        row = rows[0]
        # prefer first numeric-looking column
        for ci, cv in enumerate(row):
            if isinstance(cv, (int, float, Decimal)) or (
                isinstance(cv, str) and re.fullmatch(r"-?\d+(\.\d+)?", cv or "")
            ):
                return (cols[ci], cv)
    return (None, None)


def answer_question(question: str, last_question: Optional[str] = None) -> Dict[str, Any]:
    # NEW: follow-up preprocessing (safe no-op if last_question is None)
    if last_question:
        try:
            if is_followup(last_question, question):
                question = merge_followup(last_question, question)
        except Exception:
            # Fail-safe: if detection/merge errors, continue with original question
            pass

    intent = classify_intent(question)

    # Summarize (RAG)
    if intent["intent"] == "summarize_contract" and intent.get("readable_ids"):
        rid = intent["readable_ids"][0]
        cols, rows = run_sql(
            "SELECT chunk_id,chunk_text FROM ic.contract_chunks WHERE readable_id=%s ORDER BY chunk_id",
            (rid,),
            max_rows=5000,
        )
        texts = [r[1] for r in rows]
        acc = 0
        out = []
        for t in texts:
            if acc + len(t) > 180_000:
                break
            out.append(t)
            acc += len(t)
        return _answer_with_text(
            question, {"retrieval": "ordered_chunks", "readable_id": rid}, out, intent
        )

    # Compare
    if intent["intent"] == "compare_contracts" and len(intent.get("readable_ids", [])) >= 2:
        a, b = intent["readable_ids"][:2]

        def grab(rid):
            c, r = run_sql(
                "SELECT chunk_id,chunk_text FROM ic.contract_chunks WHERE readable_id=%s ORDER BY chunk_id",
                (rid,),
                max_rows=5000,
            )
            texts = [x[1] for x in r]
            acc = 0
            out = []
            for t in texts:
                if acc + len(t) > 120_000:
                    break
                out.append(t)
                acc += len(t)
            return out

        return _answer_with_text(
            question,
            {"retrieval": "compare", "ids": [a, b]},
            ["\n".join(grab(a)), "\n".join(grab(b))],
            intent,
        )

    # Text mention count
    if intent["intent"] == "text_mention_count":
        op = intent.get("logic", {}).get("operator", "AND").upper()
        if op not in ("AND", "OR"):
            op = "AND"
        inc = intent.get("terms", [])
        exc = intent.get("logic", {}).get("exclude", [])
        inc_where, inc_params = _ilike_clause_frag("c", inc, op)
        not_where, not_params = _not_frag("c", exc)
        sql = f"""WITH matches AS (
  SELECT DISTINCT c.readable_id FROM ic.contract_chunks c
  WHERE {inc_where}{not_where}
) SELECT COUNT(*) AS contracts_with_term,
         ARRAY(SELECT readable_id FROM matches ORDER BY readable_id LIMIT 5) AS example_ids
FROM matches"""
        cols, rows = run_sql(sql, tuple(inc_params + not_params))
        return _answer_with_rows(question, sql, cols, rows, intent)

    # Snippets
    if intent["intent"] == "text_snippets":
        terms = intent.get("terms", [])[:2]
        near = intent.get("near", {})
        limit = 10
        if len(terms) >= 2 and near.get("enabled", False):
            t1, t2 = terms[0], terms[1]
            win = int(near.get("window", 120))
            pattern = f"(?is)({re.escape(t1)}.{{0,{win}}}{re.escape(t2)}|{re.escape(t2)}.{{0,{win}}}{re.escape(t1)})"
            sql = """SELECT readable_id,chunk_id,LEFT(chunk_text,300) AS snippet
FROM ic.contract_chunks WHERE chunk_text ~ %s LIMIT %s"""
            cols, rows = run_sql(sql, (pattern, limit))
            return _answer_with_rows(question, sql, cols, rows, intent)
        else:
            term = terms[0] if terms else "termination"
            sql = """SELECT readable_id,chunk_id,LEFT(chunk_text,300) AS snippet
FROM ic.contract_chunks WHERE chunk_text ILIKE '%'||%s||'%' ORDER BY readable_id,chunk_id LIMIT %s"""
            cols, rows = run_sql(sql, (term, limit))
            return _answer_with_rows(question, sql, cols, rows, intent)

    # ✅ Generic → GPT builds SQL (covers vendor/counterparty, clauses, and all other metadata analytics)
    try:
        sql = ask_for_sql(question)
        validate_sql_safe(sql)

        # Safety net: if the model still used %s placeholders, auto-bind a single repeated parameter (e.g., vendor_term).
        params: Optional[Tuple[Any, ...]] = None
        unquoted_count = _count_unquoted_percent_s(sql)
        if unquoted_count > 0:
            if intent.get("vendor_term"):
                params = tuple([intent["vendor_term"]] * unquoted_count)
            else:
                raise ValueError(
                    "Generated SQL contains unbound %s placeholders outside of string literals, "
                    "but no parameters were provided to bind."
                )

        # ✅ Handle multi-statement SQL (e.g., weekly report bundles)
        sections = _extract_sections(sql)

        # --- Weekly report path (multi-section bundle) ---
        if len(sections) > 1:
            structured = []
            for sec in sections:
                try:
                    cols, rows = run_sql(sec["sql"])
                    metric_name, metric_value = _derive_metric(cols, rows)

                    # Optional formatting for money-like metrics
                    if metric_name and "value" in metric_name.lower() and isinstance(
                        metric_value, (int, float, Decimal)
                    ):
                        metric_value = f"${metric_value:,.0f}"

                    structured.append(
                        {
                            "title": sec["title"],
                            "sql": sec["sql"],
                            "columns": cols,
                            "rows_preview": safe_json(rows[:50]),
                            "row_count_returned": len(rows),
                            "metric": {
                                "name": metric_name,
                                "value": safe_json(metric_value),
                            }
                            if metric_name
                            else None,
                        }
                    )
                except Exception as inner_err:
                    structured.append(
                        {"title": sec["title"], "sql": sec["sql"], "error": str(inner_err)}
                    )

            # Enforce canonical section order for summarizer consistency
            canonical_order = [
                "Contracts Completed with Legal Review (Last 14 Days)",
                "New Contracts Assigned to Legal (Last 14 Days)",
                "Total Contracts Going Through Ironclad (Last 14 Days)",
                "Active Contracts Created Over 90 Days Ago",
                "Contracts with No Activity Over 90 Days",
                "Active NDAs Created in Last 14 Days",
                "Weekly Legal Team – Contracts Completed by Reviewer (Last 14 Days)",
                "Weekly Legal Team – New Contracts Assigned by Reviewer (Last 14 Days)",
                "Work in Progress by Department",
                "Work Completed by Department (Past 12 Months)",
                "Work Completed by Sum of Contract Value (Past 12 Months)",
            ]
            structured.sort(
                key=lambda x: canonical_order.index(x["title"])
                if x["title"] in canonical_order
                else 999
            )

            payload = {
                "question": question,
                "report_type": "weekly",
                "sections": structured,
                "intent": intent,
                "sql": sql,
            }

            # ✅ Weekly report summarizer
            return {
                "sql": sql,
                "columns": [],
                "rows": [],
                "stream": stream_summary_from_payload(payload),
                "intent_json": intent,
            }

        # --- Normal single-query path ---
        else:
            single_sql = sections[0]["sql"] if sections else sql
            cols, rows = run_sql(single_sql, params)
            payload = {
                "question": question,
                "sql": single_sql,
                "columns": cols,
                "rows_preview": safe_json(rows[:50]),
                "intent": intent,
            }

            # ✅ Use the general summarizer for single SQL queries
            stream = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0,
                messages=[
                    {"role": "system", "content": build_general_summarizer_prompt()},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                stream=True,
            )

            return {
                "sql": single_sql,
                "columns": cols,
                "rows": rows,
                "stream": (
                    chunk.choices[0].delta.content
                    for chunk in stream
                    if chunk.choices and chunk.choices[0].delta.content
                ),
                "intent_json": intent,
            }

    except Exception as e:
        # Summarize error message for human-readable output
        payload = {
            "question": question,
            "sql": sql if "sql" in locals() else "",
            "error": str(e),
            "intent": intent,
        }
        return {
            "sql": sql if "sql" in locals() else "",
            "columns": [],
            "rows": [],
            # ⬇️ switched to general summarizer (fix)
            "stream": stream_general_from_payload(payload),
            "intent_json": intent,
        }

    return {
        "sql": "",
        "columns": [],
        "rows": [],
        # ⬇️ switched to general summarizer (fix)
        "stream": stream_general_from_payload(
            {
                "question": question,
                "error": "Unhandled query path reached with no output",
                "intent": intent,
            }
        ),
        "intent_json": intent,
    }
