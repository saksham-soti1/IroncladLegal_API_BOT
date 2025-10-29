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

# =========================================================
# Utils
# =========================================================
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

# =========================================================
# Embeddings
# =========================================================
EMBED_MODEL = os.getenv("EMBED_MODEL","text-embedding-3-small")

def embed_query(text:str)->List[float]:
    out = client.embeddings.create(model=EMBED_MODEL,input=text)
    return out.data[0].embedding

def vector_literal(vec:List[float])->str:
    return "'[" + ",".join(f"{x:.6f}" for x in vec) + "]'::vector"

# =========================================================
# SQL generation & validation
# =========================================================
def build_sql_system_prompt(weekly_allowed: bool) -> str:
    live = get_live_schema()
    live_json = json.dumps(live, indent=2, sort_keys=True)

    weekly_switch = (
        "WEEKLY_ALLOWED=TRUE\n"
        "You MAY output a MULTI-STATEMENT SQL bundle (sections 1–11) only when the user asked for a weekly report.\n"
    ) if weekly_allowed else (
        "WEEKLY_ALLOWED=FALSE\n"
        "STRICT RULE: Output exactly ONE SELECT statement. Do NOT emit multi-statement bundles or weekly section headers.\n"
    )

    rules = f"""
You are a legal contracts analytics assistant. You must output ONLY PostgreSQL SQL inside a single ```sql ... ``` code fence.
No prose, no markdown headings, no explanations outside the fence.

{weekly_switch}

HARD RULES (controller-level):
- SELECT-only. Never emit CREATE/INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/GRANT/REVOKE/MERGE/VACUUM/COPY/SET/SHOW.
- The terms "contract(s)", "agreement(s)", and "workflow(s)" all refer to records in ic.workflows.
- For counts like "how many contracts/agreements/workflows", count rows from ic.workflows.
- For listing, return workflow-level fields (w.readable_id, w.title, etc.) and LIMIT 100 unless user asks otherwise.
- Do not invent column names. Use only columns that exist in the live schema below.

OUTPUT SHAPE:
- When WEEKLY_ALLOWED=FALSE:
  • Return exactly ONE SELECT statement (no CTE bundle of sections).
- If the user asks “how many / count”, return a single scalar COUNT in one SELECT.
- If the user asks “list / show / which”, return a single SELECT of rows (no extra counts).
- Wrap the final SQL in a single ```sql``` fenced block.

# Weekly report bundle (ONLY when WEEKLY_ALLOWED=TRUE):
# • Output a MULTI-STATEMENT SQL bundle (sections 1–11).
# • Each SELECT must be preceded by a comment line beginning with -- followed by its section title.
# • All statements and comments must be inside the same ```sql``` fenced block.
# • Separate statements with semicolons.
# • Use the canonical order and titles defined in the curated schema description (1–11).

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
    # ensure at least one SELECT
    if not re.search(r"\bselect\b", body, re.IGNORECASE):
        raise ValueError("Must contain at least one SELECT statement.")

def ask_for_sql(q: str, weekly_allowed: bool) -> str:
    sys = build_sql_system_prompt(weekly_allowed)
    resp = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0,
        messages=[{"role": "system", "content": sys}, {"role": "user", "content": q}]
    )
    return extract_sql(resp.choices[0].message.content or "")

# =========================================================
# Summarizers (Exec Brief default) + Contract summary
# =========================================================
def build_summarizer_prompt() -> str:
    """
    Weekly-report summarizer (kept as-is for your 1–11 bundle).
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
        "     - For Sections 7–10: present tabular or bullet outputs just as returned.\n"
        "     - For Section 11: treat values as monetary; format like $1,234,567.\n"
        "     - If a section has no rows, state 'No data returned for this section.'\n"
        "• Keep grammar clear; never change numeric values.\n"
        "• Finish with one concise overall summary; do not invent facts.\n"
    )

def build_general_summarizer_prompt() -> str:
    """
    Exec Brief default for single-query answers.
    Will optionally lead with prior 'primary_response' context when provided.
    """
    return (
        "You are a precise, executive-brief legal contracts analyst.\n"
        "You will receive a JSON payload with SQL results (columns, rows), the resolved question, "
        "and an optional prior primary_response.\n"
        "Write a crisp 1–2 sentence answer (no bullets) that is fully grounded in the payload.\n\n"
        "RULES:\n"
        "• Use ONLY data from the payload; do not invent values.\n"
        "• If 'true_numeric_result' exists, that is authoritative.\n"
        "• If 'primary_response' is present AND it contains a numeric value, AND this turn is a follow-up, then:\n"
        "      - Lead with a clause like 'Of the {primary_response.value} {primary_response.context}, ...'\n"
        "      - If the value is not numeric, do NOT use it to summarize the follow-up.\n"
        "• If grouping columns exist (e.g., departments, reviewers), mention the top 2–3 groups succinctly then end with 'others follow' or similar.\n"
        "• Format money with $ and commas when present.\n"
        "• If zero rows, say 'No matching results were found.'\n"
        "• Keep to 1–2 sentences; no SQL, no tables.\n"
    )

def build_contract_summarizer_prompt() -> str:
    return (
        "You are a legal contract summarizer.\n"
        "You will receive text chunks from a single contract in JSON format.\n"
        "Write a concise summary titled 'Summary for <readable_id>'.\n"
        "Include key sections such as parties, term, termination, obligations, "
        "confidentiality, payment, governing law, and notable terms.\n"
        "Use bullet points when appropriate; never invent details.\n"
        "Do NOT use weekly report formatting.\n"
    )

def stream_contract_summary_from_text(payload: Dict[str, Any]):
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

def stream_general_from_payload(payload: Dict[str, Any]):
    stream = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0,
        messages=[
            {"role": "system", "content": build_general_summarizer_prompt()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        stream=True,
    )
    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content

# =========================================================
# Multi-turn Memory: History Selector + Follow-up Rewriter
# =========================================================
HISTORY_SELECTOR_PROMPT = """
You compress conversation context for a contracts analytics bot.

INPUTS:
- prior_summary: 1–2 sentence summary of the conversation so far (may be null)
- prior_scope: JSON of active filters (timeframe/status/vendor/department/ids/etc.)
- prior_resolved_question: last fully self-contained question (may be null)
- prior_primary_response: last main answer object (may be null), e.g.
  {"type":"numeric","value":77,"context":"contracts executed this quarter"}

TASK:
- Produce a minimal set of 2–5 bullets 'relevant_history' that should guide interpreting the next user message.
- Update the 1–2 sentence 'updated_summary' to reflect the latest state.
- Never invent values. If something is unknown, omit it.

OUTPUT STRICT JSON:
{
  "relevant_history": [ "executed this quarter", "status=completed", "vendor=Lonza" ],
  "updated_summary": "Asked executed this quarter (77). Then breakdown by department."
}
"""

REWRITER_PROMPT = """
You rewrite user messages into self-contained questions and decide if they are follow-ups.

INPUTS:
- user_text: the new raw user message
- relevant_history: 2–5 short bullets describing the active topic/scope
- scope: JSON dict of active filters (timeframe/status/vendor/department/ids/etc.)
- prior_resolved_question: last fully self-contained question (may be null)

RULES:
- Determine if this is a FOLLOW-UP to the active topic.
  - Only mark followup=true if the user is clearly continuing or refining a prior query.
  - Examples: adding filters (vendor, type, date), asking for breakdowns, exclusions, grouping, totals, or follow-up questions like “how many of those…”, “break that down…”, “what about…”

- If the new message asks about a different entity or task (e.g., new timeframe, new status, new subject like "in-progress workflows"), then it's a NEW TOPIC → followup=false.

- NEVER assume a question is a follow-up just because there is prior context. The message must clearly depend on it.

- If follow-up:
    - Fill in any missing filters (status, timeframe, vendor, etc.) from the scope.
- If new topic:
    - Set is_followup=false.
    - Suggest which scope keys to reset in "reset_keys" (e.g., timeframe, vendor, status, department, ids).
    - Do NOT carry over old filters.

OUTPUT STRICT JSON ONLY:
{
  "is_followup": true|false,
  "resolved_question": "fully self-contained question string",
  "scope_updates": { "timeframe": "...", "status": "...", "vendor": "...", "ids": ["..."] },
  "reset_keys": ["timeframe","vendor"],
  "topic_label": "short_topic_name",
  "notes": "brief rationale"
}
"""


def history_selector(prior_summary: Optional[str],
                     prior_scope: Optional[Dict[str,Any]],
                     prior_resolved_question: Optional[str],
                     prior_primary_response: Optional[Dict[str,Any]]) -> Dict[str,Any]:
    payload = {
        "prior_summary": prior_summary,
        "prior_scope": prior_scope or {},
        "prior_resolved_question": prior_resolved_question,
        "prior_primary_response": prior_primary_response
    }
    resp = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0,
        messages=[
            {"role":"system","content":HISTORY_SELECTOR_PROMPT},
            {"role":"user","content":json.dumps(payload, ensure_ascii=False)}
        ]
    )
    txt = resp.choices[0].message.content or "{}"
    try:
        js = json.loads(txt)
        if not isinstance(js.get("relevant_history", []), list):
            js["relevant_history"] = []
        js["updated_summary"] = js.get("updated_summary") or prior_summary
        return js
    except Exception:
        return {"relevant_history": [], "updated_summary": prior_summary}

def followup_rewriter(user_text: str,
                      relevant_history: List[str],
                      scope: Dict[str,Any],
                      prior_resolved_question: Optional[str]) -> Dict[str,Any]:
    payload = {
        "user_text": user_text,
        "relevant_history": relevant_history,
        "scope": scope or {},
        "prior_resolved_question": prior_resolved_question
    }
    resp = client.chat.completions.create(
        model="gpt-4o-mini", temperature=0,
        messages=[
            {"role":"system","content":REWRITER_PROMPT},
            {"role":"user","content":json.dumps(payload, ensure_ascii=False)}
        ]
    )
    txt = resp.choices[0].message.content or "{}"
    try:
        js = json.loads(txt)
    except Exception:
        js = {
            "is_followup": False,
            "resolved_question": user_text,
            "scope_updates": {},
            "reset_keys": [],
            "topic_label": None,
            "notes": "fallback"
        }
    # Ensure minimal fields
    js.setdefault("is_followup", False)
    js.setdefault("resolved_question", user_text)
    js.setdefault("scope_updates", {})
    js.setdefault("reset_keys", [])
    js.setdefault("topic_label", None)
    js.setdefault("notes", None)
    return js

# =========================================================
# Legacy follow-up detector (kept for safety; used only when needed)
# =========================================================
FOLLOWUP_DETECT_PROMPT = """
You are a classifier that determines if a user's new question is a follow-up to their last question in a conversation.
You must respond ONLY with JSON in the format: {"followup": true} or {"followup": false}

Rules:
- A follow-up = ONLY if the new question is incomplete or ambiguous without the previous one.
- If uncertain, return {"followup": false}.
"""
FOLLOWUP_MERGE_PROMPT = """
You are a question rewriter. Given the previous user question (Last) and the current follow-up (Now),
rewrite them into ONE clear standalone question that does not rely on prior context.

CRITICAL:
- Preserve the TASK TYPE from the Now message.
- Keep all important filters from Last and Now.
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

# =========================================================
# Intent classification
# =========================================================
IC_ID_RE = re.compile(r"\bIC-\d+\b", re.IGNORECASE)

INTENT_SYSTEM_PROMPT = """
You are an intent classifier for a legal contracts assistant. Your job is to classify the user's request into one of the supported query types below.

Always return STRICT JSON in this format:
{
  "intent": "text_mention_count | text_snippets | summarize_contract | compare_contracts | semantic_find | similar_to_contract | weekly_report | sql_generic",
  "terms": [string],                     # any keywords to match on (if applicable)
  "logic": {"operator":"AND|OR","exclude":[string]},
  "near": {"enabled":true|false,"window":120},
  "readable_ids":[string],              # like IC-1234
  "query_text":string|null,
  "vendor_term":string|null,
  "notes":string|null
}

---

## INTENT DEFINITIONS AND EXAMPLES:

### 🔹 text_mention_count  
Count how many contracts contain specific **words or phrases in the text**.

**Trigger phrases:**  
- “how many contracts mention…”  
- “how many contain/include/mention the phrase…”  
- “how many say ‘governing law’…”  
- user quotes a phrase (e.g. “force majeure”) and wants count

Use only when the question is explicitly about **textual content** (not metadata or status).

---

### 🔹 text_snippets  
Return snippets of contract text that include or surround keywords or concepts.

**Trigger phrases:**  
- “show snippets where…”  
- “give examples of clauses that mention…”  
- “snippets with indemnification near liability”  
- “show me the text around ‘termination’”  
- any mention of “near”, “snippet”, or "context around"

Also supports proximity logic (e.g. “X near Y”).

---

### 🔹 sql_generic  
Questions about contract **metadata, filters, counts, or time windows** — not specific wording.

**Trigger phrases / patterns:**  
- “how many contracts were executed/signed/completed…”  
- “how many NDAs were created last quarter”  
- “how many contracts were finished this month”  
- “how many workflows are pending approval”  
- “list of contracts signed by Johnson & Johnson”  
- anything about vendor, status, execution date, contract value, type, department, etc.

Also use when:
- user asks about **clause types** generically (e.g., “how many contracts have an indemnity clause”)
- question includes **date/time filters** but is not about specific phrases

⛔️ Do **not** use `text_mention_count` for time-based execution questions — those are `sql_generic`.

---

### 🔹 summarize_contract  
User asks for a summary of a specific contract.

**Trigger phrases:**  
- “summarize IC-1234”  
- “what’s the overview of IC-9876”

Requires a single `readable_id`.

---

### 🔹 compare_contracts  
User wants a comparison of two contracts.

**Trigger phrases:**  
- “compare IC-1234 and IC-5678”  
- “how does IC-4444 differ from IC-1111”

Requires two `readable_ids`.

---

### 🔹 similar_to_contract  
User wants to find contracts that resemble a given one.

**Trigger phrases:**  
- “find contracts similar to IC-7890”  
- “which contracts are like IC-2345”

---

### 🔹 semantic_find  
Conceptual or topical search without specific phrases.

**Trigger phrases:**  
- “find contracts about IP ownership”  
- “which contracts are related to data privacy”  
- “any agreements that deal with subcontractors”  
- general topic-based exploration

Use when the user is describing a **concept** they want to find, not a specific clause or term.

---

### 🔹 weekly_report  
User requests a summary of recent activity (e.g., new workflows, approvals, completions this week).

**Trigger phrases:**  
- “what happened this week”  
- “show me a summary of recent contracts”  
- “weekly activity summary”

---

## GENERAL RULES:
- If the user mentions **“clause” or “clauses”**, and isn't asking for text/snippets → `sql_generic`
- If the user asks **how many contracts contain specific phrases**, or quotes wording → `text_mention_count`
- If the user wants **text excerpts or examples** → `text_snippets`
- If the user question involves **time + status** (executed/signed/created in last X), it is ALWAYS `sql_generic`
- If unsure between `text_mention_count` and `sql_generic`, ask:  
   ❓ “is the user talking about the actual wording inside contracts?” → text  
   ❓ “or just filtering based on metadata/date/status?” → sql_generic

NEVER include SQL or implementation logic. Just classify intent based on the user's language.
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
    if "clause" in q.lower():
        js["intent"] = "sql_generic"
    return js

# =========================================================
# Deterministic helpers for text paths
# =========================================================
def _ilike_clause_frag(alias,terms,op):
    if not terms: return "TRUE",[]
    frags=[];params=[]
    for t in terms:
        frags.append(f"{alias}.chunk_text ILIKE %s")
        params.append(f"%{t}%")
    return ("(" + f" {op} ".join(frags) + ")"), params

def _not_frag(alias,terms):
    if not terms: return "",[]
    frags=[];params=[]
    for t in terms:
        frags.append(f"NOT ({alias}.chunk_text ILIKE %s)")
        params.append(f"%{t}%")
    return " AND " + " AND ".join(frags), params

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
        if not in_single and ch == "%" and i + 1 < n and sql[i + 1] == "s":
            count += 1
            i += 2
            continue
        i += 1
    return count

def _extract_sections(sql_block: str):
    """
    Split a multi-statement SQL block into sections with optional titles.
    Title taken from nearest preceding '-- ...' comment.
    """
    parts = [p for p in sql_block.split(";") if p.strip()]
    sections = []
    src = sql_block.splitlines()
    buf = []
    current_title = None
    for line in src:
        if line.strip().startswith("--"):
            current_title = line.strip().lstrip("-").strip()
        buf.append(line)
        if ";" in line:
            stmt = "\n".join(buf).strip()
            sections.append({"title": current_title, "sql": stmt.rstrip(";").strip()})
            buf = []
            current_title = None
    leftover = "\n".join(buf).strip()
    if leftover:
        sections.append({"title": current_title, "sql": leftover})
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
        for ci, cv in enumerate(row):
            if isinstance(cv, (int, float, Decimal)) or (
                isinstance(cv, str) and re.fullmatch(r"-?\d+(\.\d+)?", cv or "")
            ):
                return (cols[ci], cv)
    return (None, None)

# =========================================================
# Main answer function (now stateful)
# =========================================================
def answer_question(
    question: str,
    last_question: Optional[str] = None,
    conversation_summary: Optional[str] = None,
    scope: Optional[Dict[str, Any]] = None,
    resolved_question: Optional[str] = None,
    primary_response: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Core controller:
      1) History selector (compress prior context)
      2) Follow-up detector + rewriter → resolved_question + scope updates
      3) Intent + routing on resolved_question
      4) Execute (SQL/text/semantic/weekly)
      5) Exec-brief summarizer (uses prior primary_response only if follow-up)
      6) Persist/return updated state: conversation_summary, scope, resolved_question, primary_response
    """
    scope = dict(scope or {})
    # -- (1) History selector: compress prior context into focused bullets + updated summary
    hs = history_selector(conversation_summary, scope, resolved_question, primary_response)
    relevant_history = hs.get("relevant_history", [])
    updated_summary = hs.get("updated_summary", conversation_summary)

    # -- (2) Follow-up detector + rewriter (preferred path)
    rew = followup_rewriter(
        user_text=question,
        relevant_history=relevant_history,
        scope=scope,
        prior_resolved_question=resolved_question
    )
    is_followup_turn = bool(rew.get("is_followup"))
    resolved_q = rew.get("resolved_question") or question

    # Merge scope updates & apply resets when the rewriter signals a new topic
    resets = rew.get("reset_keys") or []
    if resets:
        for k in resets:
            if k in scope:
                scope.pop(k, None)
    for k, v in (rew.get("scope_updates") or {}).items():
        scope[k] = v

    # Legacy single-hop fallback (only if rewriter didn't mark as follow-up and we have last_question)
    if not is_followup_turn and last_question:
        try:
            if is_followup(last_question, question):
                resolved_q = merge_followup(last_question, question)
                is_followup_turn = True
        except Exception:
            pass

    # -- (3) Intent classification on the RESOLVED question
    intent = classify_intent(resolved_q)
    is_weekly = (intent.get("intent") == "weekly_report")

    # ===========================================
    # Summarize a single contract (RAG text path)
    # ===========================================
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
        stream = stream_contract_summary_from_text(
            {"retrieval": "ordered_chunks", "readable_id": rid, "question": resolved_q}
        )
        # Update state (primary_response is a text anchor for this turn)
        new_primary = {
            "type": "text",
            "value": f"Summary generated for {rid}",
            "context": f"contract {rid}"
        }
        return {
            "sql": "",
            "columns": [],
            "rows": [],
            "stream": stream,
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary
        }

    # ===========================================
    # Compare two contracts (RAG text path)
    # ===========================================
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
        stream = stream_contract_summary_from_text(
            {"retrieval": "compare", "ids": [a, b], "question": resolved_q,
             "texts": ["\n".join(grab(a)), "\n".join(grab(b))]}
        )
        new_primary = {
            "type": "text",
            "value": f"Comparison generated for {a} vs {b}",
            "context": f"{a} vs {b}"
        }
        return {
            "sql": "",
            "columns": [],
            "rows": [],
            "stream": stream,
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary
        }

    # ===========================================
    # Text mention count (keyword Boolean)
    # ===========================================
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
        # Build exec-brief stream with prior anchor if follow-up
        numeric_value = None
        if len(rows) == 1 and len(cols) == 1 and isinstance(rows[0][0], (int,float,Decimal)):
            numeric_value = float(rows[0][0])
        payload = {
            "question": resolved_q,
            "sql": sql,
            "columns": cols,
            "rows_preview": safe_json(rows[:50]),
            "row_count_returned": len(rows),
            "true_numeric_result": numeric_value,
            "intent": intent,
            "primary_response": primary_response if is_followup_turn else None
        }
        stream = stream_general_from_payload(payload)
        new_primary = {
            "type": "numeric" if numeric_value is not None else "text",
            "value": numeric_value if numeric_value is not None else "",
            "context": resolved_q
        }
        return {
            "sql": sql,
            "columns": cols,
            "rows": rows,
            "stream": stream,
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary
        }

    # ===========================================
    # Text snippets (keyword & proximity)
    # ===========================================
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
        else:
            term = terms[0] if terms else "termination"
            sql = """SELECT readable_id,chunk_id,LEFT(chunk_text,300) AS snippet
FROM ic.contract_chunks WHERE chunk_text ILIKE '%'||%s||'%' ORDER BY readable_id,chunk_id LIMIT %s"""
            cols, rows = run_sql(sql, (term, limit))

        payload = {
            "question": resolved_q,
            "sql": sql,
            "columns": cols,
            "rows_preview": safe_json(rows[:50]),
            "intent": intent,
            "primary_response": primary_response if is_followup_turn else None
        }
        stream = stream_general_from_payload(payload)
        new_primary = {
            "type": "text",
            "value": f"{len(rows)} snippets",
            "context": resolved_q
        }
        return {
            "sql": sql,
            "columns": cols,
            "rows": rows,
            "stream": stream,
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary
        }

    # ===========================================
    # Generic (SQL path) – vendor/approvals/clauses/imported/etc.
    # ===========================================
    try:
        sql = ask_for_sql(resolved_q, weekly_allowed=is_weekly)
        validate_sql_safe(sql)

        # Parameter safety (rare): if %s are present, try binding vendor_term uniformly.
        params: Optional[Tuple[Any, ...]] = None
        unquoted_count = _count_unquoted_percent_s(sql)
        if unquoted_count > 0:
            intent_vendor = intent.get("vendor_term")
            if intent_vendor:
                params = tuple([intent_vendor] * unquoted_count)
            else:
                raise ValueError(
                    "Generated SQL contains unbound %s placeholders outside of string literals, "
                    "but no parameters were provided to bind."
                )

        sections = _extract_sections(sql)

        # ---------- Weekly bundle ----------
        if is_weekly:
            structured = []
            target_sections = sections if sections else [{"title": None, "sql": sql}]
            for sec in target_sections:
                try:
                    cols, rows = run_sql(sec["sql"])
                    metric_name, metric_value = _derive_metric(cols, rows)
                    if metric_name and "value" in (metric_name or "").lower() and isinstance(
                        metric_value, (int, float, Decimal)
                    ):
                        metric_value = f"${float(metric_value):,.0f}"
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
                            } if metric_name else None,
                        }
                    )
                except Exception as inner_err:
                    structured.append(
                        {"title": sec["title"], "sql": sec["sql"], "error": str(inner_err)}
                    )

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
                if x["title"] in canonical_order else 999
            )

            payload = {
                "question": resolved_q,
                "report_type": "weekly",
                "sections": structured,
                "intent": intent,
                "sql": sql,
                "primary_response": primary_response if is_followup_turn else None
            }
            return {
                "sql": sql,
                "columns": [],
                "rows": [],
                "stream": stream_summary_from_payload(payload),
                "intent_json": intent,
                "conversation_summary": updated_summary,
                "scope": scope,
                "resolved_question": resolved_q,
                "primary_response": {
                    "type": "text",
                    "value": "Weekly report generated",
                    "context": "weekly report"
                }
            }

        # ---------- Single-statement normal SQL ----------
        single_sql = sections[0]["sql"] if sections else sql
        cols, rows = run_sql(single_sql, params)

        # Build exec-brief with prior anchor (if follow-up)
        numeric_value = None
        if len(rows) == 1 and len(cols) == 1:
            val = rows[0][0]
            if isinstance(val, (int, float, Decimal)):
                numeric_value = float(val)

        payload = {
            "question": resolved_q,
            "sql": single_sql,
            "columns": cols,
            "rows_preview": safe_json(rows[:50]),
            "row_count_returned": len(rows),
            "true_numeric_result": numeric_value,
            "intent": intent,
            "primary_response": primary_response if is_followup_turn else None
        }
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": build_general_summarizer_prompt()},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            stream=True,
        )

        # ✅ Only save primary_response if there's a true numeric result (for follow-ups)
        if numeric_value is not None:
            new_primary = {
                "type": "numeric",
                "value": numeric_value,
                "context": resolved_q
            }
        else:
            new_primary = None  # don't store grouped row counts like "21 rows"


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
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary if new_primary else primary_response,

        }

    except Exception as e:
        # Error → Exec-brief the error cleanly
        payload = {
            "question": resolved_q,
            "sql": sql if "sql" in locals() else "",
            "error": str(e),
            "intent": intent,
            "primary_response": primary_response if is_followup_turn else None
        }
        return {
            "sql": sql if "sql" in locals() else "",
            "columns": [],
            "rows": [],
            "stream": stream_general_from_payload(payload),
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": primary_response  # keep prior anchor on error
        }

    # Fallback (should not reach)
    payload = {
        "question": resolved_q,
        "error": "Unhandled query path reached with no output",
        "intent": intent,
        "primary_response": primary_response if is_followup_turn else None
    }
    return {
        "sql": "",
        "columns": [],
        "rows": [],
        "stream": stream_general_from_payload(payload),
        "intent_json": intent,
        "conversation_summary": updated_summary,
        "scope": scope,
        "resolved_question": resolved_q,
        "primary_response": primary_response
    }
