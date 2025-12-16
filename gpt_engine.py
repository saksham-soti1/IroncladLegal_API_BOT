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
                print("DEBUG RUN_SQL:", sql)
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows)
            print("DEBUG ROWS:", rows)
            return cols, rows

# =========================================================
# Embeddings
# =========================================================
EMBED_MODEL = os.getenv("EMBED_MODEL","text-embedding-3-small")

def embed_query(text:str)->List[float]:
    out = client.embeddings.create(model=EMBED_MODEL,input=text)
    return out.data[0].embedding

def vector_literal(vec: List[float]) -> str:
    # Return ONLY the bracketed vector. Psycopg2 will add the single quotes;
    # the SQL itself will add the ::vector cast.
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"


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
- For listing, return workflow-level fields (w.readable_id, w.title, etc.) and LIMIT 200 unless user asks otherwise.
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
PROHIBITED = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|MERGE|VACUUM|COPY|SET|SHOW)\b",
    re.IGNORECASE
)


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
    Strongly prefers the CURRENT resolved_question; prior context may guide scoping,
    but must never override the current question.
    """
    return (
        "You are a precise, executive-brief legal contracts analyst.\n"
        "You will receive a JSON payload that ALWAYS includes:\n"
        "  - 'columns' and 'rows' (SQL results preview)\n"
        "  - 'resolved_question' (the CURRENT turn's fully self-contained question)\n"
        "  - optional 'primary_response' (from a prior turn; may be numeric or grouped)\n"
        "  - optional 'scope' and 'relevant_history' (helpful context only)\n\n"
        "Write a crisp 1–2 sentence answer (no bullets) fully grounded in the payload.\n\n"
        "HARD RULES:\n"
        "- Use ONLY data from the payload; do not invent values.\n"
        "- ALWAYS interpret and answer the CURRENT 'resolved_question'.\n"
        "- Prior context ('primary_response', 'relevant_history', 'scope') can refine scoping\n"
        "  IF it is consistent with the CURRENT question; if there is any conflict, ignore the prior context.\n"
        "- If 'true_numeric_result' exists, that number is authoritative for totals.\n"
        "- If this turn is a follow-up AND 'primary_response' is provided:\n"
        "    • If primary_response.type == 'numeric': You MAY lead with\n"
        "      'Of the {primary_response.value} {primary_response.context}, ...' when the current question\n"
        "      is clearly a subset or breakdown of that number.\n"
        "    • If primary_response.type == 'grouped': Treat the group's labels as categories that follow-ups\n"
        "      may refer to (e.g., 'counterparties', 'internal'). If the CURRENT question mentions one of those\n"
        "      labels, treat it as a subset and answer accordingly. Do NOT assume a subset when the label\n"
        "      is not mentioned in the CURRENT question.\n"
        "- If grouping columns exist and one numeric column appears in all rows, assume it represents row-level counts.\n"
        "  If the question asks for a total, sum that column exactly — never guess.\n"
        "- If the result has only 1 row, report its value directly.\n"
        "- If 'text_singleton_value' exists and is not null, use that value as the authoritative SQL answer.\n"
        "  Write a concise 1–2 sentence interpretation of that value that answers the resolved_question.\n"
        "- Format money with $ and commas.\n"
        "- Only say 'No matching results were found' when the SQL 'rows' array is truly empty (length = 0). If rows contain any data, NEVER say this.\n"
        "- Keep to 1–2 sentences; no SQL, no tables.\n"
    )



def build_contract_summarizer_prompt() -> str:
    return (
        "You are a legal contract summarizer.\n"
        "You will receive a list of ordered text chunks under 'texts' for a single contract.\n"
        "Write a professional, structured summary titled 'Summary for <readable_id>'.\n"
        "First and foremost, write a short summary of the contract in a few sentences. Make sure its descriptive and captures the essence of the entirety of the contract.\n"
        "Include key sections ONLY when present:\n"
        "- Parties (who is involved)\n"
        "- Term (start + duration)\n"
        "- Termination (notice, triggers)\n"
        "- Obligations (what each party must do)\n"
        "- Confidentiality (what’s covered)\n"
        "- Payment (amounts, terms)\n"
        "- Governing Law\n"
        "- Notable Terms (anything unusual or specific)\n\n"
        "NEVER invent or guess — only include what’s present.\n"
        "Skip sections that are not mentioned in the text.\n"
        "Use bullet points where helpful. Be concise and accurate."
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
You rewrite user messages into fully self-contained questions and determine whether they are follow-ups to the active topic.

INPUTS:
- user_text: the raw user message
- relevant_history: a small list of 2–5 bullet points describing the current topic, active filters, or scope
- scope: a JSON dict of active filters (e.g., timeframe, status, vendor, department, step, record_type, approver, priority, etc.)
- prior_resolved_question: the last fully self-contained question that was asked

GOAL:
- Resolve whether this is a follow-up or a new topic.
- If it is a follow-up, inherit any relevant filters or labels from the current scope, unless explicitly overridden.
- If it is a new topic, start fresh (reset irrelevant filters).

RULES:

-- FOLLOW-UP DETECTION --

Elliptical Follow-Ups (Critical Rule):

- You MUST mark is_followup = true when the new user message is NOT a complete
  standalone question by itself AND requires the prior_resolved_question to make sense.

  A message is considered NOT standalone when:
    • it lacks a subject or object (“list them”, “show me”, “break it down”, “and the rest?”)
    • it uses incomplete references (“what about pricing?”, “and termination?”, “the vendor?”)
    • it only makes sense when tied to the prior question’s scope, entity, timeframe, or filters
    • its meaning is ambiguous or empty without the previous context

- In these cases:
    • treat the message as a follow-up even if no explicit referential keywords appear
    • inherit the prior scope exactly (unless user overrides it explicitly)
    • resolved_question must embed the prior context clearly

This rule ALWAYS applies before any other follow-up rules.


- You MUST mark is_followup = true only when the new message clearly depends on or refers back to the prior result, including:
    • Referential language: "those", "these", "that", "them", "of the ones", "what about", "and how many of those"
    • Incremental filters: "only high priority", "just legal", "by department", "from counterparties"
    • Comparative or continuation phrasing: "and how about", "now show me", "what about the rest"
    • Mentions or uses of a label (like "counterparties", "internal", "IT", "NDAs") that appeared in relevant_history **only when** the phrasing implies subset or continuation — not when the question stands alone.
      (e.g., “of the ones from counterparties” → follow-up; “how many counterparty contracts exist overall” → new topic)
    • Requests for further filtering, breakdown, or listing of a previous result (“list them”, “show which ones”)
    • Questions that reference a number, timeframe, or condition from a previous answer

- You MUST mark is_followup = false if:
    • The user starts a new analytical topic or timeframe
    • The message can be understood completely without previous context
    • The user reuses terms like “counterparties” or “vendors” generically, not as continuation (“how many counterparty contracts do we have this year” = new topic)

-- SCOPE INHERITANCE --

- If follow-up = true:
    • Inherit filters from prior scope unless overridden
    • Add any clearly implied label from relevant_history (e.g., "counterparty", "IT department") only when phrasing suggests “those” or a subset
    • Keep keys: status, timeframe, vendor, department, record_type, approver, step, priority

-- SCOPE RESETTING --

- If follow-up = false:
    • Do not inherit filters from the prior scope
    • Reset unrelated filters (e.g., timeframe, vendor, department, step)

-- RESOLVED QUESTION QUALITY --

- resolved_question must be a clear, standalone question explicitly listing filters and context
- Rewrite vague pronouns into explicit forms when follow-up = true
- Example:
      user_text: "and how many of those were MSAs?"
      resolved_question: "How many MSA contracts were completed this quarter?"

OUTPUT STRICT JSON ONLY:
{
  "is_followup": true|false,
  "resolved_question": "fully self-contained question string",
  "scope_updates": { "timeframe": "...", "status": "...", "vendor": "...", "approver": "...", "priority": "...", "record_type": "...", "step": "...", "department": "...", "ids": ["..."] },
  "reset_keys": ["timeframe", "vendor", "status", "record_type", ...],
  "topic_label": "short_topic_name",
  "notes": "brief rationale explaining why this is or isn't a follow-up"
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
    print("DEBUG RAW REWRITER OUTPUT:", txt)

    try:
        js = json.loads(txt)
        print("DEBUG PARSED REWRITER JSON:", js)

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
  "intent": "text_mention_count | text_snippets | summarize_contract | compare_contracts | semantic_find | similar_to_contract | weekly_report | sql_generic | rag_text_qa",
  "terms": [string],                     # any keywords to match on (if applicable)
  "logic": {"operator":"AND|OR","exclude":[string]},
  "near": {"enabled":true|false,"window":120},
  "readable_ids":[string],              # like IC-1234
  "query_text":string|null,
  "vendor_term":string|null,
  "notes":string|null
}

CRITICAL NON-NEGOTIABLE RULE:
- "query_text" MUST be an EXACT character-for-character copy of the user's original question.
- NEVER rewrite, rephrase, expand, shorten, or modify the user's question in any way.
- NEVER add generic words like "contract", "agreement", "document", "clause", etc.
- NEVER add vendor names, entity names, IC IDs, or inferred attributes unless the user explicitly typed them.
- The assistant MUST preserve the original user wording exactly.

---

## INTENT DEFINITIONS AND EXAMPLES:

### 🔹 text_mention_count  
Count how many contracts contain specific **words or phrases in the tet**.

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

### 🔹 rag_text_qa
Broad natural-language questions that require reading or interpreting contract text,
not just metadata or phrase matching.

Use this when the user asks about the *content* or *meaning* of clauses,
sections, or terms — even if they don’t quote words or use “snippet”.

**Trigger examples, not limited to these, use these as a guide and interpret the question to see if it is a RAG text question:**
- “what does IC-6420 say about termination?”
- “explain the confidentiality clause in this agreement”
- “what does the contract say about payment terms?”
- “how is governing law handled?”
- “does this mention how many hours the vendor works?”
- “what does it say about subcontractors?”
- “how does this define intellectual property?”
- “where does it describe indemnification?”
- “show me the language covering warranties”
- “tell me what the MSAs say about data privacy”
- “how are service levels described?”

**Key difference:**  
If the question requires *interpreting* or *retrieving text language*,
rather than counting or filtering by metadata, it is `rag_text_qa`.

Scope can be a single contract (e.g., “IC-6927”) or many (e.g., “our NDAs”);
the system will decide dynamically.

---
### 🔹 weekly_report  
User explicitly requests the **full Legal & Contract Weekly Report** — a structured summary of multiple metrics (the 11-section bundle).

Only classify as `weekly_report` when the user clearly asks for a **formal report or overview**, not just when they mention a timeframe.

**Trigger phrases (explicit requests only):**
- “generate the weekly report”
- “create the weekly legal report”
- “show me the full weekly report”
- “make the weekly report”
- “weekly report for legal team”
- “give me the legal team report”
- “generate the full report for this week”

**Do NOT use this intent** for general time-based questions such as:
- “how many contracts were created this week”
- “show contracts completed this week”
- “what’s the spend this week”
Those should remain `sql_generic`.

In summary:
- `weekly_report` = user explicitly wants the multi-section weekly summary.
- `sql_generic` = any normal analytical or timeframe question (even if it includes 'week' or 'weekly').

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

def classify_intent(q: str) -> Dict[str, Any]:
    ids = [m.group(0).upper() for m in IC_ID_RE.finditer(q)]
    quoted = re.findall(r"['\"]([^'\"]+)['\"]", q)
    hints = {"readable_ids_detected": ids, "quoted_terms_detected": quoted}

    msgs = [
        {"role": "system", "content": INTENT_SYSTEM_PROMPT},
        {"role": "system", "content": "HINTS: " + json.dumps(hints)},
        {"role": "user", "content": q},
    ]

    resp = client.chat.completions.create(model="gpt-4o-mini",
                                          temperature=0,
                                          messages=msgs)
    content = resp.choices[0].message.content or "{}"
    print("DEBUG RAW INTENT LLM OUTPUT:", content)

    try:
        js = json.loads(_extract_first_json(content))
    except:
        js = {}

    # Default shape
    js.setdefault("intent", "sql_generic")
    js.setdefault("terms", [])
    js.setdefault("logic", {"operator": "AND", "exclude": []})
    js.setdefault("near", {"enabled": False, "window": 120})

    # ======================================================
    # 🔥 READABLE IDS — HARD NORMALIZATION (THE REAL FIX)
    # ======================================================
    rids = js.get("readable_ids")
    if not isinstance(rids, list):
        rids = []
    # Remove non-string values
    rids = [rid for rid in rids if isinstance(rid, str)]
    js["readable_ids"] = rids
    # ======================================================

    # Terms must always be a list
    if js.get("terms") is None:
        js["terms"] = []

    # Logic must always be a dict
    if js.get("logic") is None or not isinstance(js["logic"], dict):
        js["logic"] = {"operator": "AND", "exclude": []}
    else:
        js["logic"].setdefault("operator", "AND")
        js["logic"].setdefault("exclude", [])
        if js["logic"]["exclude"] is None:
            js["logic"]["exclude"] = []

    # Normalize readable IDs → IC-#### only
    valid_ids = []
    for rid in js["readable_ids"]:
        if isinstance(rid, str) and re.fullmatch(r"IC-\d+", rid.strip(), re.IGNORECASE):
            valid_ids.append(rid.upper())
    js["readable_ids"] = valid_ids

    js.setdefault("query_text", None)
    js.setdefault("vendor_term", None)
    js.setdefault("notes", None)

    # Clause rule
    if "clause" in q.lower():
        js["intent"] = "sql_generic"

    print("DEBUG FINAL INTENT JSON:", js)

    # Final sanitize (guaranteed list — safe to iterate)
    ids = js.get("readable_ids", [])
    if not isinstance(ids, list):
        ids = []
    cleaned = []
    for x in ids:
        if isinstance(x, str) and x.strip():
            cleaned.append(x.strip())
    js["readable_ids"] = cleaned

    return js




def extract_title_terms(question: str) -> List[str]:
    """
    Extract the specific title-identifying words the user typed.
    This uses ONLY the question itself (no title list).
    """

    system_prompt = (
        "Your ONLY task is to extract the exact user-typed words that identify a contract title.\n"
        "You do NOT answer the question.\n"
        "You do NOT need to know the list of titles.\n"
        "You ONLY pull out the meaningful title-indicating words the user typed.\n\n"

        "WHAT TO EXTRACT:\n"
        "- Company/vendor names (e.g., hamilton, lonza, pfizer, stanford)\n"
        "- Contract-type indicators the user typed (e.g., nda, msa, dmsa, cda, sow)\n"
        "- Product/model identifiers if present (e.g., abc123)\n\n"

        "WHAT NOT TO EXTRACT:\n"
        "- Clause/topic words (confidentiality, payment, pricing)\n"
        "- Generic verbs or filler language (what, does, say, show, about)\n\n"

        "EXAMPLES:\n"
        "User: 'What does the Hamilton Company NDA say about confidentiality?'\n"
        "Output: {\"title_terms\": [\"hamilton\", \"company\", \"nda\"]}\n\n"
        "User: 'Explain the Lonza DMSA termination clause'\n"
        "Output: {\"title_terms\": [\"lonza\", \"dmsa\"]}\n\n"
        "User: 'Show me what the Pfizer Master Services Agreement says about IP'\n"
        "Output: {\"title_terms\": [\"pfizer\", \"master\", \"services\"]}\n\n"

        "RULES:\n"
        "- Only return words the user actually typed.\n"
        "- Lowercase all extracted words.\n"
        "- Never hallucinate or infer new words.\n"
        "- Always output strict JSON: {\"title_terms\": [ ... ]}"
    )

    user_prompt = f"User Question: \"{question}\""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    )

    txt = resp.choices[0].message.content or "{}"

    try:
        js = json.loads(_extract_first_json(txt))
        out = js.get("title_terms", [])
        return [w.lower() for w in out if isinstance(w, str)]
    except Exception:
        return []


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
    # Enrich relevant_history with grouped labels from the last primary_response (if any)
    extra_labels = []
    if primary_response and isinstance(primary_response, dict):
        if primary_response.get("type") == "grouped":
            extra_labels = [lbl.lower() for lbl in primary_response.get("labels", []) if lbl]
    scope.setdefault("relevant_history", [])
    if extra_labels:
        # merge labels into history so the rewriter can see them as contextual keywords
        merged = list(dict.fromkeys(scope["relevant_history"] + extra_labels))
        scope["relevant_history"] = merged[-20:]

    # -- (1) History selector: compress prior context into focused bullets + updated summary
    # Merge new bullets with old to persist memory beyond one turn
    hs = history_selector(conversation_summary, scope, resolved_question, primary_response)
    new_relevant = hs.get("relevant_history", [])
    if conversation_summary and "relevant_history" in scope:
        combined_history = list(dict.fromkeys(scope["relevant_history"] + new_relevant))
    else:
        combined_history = new_relevant
    relevant_history = combined_history[-10:]  # keep the last 10 for clarity
    updated_summary = hs.get("updated_summary", conversation_summary)
    # keep history persistent and capped for context longevity
    existing_history = scope.get("relevant_history", [])
    merged_history = list(dict.fromkeys(existing_history + relevant_history))
    scope["relevant_history"] = merged_history[-20:]

    # -- (2) Follow-up detector + rewriter (preferred path)
    print("DEBUG FIRST-TURN CHECK — last_question:", last_question)
    print("DEBUG FIRST-TURN CHECK — prior_resolved_question:", resolved_question)
    print("DEBUG FIRST-TURN CHECK — is_followup BEFORE rewriter SHOULD BE FALSE")

        # --- FIRST TURN SAFETY: do NOT rewrite the user's question ---
    if last_question is None:
        print("DEBUG: FIRST TURN — SKIPPING REWRITER ENTIRELY")
        is_followup_turn = False
        resolved_q = question

        # Ensure rew exists so later code does not crash
        rew = {"reset_keys": [], "scope_updates": {}}
    else:
        rew = followup_rewriter(
            user_text=question,
            relevant_history=relevant_history,
            scope=scope,
            prior_resolved_question=resolved_question
        )
        is_followup_turn = bool(rew.get("is_followup"))
        resolved_q = rew.get("resolved_question") or question


    print("DEBUG IS_FOLLOWUP_TURN:", is_followup_turn)
    print("DEBUG RESOLVED_Q AFTER FIRST-TURN LOGIC:", resolved_q)

    # -- (3) Intent classification on the RESOLVED question
    intent = classify_intent(resolved_q)
    print("DEBUG INTENT:", intent)
    print("DEBUG RESOLVED QUESTION:", resolved_q)


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


    is_weekly = (intent.get("intent") == "weekly_report")

    # ===========================================
    # Summarize a single contract (RAG text path)
    # ===========================================
    # ===========================================
# Summarize a single contract (RAG text path)
# ===========================================
    # ===========================================
    # Summarize a single contract (RAG text path)
    # ===========================================
    if intent["intent"] == "summarize_contract":
        # If no explicit IC id was given, try to reuse the last turn's example_ids
        if not intent.get("readable_ids"):
            prior_ids = (primary_response or {}).get("example_ids") or []
            if isinstance(prior_ids, (list, tuple)) and prior_ids:
                intent["readable_ids"] = [prior_ids[0]]

        # Only proceed if we now have an ID
        if intent.get("readable_ids"):
            rid = intent["readable_ids"][0]
            cols, rows = run_sql(
                "SELECT chunk_id,chunk_text FROM ic.contract_chunks WHERE readable_id=%s ORDER BY chunk_id",
                (rid,),
                max_rows=5000,
            )
            texts = [r[1] for r in rows]
            acc, out = 0, []
            for t in texts:
                if acc + len(t) > 180_000:
                    break
                out.append(t)
                acc += len(t)

            stream = stream_contract_summary_from_text(
                {
                    "retrieval": "ordered_chunks",
                    "readable_id": rid,
                    "question": resolved_q,
                    "texts": out
                }
            )

            new_primary = {"type": "text", "value": f"Summary generated for {rid}", "context": f"contract {rid}"}
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
    # Unified RAG text QA (broad question understanding)
    # ===========================================
    if intent["intent"] == "rag_text_qa":
        # --- Detect single vs multi-contract scope ---
        # --- Detect single vs multi-contract scope ---
        # NEVER use semantic nearest-neighbors to determine contract scope.
# Only use explicit IDs or title extraction.
        readable_ids = intent.get("readable_ids") or []
        # If this is a follow-up RAG query AND no explicit new contract was given,
        # lock onto the previously active contract
        if is_followup_turn and scope.get("active_contract_id") and not intent.get("readable_ids"):
            readable_ids = [scope["active_contract_id"]]


        # Attempt fallback: check title match if no IC-ID provided
        # Attempt fallback: fuzzy title match using model-based title term extraction
        if not readable_ids:
            # 1. Fetch known titles (anchor set)
            # -------------------------------------------
            # NEW PREFILTER: detect user words & limit title set BEFORE LLM extraction
            # -------------------------------------------
            # Extract raw user-typed words
            raw_words = re.findall(r"[a-zA-Z0-9]+", resolved_q.lower())
            title_words = [w for w in raw_words if len(w) >= 3]
            print("DEBUG RAW WORDS:", raw_words)
            print("DEBUG TITLE WORDS (words >=3 chars):", title_words)


            prefiltered_titles = []
            if title_words:
                where_clauses = " AND ".join(["LOWER(title) ILIKE %s" for _ in title_words])
                params = [f"%{w}%" for w in title_words]

                sql_prefilter = f"""
                    SELECT title
                    FROM ic.contract_texts
                    WHERE {where_clauses}
                    LIMIT 10
                """

                print("DEBUG PREFILTER TITLE SQL:", sql_prefilter)
                print("DEBUG PREFILTER PARAMS:", params)

                cols, title_rows = run_sql(sql_prefilter, tuple(params))
                prefiltered_titles = [r[0] for r in title_rows if r[0]]
                print("DEBUG PREFILTERED_TITLES_COUNT:", len(prefiltered_titles))
                print("DEBUG FIRST_5_PREFILTERED_TITLES:", prefiltered_titles[:5])



            candidate_terms = extract_title_terms(resolved_q)
            print("DEBUG CANDIDATE TERMS FROM LLM:", candidate_terms)
            print("DEBUG TITLE TERMS:", candidate_terms)

            print("DEBUG TYPE OF CANDIDATE_TERMS:", type(candidate_terms))
            if candidate_terms:
                like_clauses = " AND ".join(["LOWER(title) ILIKE %s" for _ in candidate_terms])
                params = [f"%{t}%" for t in candidate_terms]

                sql = f"""
                    SELECT readable_id
                    FROM ic.contract_texts
                    WHERE {like_clauses}
                    ORDER BY updated_at DESC
                    LIMIT 1
                """

                print("DEBUG TITLE SEARCH SQL:", sql)
                print("DEBUG TITLE SEARCH PARAMS:", params)

                cols, rows = run_sql(sql, tuple(params))
                if rows:
                    readable_ids = [rows[0][0]]



        is_single_contract = len(readable_ids) == 1
        rid = readable_ids[0] if is_single_contract else None
        if not readable_ids:
            return {
                "sql": "",
                "columns": [],
                "rows": [],
                "stream": iter(["I'm sorry, I couldn't find a matching contract based on the title keywords."]),
                "intent_json": intent,
                "conversation_summary": updated_summary,
                "scope": scope,
                "resolved_question": resolved_q,
                "primary_response": {
                    "type": "text",
                    "value": "No matching contract found for title keywords",
                    "context": "title fallback failure"
                }
            }
        qvec = embed_query(resolved_q)


        if is_single_contract:
            # single-contract: narrow to one doc
            # Remember active contract for follow-ups
            scope["active_contract_id"] = rid
            sql = """
                SELECT readable_id, chunk_id, chunk_text,
                       (embedding <=> %s::vector) AS distance
                FROM ic.contract_chunks
                WHERE readable_id = %s
                ORDER BY embedding <=> %s::vector
                LIMIT 24
            """
            cols, rows = run_sql(sql, (vector_literal(qvec), rid, vector_literal(qvec)))
        else:
            # multi-contract: search corpus
            sql = """
                SELECT readable_id, chunk_id, chunk_text,
                       (embedding <=> %s::vector) AS distance
                FROM ic.contract_chunks
                ORDER BY embedding <=> %s::vector
                LIMIT 40
            """
            cols, rows = run_sql(sql, (vector_literal(qvec), vector_literal(qvec)))

        # --- Prepare prompt dynamically ---
        if is_single_contract:
            system_prompt = (
                f"You are a legal contracts analyst. Your primary source is the provided text "
                f"chunks of contract {rid}. Cite exact phrases using (IC-#### #chunk_id) whenever possible.\n\n"

                "RULES:\n"
                "1. Always ground factual details (definitions, obligations, clauses) ONLY in the provided text.\n"
                "2. If the user's question asks for evaluation, comparison, risk assessment, typicality, "
                "industry standards, or interpretation that is NOT explicitly stated in the text:\n"
                "      • You MAY use your general legal and commercial knowledge.\n"
                "      • Make it clear when the text does NOT state something directly.\n"
                "      • Provide a reasoned, professional opinion based on common contract practices.\n"
                "3. Never fabricate contract-specific facts that are not in the text.\n"
            )
        else:
            system_prompt = (
                "You are a legal contracts analyst. Synthesize insights from multiple retrieved contracts.\n"
                "Use the text as primary evidence but you MAY use general legal knowledge when the user asks for:\n"
                "   • comparisons\n"
                "   • risk assessments\n"
                "   • industry-standard evaluations\n"
                "   • typicality/market-norm commentary\n"
                "Cite text when relevant. Do NOT fabricate contract-specific facts.\n"
            )


        payload = {
            "question": resolved_q,
            "chunks": [
                {"readable_id": r[0], "chunk_id": r[1], "text": r[2]}
                for r in rows
            ],
            "row_count": len(rows)
        }

        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
            ],
            stream=True,
        )

        new_primary = {
            "type": "text",
            "value": f"RAG answer for {'contract ' + rid if is_single_contract else 'multiple contracts'}",
            "context": rid or "corpus",
            "example_ids": [r[0] for r in rows[:5]]  # store top docs for follow-ups
        }

        return {
            "sql": sql,
            "columns": cols,
            "rows": safe_json(rows),
            "stream": (chunk.choices[0].delta.content
                       for chunk in stream
                       if chunk.choices and chunk.choices[0].delta.content),
            "intent_json": intent,
            "conversation_summary": updated_summary,
            "scope": scope,
            "resolved_question": resolved_q,
            "primary_response": new_primary,
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
        # NEW: capture example_ids (array of readable_ids) for follow-ups like "summarize it"
        example_ids = []
        if rows and 'example_ids' in cols:
            _idx = cols.index('example_ids')
            example_ids = rows[0][_idx] or []
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
            "scope": scope,
            "relevant_history": scope.get("relevant_history", []),
            "primary_response": primary_response if is_followup_turn else None
        }
        stream = stream_general_from_payload(payload)
        new_primary = {
            "type": "numeric" if numeric_value is not None else "text",
            "value": numeric_value if numeric_value is not None else "",
            "context": resolved_q,
            "example_ids": example_ids  # carry IDs forward for follow-ups
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
            "scope": scope,
            "relevant_history": scope.get("relevant_history", []),
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

        print("DEBUG: SINGLE SQL:", single_sql)
        print("DEBUG: PARAMS:", params)
    

        # Build exec-brief with prior anchor (if follow-up)
        numeric_value = None
        new_primary = None
        text_singleton_value = None

        # Case 1: single-row single-column
        if len(rows) == 1 and len(cols) == 1:
            val = rows[0][0]

            if isinstance(val, (int, float, Decimal)):
                numeric_value = float(val)
            else:
                # store the text result explicitly so the summarizer can use it
                text_singleton_value = str(val)

        

        payload = {
            "question": resolved_q,
            "sql": single_sql,
            "columns": cols,
            "rows_preview": safe_json(rows[:50]),
            "row_count_returned": len(rows),
            "has_rows": len(rows) > 0,
            "true_numeric_result": numeric_value,
            "text_singleton_value": text_singleton_value,
            "intent": intent,
            "scope": scope,
            "relevant_history": scope.get("relevant_history", []),
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


        if numeric_value is not None:
            # simple scalar result (e.g., “106”)
            new_primary = {
                "type": "numeric",
                "value": numeric_value,
                "context": resolved_q
            }
        else:
            # detect a simple grouped result: one text-like column + one numeric-like column
            # this lets us remember category labels like "counterparties", "internal", etc.
            try:
                text_col_idx = None
                for ci, cname in enumerate(cols):
                    if any(isinstance(r[ci], str) and r[ci].strip() for r in rows if r is not None):
                        text_col_idx = ci
                        break

                num_col_idx = None
                for ci, cname in enumerate(cols):
                    if any(isinstance(r[ci], (int, float, Decimal)) for r in rows if r is not None):
                        num_col_idx = ci
                        break

                if text_col_idx is not None and num_col_idx is not None and text_col_idx != num_col_idx:
                    labels = []
                    for r in rows:
                        v = r[text_col_idx]
                        if isinstance(v, str) and v.strip():
                            labels.append(v.strip())
                    if labels:
                        new_primary = {
                            "type": "grouped",
                            "context": resolved_q,
                            "group_col": cols[text_col_idx],
                            "value_col": cols[num_col_idx],
                            "labels": [l.lower() for l in labels][:100]
                        }
            except Exception:
                pass

        return {
            "sql": single_sql,
            "columns": cols,
            "rows": safe_json(rows),
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
        print("DEBUG SQL EXCEPTION:", repr(e))
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
