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
You are a legal contracts analytics assistant. Write a single, safe PostgreSQL SELECT query against schema ic.

HARD RULES:
- SELECT-only. Never emit CREATE/INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/GRANT/REVOKE/MERGE/VACUUM/COPY/SET/SHOW.
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


CONSTANTS:
- Do NOT use parameter placeholders like %s. Inline constants as proper SQL string literals (escape ' by doubling).
- Return exactly one SQL block fenced with ```sql ... ```
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

def validate_sql_safe(sql:str)->None:
    body = sql.strip()
    if body.endswith(";"): body = body[:-1].strip()
    if ";" in body: raise ValueError("Disallowed: multiple statements.")
    if PROHIBITED.search(body): raise ValueError("Only SELECT allowed.")
    if not body.lower().startswith("select"): raise ValueError("Must start with SELECT.")

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
def build_summarizer_prompt()->str:
    return (
        "You are a precise legal/contract analyst. Given the user's question and either SQL results or retrieved text, "
        "write a concise, factual answer:\n"
        "1) Direct Answer\n2) How it was computed\n3) Caveats\n"
    )

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
def _answer_with_rows(question,sql,cols,rows,intent):
    payload={"question":question,"sql":sql,"columns":cols,"rows_preview":safe_json(rows[:50]),"row_count_returned":len(rows),"intent":intent}
    return {"sql":sql,"columns":cols,"rows":rows,"stream":stream_summary_from_payload(payload),"intent_json":intent}

def _answer_with_text(question,meta,texts,intent):
    payload={"question":question,"meta":meta,"text_blobs":texts,"intent":intent}
    return {"sql":meta.get("sql",""),"columns":[],"rows":[],"stream":stream_summary_from_payload(payload),"intent_json":intent}

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
def answer_question(question:str)->Dict[str,Any]:
    intent=classify_intent(question)

    # Summarize (RAG)
    if intent["intent"]=="summarize_contract" and intent.get("readable_ids"):
        rid=intent["readable_ids"][0]
        cols,rows=run_sql("SELECT chunk_id,chunk_text FROM ic.contract_chunks WHERE readable_id=%s ORDER BY chunk_id",(rid,),max_rows=5000)
        texts=[r[1] for r in rows]; acc=0; out=[]
        for t in texts:
            if acc+len(t)>180_000: break
            out.append(t); acc+=len(t)
        return _answer_with_text(question,{"retrieval":"ordered_chunks","readable_id":rid},out,intent)

    # Compare
    if intent["intent"]=="compare_contracts" and len(intent.get("readable_ids",[]))>=2:
        a,b=intent["readable_ids"][:2]
        def grab(rid):
            c,r=run_sql("SELECT chunk_id,chunk_text FROM ic.contract_chunks WHERE readable_id=%s ORDER BY chunk_id",(rid,),max_rows=5000)
            texts=[x[1] for x in r]; acc=0; out=[]
            for t in texts:
                if acc+len(t)>120_000: break
                out.append(t); acc+=len(t)
            return out
        return _answer_with_text(question,{"retrieval":"compare","ids":[a,b]},["\n".join(grab(a)),"\n".join(grab(b))],intent)

    # Text mention count
    if intent["intent"]=="text_mention_count":
        op=intent.get("logic",{}).get("operator","AND").upper()
        if op not in ("AND","OR"): op="AND"
        inc=intent.get("terms",[]); exc=intent.get("logic",{}).get("exclude",[])
        inc_where,inc_params=_ilike_clause_frag("c",inc,op)
        not_where,not_params=_not_frag("c",exc)
        sql=f"""WITH matches AS (
  SELECT DISTINCT c.readable_id FROM ic.contract_chunks c
  WHERE {inc_where}{not_where}
) SELECT COUNT(*) AS contracts_with_term,
         ARRAY(SELECT readable_id FROM matches ORDER BY readable_id LIMIT 5) AS example_ids
FROM matches"""
        cols,rows=run_sql(sql,tuple(inc_params+not_params))
        return _answer_with_rows(question,sql,cols,rows,intent)

    # Snippets
    if intent["intent"]=="text_snippets":
        terms=intent.get("terms",[])[:2]; near=intent.get("near",{}); limit=10
        if len(terms)>=2 and near.get("enabled",False):
            t1,t2=terms[0],terms[1]; win=int(near.get("window",120))
            pattern=f"(?is)({re.escape(t1)}.{{0,{win}}}{re.escape(t2)}|{re.escape(t2)}.{{0,{win}}}{re.escape(t1)})"
            sql="""SELECT readable_id,chunk_id,LEFT(chunk_text,300) AS snippet
FROM ic.contract_chunks WHERE chunk_text ~ %s LIMIT %s"""
            cols,rows=run_sql(sql,(pattern,limit))
            return _answer_with_rows(question,sql,cols,rows,intent)
        else:
            term=terms[0] if terms else "termination"
            sql="""SELECT readable_id,chunk_id,LEFT(chunk_text,300) AS snippet
FROM ic.contract_chunks WHERE chunk_text ILIKE '%'||%s||'%' ORDER BY readable_id,chunk_id LIMIT %s"""
            cols,rows=run_sql(sql,(term,limit))
            return _answer_with_rows(question,sql,cols,rows,intent)

    # ✅ Generic → GPT builds SQL (covers vendor/counterparty, clauses, and all other metadata analytics)
    sql=ask_for_sql(question)
    validate_sql_safe(sql)

    # Safety net: if the model still used %s placeholders, auto-bind a single repeated parameter (e.g., vendor_term).
    params: Optional[Tuple[Any,...]] = None
    if "%s" in sql:
        if intent.get("vendor_term"):
            n = sql.count("%s")
            params = tuple([intent["vendor_term"]] * n)
        else:
            # No safe param we can infer – better to error in dev than run wrong SQL
            raise ValueError("Generated SQL is parameterized but no parameters were detected to bind.")

    cols,rows=run_sql(sql, params)
    return _answer_with_rows(question,sql,cols,rows,intent)
