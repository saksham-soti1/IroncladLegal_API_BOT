# schema_introspect.py
from typing import Dict, List
from db import get_conn

def get_live_schema() -> Dict[str, List[str]]:
    """
    Returns {'ic.workflows': [...cols], 'ic.documents': [...], ...}
    so GPT always sees the *real* tables/columns (including new ones like expiration_date).
    """
    sql = """
    SELECT table_schema, table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'ic'
    ORDER BY table_name, ordinal_position;
    """
    out: Dict[str, List[str]] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for schema, t, c in cur.fetchall():
                out.setdefault(f"{schema}.{t}", []).append(c)
    return out
