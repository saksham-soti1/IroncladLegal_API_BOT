import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # <-- make sure .env values are available

def get_conn():
    host = os.getenv("PG_HOST")
    port = int(os.getenv("PG_PORT", "5432"))
    db   = os.getenv("PG_DB", "postgres")
    user = os.getenv("PG_USER")
    pwd  = os.getenv("PG_PASSWORD")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
        sslmode="require",
    )
