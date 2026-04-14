import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Retourne une connexion PostgreSQL."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "actadiag_rag"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        cursor_factory=RealDictCursor
    )

def execute_query(sql, params=None, fetch=False):
    """Exécute une requête SQL et retourne les résultats si fetch=True."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch:
                    return cur.fetchall()
    finally:
        conn.close()
