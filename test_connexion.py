"""
Test de Python vers PostgreSQL actadiag_rag
"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def test_connexion():
    try:
        conn = psycopg2.connect(
            host    = os.getenv("DB_HOST", "localhost"),
            port    = os.getenv("DB_PORT", 5433),
            dbname  = os.getenv("DB_NAME", "actadiag_rag"),
            user    = os.getenv("DB_USER", "postgres"),
            password= os.getenv("DB_PASSWORD")
        )
        print("Connexion PostgreSQL réussie ✓")

        with conn.cursor() as cur:
            # Vérifier les tables
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = cur.fetchall()
            print(f"Tables trouvées : {[t[0] for t in tables]}")

            # Vérifier pgvector
            cur.execute("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname = 'vector'
            """)
            ext = cur.fetchone()
            print(f"pgvector : {ext[0]} v{ext[1]} ✓")

        conn.close()
        print("\nTout est prêt pour l'Étape 2 !")

    except Exception as e:
        print(f"Erreur connexion : {e}")

if __name__ == "__main__":
    test_connexion()