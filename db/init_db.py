"""
Script d'initialisation de la base actadiag_rag.
Lance ce script une seule fois au début du projet.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def init_database():
    print("Initialisation de la base actadiag_rag...")

    # Connexion à PostgreSQL (base postgres par défaut)
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname="postgres",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Créer la base si elle n'existe pas
        cur.execute("""
            SELECT 1 FROM pg_database 
            WHERE datname = 'actadiag_rag'
        """)
        if not cur.fetchone():
            cur.execute("CREATE DATABASE actadiag_rag")
            print("Base actadiag_rag créée.")
        else:
            print("Base actadiag_rag déjà existante.")

    conn.close()

    # Connexion à actadiag_rag pour créer les tables
    conn2 = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname="actadiag_rag",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    # Lire et exécuter le schema.sql
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    with conn2:
        with conn2.cursor() as cur:
            cur.execute(schema_sql)

    conn2.close()
    print("Tables créées avec succès !")
    print("Base actadiag_rag prête.")

if __name__ == "__main__":
    init_database()
