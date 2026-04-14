"""
Étape 7 du pipeline :
- Enregistre chaque synchronisation dans sync_log
"""
from db.connection import get_connection

def log_sync(source, hash_fichier, nb_lignes, statut, message_erreur=None):
    """Enregistre le résultat d'une synchronisation."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sync_log
                        (source, hash_fichier, statut,
                         nb_lignes_importees, message_erreur)
                    VALUES (%s, %s, %s, %s, %s)
                """, (source, hash_fichier, statut,
                      nb_lignes, message_erreur))
        print(f"Log enregistré : {source} — {statut} — {nb_lignes} lignes")
    finally:
        conn.close()
