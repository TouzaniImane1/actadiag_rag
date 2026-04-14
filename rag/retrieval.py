"""
Étape 6 — Retrieval RAG
Recherche sémantique dans rag_chunks via pgvector.
"""
import sys
import os
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

import requests
from dotenv import load_dotenv
from db.connection import get_connection

load_dotenv()

NB_CHUNKS = 5

def vectoriser_question(question):
    """Convertit la question en vecteur via Ollama."""
    response = requests.post(
        "http://localhost:11434/api/embeddings",
        json={
            "model" : "nomic-embed-text",
            "prompt": question
        }
    )
    return response.json()["embedding"]

def retrieve_rag_context(question, culture=None,
                         nb_resultats=NB_CHUNKS):
    """
    Recherche les chunks les plus pertinents
    pour une question donnée.
    """
    import time
    debut = time.time()

    # Vectoriser la question
    vecteur = vectoriser_question(question)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if culture:
                cur.execute("""
                    SELECT
                        contenu_texte,
                        source,
                        culture,
                        1 - (embedding <=> %s::vector) AS score
                    FROM rag_chunks
                    WHERE culture ILIKE %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (
                    str(vecteur), f"%{culture}%",
                    str(vecteur), nb_resultats
                ))
            else:
                cur.execute("""
                    SELECT
                        contenu_texte,
                        source,
                        culture,
                        1 - (embedding <=> %s::vector) AS score
                    FROM rag_chunks
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (
                    str(vecteur),
                    str(vecteur),
                    nb_resultats
                ))

            chunks = cur.fetchall()

    finally:
        conn.close()

    duree_ms = (time.time() - debut) * 1000
    print(f"  Retrieval : {duree_ms:.0f} ms "
          f"({len(chunks)} chunks)")

    if not chunks:
        return "Aucune information réglementaire trouvée."

    return formater_contexte(chunks)

def formater_contexte(chunks):
    """Formate les chunks pour injection dans le LLM."""
    contexte = "=== INFORMATIONS RÉGLEMENTAIRES ONSSA ===\n\n"

    for i, chunk in enumerate(chunks, 1):
        score = chunk.get("score", 0)
        source = chunk.get("source", "ONSSA")
        contexte += f"[Source {i} — {source} | "
        contexte += f"Pertinence : {float(score):.0%}]\n"
        contexte += f"{chunk.get('contenu_texte', '')}\n"
        contexte += "-" * 40 + "\n"

    contexte += "\n=== FIN INFORMATIONS RÉGLEMENTAIRES ===\n"
    contexte += "Cite toujours ta source ONSSA.\n"

    return contexte

def tester_retrieval():
    """
    Teste le retrieval sur des questions exemples.
    """
    print("="*50)
    print("ÉTAPE 6 — TEST RETRIEVAL RAG")
    print("="*50)

    questions_test = [
        {
            "question": "Quels fongicides sont homologués "
                       "contre Botrytis sur fraise ?",
            "culture" : "Fraisier"
        },
        {
            "question": "Quel est le délai avant récolte "
                       "pour les produits sur tomate ?",
            "culture" : "Tomate"
        },
        {
            "question": "Insecticides autorisés sur agrumes",
            "culture" : "Agrumes"
        },
        {
            "question": "Produits contre les acariens",
            "culture" : None
        },
        {
            "question": "Herbicides homologués pour blé",
            "culture" : None
        },
    ]

    for i, test in enumerate(questions_test, 1):
        print(f"\n--- Question {i} ---")
        print(f"Q : {test['question']}")
        if test['culture']:
            print(f"Culture : {test['culture']}")

        contexte = retrieve_rag_context(
            test['question'],
            culture=test['culture']
        )
        print(contexte[:500] + "...")
        print()

if __name__ == "__main__":
    tester_retrieval()