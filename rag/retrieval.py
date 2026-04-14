"""
Recherche sémantique dans rag_chunks via pgvector.
C'est le cœur du RAG — utilisé par llm_service.py.
"""
import os
import openai
from dotenv import load_dotenv
from db.connection import get_connection

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

MODELE_EMBEDDING = "text-embedding-3-small"
NB_CHUNKS_RETOURNES = 5

def vectoriser_question(question):
    """Convertit la question en vecteur."""
    response = openai.embeddings.create(
        model=MODELE_EMBEDDING,
        input=question
    )
    return response.data[0].embedding

def retrieve_rag_context(question, culture=None, nb_resultats=NB_CHUNKS_RETOURNES):
    """
    Recherche les chunks les plus pertinents pour une question.
    
    Paramètres :
        question    : la question de l'utilisateur
        culture     : filtre optionnel par culture (ex: 'Fraisier')
        nb_resultats: nombre de chunks à retourner
    
    Retourne un texte formaté prêt à injecter dans le prompt LLM.
    """
    # Vectoriser la question
    vecteur_question = vectoriser_question(question)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if culture:
                # Recherche filtrée par culture
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
                """, (vecteur_question, f"%{culture}%",
                      vecteur_question, nb_resultats))
            else:
                # Recherche sémantique globale
                cur.execute("""
                    SELECT
                        contenu_texte,
                        source,
                        culture,
                        1 - (embedding <=> %s::vector) AS score
                    FROM rag_chunks
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (vecteur_question, vecteur_question, nb_resultats))

            chunks = cur.fetchall()

    finally:
        conn.close()

    if not chunks:
        return "Aucune information réglementaire trouvée dans la base."

    return formater_contexte(chunks)

def formater_contexte(chunks):
    """
    Formate les chunks en bloc de contexte pour le LLM.
    """
    contexte = "=== INFORMATIONS RÉGLEMENTAIRES OFFICIELLES ===\n\n"

    for i, chunk in enumerate(chunks, 1):
        contexte += f"[Source {i} — {chunk['source']}]\n"
        contexte += f"{chunk['contenu_texte']}\n"
        contexte += f"Pertinence : {chunk['score']:.2%}\n"
        contexte += "-" * 40 + "\n"

    contexte += "\n=== FIN DES INFORMATIONS RÉGLEMENTAIRES ===\n"
    contexte += "Cite toujours la source (ONSSA ou GlobalG.A.P) dans ta réponse.\n"

    return contexte
