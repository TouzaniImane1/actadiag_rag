"""
Étape 6 du pipeline :
- Génère les embeddings pour chaque produit
- Stocke les chunks dans rag_chunks
"""
import os
import openai
from dotenv import load_dotenv
from db.connection import get_connection

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

MODELE_EMBEDDING = "text-embedding-3-small"
DIMENSION_VECTEUR = 1536

def construire_texte_chunk(produit):
    """
    Construit un texte descriptif à partir d'un produit.
    C'est ce texte qui sera converti en vecteur.
    """
    return f"""
    Produit phytosanitaire homologué ONSSA au Maroc.
    Nom commercial : {produit.get('nom_commercial', '')}
    Matière active : {produit.get('matiere_active', '')}
    Culture concernée : {produit.get('culture', '')}
    Usage / Maladie traitée : {produit.get('usage', '')}
    Dose recommandée : {produit.get('dose', '')}
    Société : {produit.get('societe', '')}
    Statut : Homologué ONSSA
    """.strip()

def generer_embedding(texte):
    """Appelle l'API OpenAI et retourne le vecteur."""
    response = openai.embeddings.create(
        model=MODELE_EMBEDDING,
        input=texte
    )
    return response.data[0].embedding

def generer_tous_les_embeddings():
    """
    Récupère tous les produits et génère leurs embeddings.
    Stocke les résultats dans rag_chunks.
    """
    conn = get_connection()

    try:
        # Récupérer tous les produits
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM produits_homologues")
            produits = cur.fetchall()

        print(f"Génération des embeddings pour {len(produits)} produits...")

        nb_generes = 0
        with conn:
            with conn.cursor() as cur:
                for produit in produits:
                    # Construire le texte
                    texte = construire_texte_chunk(produit)

                    # Générer le vecteur
                    vecteur = generer_embedding(texte)

                    # Insérer dans rag_chunks
                    cur.execute("""
                        INSERT INTO rag_chunks
                            (source, culture, contenu_texte, embedding, metadata)
                        VALUES (%s, %s, %s, %s::vector, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        "ONSSA",
                        produit.get("culture"),
                        texte,
                        vecteur,
                        '{"type": "produit_homologue"}'
                    ))
                    nb_generes += 1

                    if nb_generes % 100 == 0:
                        print(f"  {nb_generes}/{len(produits)} embeddings générés...")

        print(f"Embeddings terminés : {nb_generes} chunks stockés.")
        return nb_generes

    finally:
        conn.close()
