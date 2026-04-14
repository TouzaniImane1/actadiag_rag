"""
Étape 5 — Génération des embeddings
Convertit chaque produit en vecteur numérique
et le stocke dans rag_chunks.
"""
import sys
import os
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from dotenv import load_dotenv
from db.connection import get_connection
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

MODELE_EMBEDDING = "text-embedding-3-small"
DIMENSION        = 1536

def construire_chunk_produit(row):
    """
    Construit le texte descriptif d'un produit.
    C'est ce texte qui sera converti en vecteur.
    """
    return f"""Produit phytosanitaire homologué ONSSA au Maroc.
Nom commercial : {row.get('nom_commercial', '')}
Catégorie : {row.get('categorie', '')}
Matière active : {row.get('matiere_active', '')} ({row.get('teneur', '')})
Culture concernée : {row.get('culture', '')}
Usage / Maladie traitée : {row.get('usage', '')}
Dose recommandée : {row.get('dose', '')}
Délai avant récolte (DAR) : {row.get('dar', '')} jours
Nombre d'applications : {row.get('nb_applications', '')}
Valable jusqu'au : {row.get('valable_jusqu_au', '')}
Détenteur : {row.get('detenteur', '')}
Numéro homologation : {row.get('numero_homologation', '')}
Statut : Homologué ONSSA""".strip()

def construire_chunk_changement(row):
    """
    Construit le texte descriptif d'un changement dose/DAR.
    """
    type_ch = row.get('type_changement', '')

    if type_ch == 'dose':
        return f"""Changement de dose homologué ONSSA.
Produit : {row.get('produit', '')}
Composition : {row.get('composition', '')}
Usage : {row.get('usage', '')}
Nouvelle dose : {row.get('nouvelle_dose', '')}
Ancienne dose : {row.get('ancienne_dose', '')}
Date effet : {row.get('date_effet', '')}
Détenteur : {row.get('detenteur', '')}
N° homologation : {row.get('no_homologation', '')}""".strip()
    else:
        return f"""Changement de DAR homologué ONSSA.
Produit : {row.get('produit', '')}
Composition : {row.get('composition', '')}
Usage : {row.get('usage', '')}
Nouveau DAR : {row.get('nouveau_dar', '')} jours
Ancien DAR : {row.get('ancien_dar', '')} jours
Date effet : {row.get('date_effet', '')}
Détenteur : {row.get('detenteur', '')}
N° homologation : {row.get('no_homologation', '')}""".strip()

def generer_embedding(texte):
    """Embedding local avec Ollama — gratuit."""
    import requests
    response = requests.post(
        "http://localhost:11434/api/embeddings",
        json={
            "model" : "nomic-embed-text",
            "prompt": texte
        }
    )
    return response.json()["embedding"]

def stocker_chunk(cur, source, culture, texte, vecteur, metadata):
    """Insère un chunk dans rag_chunks."""
    import json
    cur.execute("""
        INSERT INTO rag_chunks
            (source, culture, contenu_texte, embedding, metadata)
        VALUES (%s, %s, %s, %s::vector, %s)
    """, (
        source,
        culture,
        texte,
        str(vecteur),
        json.dumps(metadata)
    ))

def generer_embeddings_produits():
    """
    Génère les embeddings pour tous les produits homologués.
    """
    print("\n" + "="*50)
    print("EMBEDDINGS : produits_homologues")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        # Récupérer tous les produits
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM produits_homologues")
            produits = cur.fetchall()

        print(f"  {len(produits)} produits à vectoriser...")

        with conn:
            with conn.cursor() as cur:
                for produit in produits:
                    # Construire le texte
                    texte = construire_chunk_produit(produit)

                    # Générer le vecteur
                    vecteur = generer_embedding(texte)

                    # Stocker dans rag_chunks
                    stocker_chunk(
                        cur,
                        source   = "ONSSA",
                        culture  = produit.get("culture"),
                        texte    = texte,
                        vecteur  = vecteur,
                        metadata = {
                            "type"              : "produit_homologue",
                            "nom_commercial"    : produit.get("nom_commercial"),
                            "categorie"         : produit.get("categorie"),
                            "numero_homologation": produit.get("numero_homologation"),
                        }
                    )
                    nb += 1

                    if nb % 100 == 0:
                        print(f"  {nb}/{len(produits)} embeddings générés...")

        print(f"  Embeddings produits terminés : {nb} chunks ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR embeddings produits : {e}")
        raise
    finally:
        conn.close()

def generer_embeddings_changements():
    """
    Génère les embeddings pour les changements dose/DAR.
    """
    print(f"\n{'='*50}")
    print("EMBEDDINGS : changements_dose_dar")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM changements_dose_dar")
            changements = cur.fetchall()

        print(f"  {len(changements)} changements à vectoriser...")

        with conn:
            with conn.cursor() as cur:
                for ch in changements:
                    texte = construire_chunk_changement(ch)
                    vecteur = generer_embedding(texte)

                    stocker_chunk(
                        cur,
                        source   = "ONSSA_MODIF",
                        culture  = None,
                        texte    = texte,
                        vecteur  = vecteur,
                        metadata = {
                            "type"           : "changement",
                            "type_changement": ch.get("type_changement"),
                            "produit"        : ch.get("produit"),
                        }
                    )
                    nb += 1

        print(f"  Embeddings changements terminés : {nb} chunks ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR embeddings changements : {e}")
        raise
    finally:
        conn.close()

def generer_tous_embeddings():
    """
    Génère tous les embeddings et remplit rag_chunks.
    """
    print("="*50)
    print("ÉTAPE 5 — GÉNÉRATION EMBEDDINGS")
    print("="*50)

    # Vider rag_chunks avant de regénérer
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE rag_chunks")
    conn.close()
    print("  Table rag_chunks vidée ✓")

    nb_produits    = generer_embeddings_produits()
    nb_changements = generer_embeddings_changements()
    total          = nb_produits + nb_changements

    print("\n" + "="*50)
    print("RÉSUMÉ EMBEDDINGS")
    print("="*50)
    print(f"  ✓ Produits    → {nb_produits} chunks")
    print(f"  ✓ Changements → {nb_changements} chunks")
    print(f"  ✓ Total       → {total} chunks dans rag_chunks")

    return total

if __name__ == "__main__":
    generer_tous_embeddings()