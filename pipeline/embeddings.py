"""
Étape 5 — Génération des embeddings
Convertit chaque produit/changement en vecteur numérique
et le stocke dans rag_chunks.
Mode delta intelligent :
  - Nouveaux produits     → embedding généré
  - Produits modifiés     → embedding régénéré
  - Changements nouveaux  → embedding généré
  - Changements modifiés  → embedding régénéré
  - Inchangés             → rien
"""
import sys
import os
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from dotenv import load_dotenv
from db.connection import get_connection

load_dotenv()

def construire_chunk_produit(row):
    """Construit le texte descriptif d'un produit."""
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
    """Construit le texte descriptif d'un changement dose/DAR."""
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

def stocker_chunk(cur, source, culture, texte,
                  vecteur, metadata):
    """Insère un chunk dans rag_chunks."""
    import json
    cur.execute("""
        INSERT INTO rag_chunks
            (source, culture, contenu_texte,
             embedding, metadata)
        VALUES (%s, %s, %s, %s::vector, %s)
    """, (
        source,
        culture,
        texte,
        str(vecteur),
        json.dumps(metadata)
    ))

def generer_embeddings_produits():
    print("\n" + "="*50)
    print("EMBEDDINGS : produits_homologues")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn.cursor() as cur:
            # Utiliser DISTINCT ON pour éviter les doublons
            cur.execute("""
                SELECT p.*
                FROM produits_homologues p
                WHERE NOT EXISTS (
                    SELECT 1 FROM rag_chunks r
                    WHERE r.metadata->>'nom_commercial'
                          = p.nom_commercial
                    AND r.source = 'ONSSA'
                    AND r.created_at >= p.updated_at
                )
            """)
            produits = cur.fetchall()

        if not produits:
            print("  Aucun produit nouveau ou modifié ✓")
            return 0

        print(f"  {len(produits)} produits à vectoriser...")

        with conn:
            with conn.cursor() as cur:
                for produit in produits:
                    texte   = construire_chunk_produit(produit)
                    vecteur = generer_embedding(texte)

                    # Supprimer l'ancien chunk si existe
                    cur.execute("""
                        DELETE FROM rag_chunks
                        WHERE source = 'ONSSA'
                        AND metadata->>'nom_commercial' = %s
                    """, (produit.get("nom_commercial"),))

                    # Insérer le nouveau chunk
                    stocker_chunk(
                        cur,
                        source   = "ONSSA",
                        culture  = produit.get("culture"),
                        texte    = texte,
                        vecteur  = vecteur,
                        metadata = {
                            "type": "produit_homologue",
                            "nom_commercial": produit.get(
                                "nom_commercial"
                            ),
                            "categorie": produit.get(
                                "categorie"
                            ),
                            "numero_homologation": produit.get(
                                "numero_homologation"
                            ),
                        }
                    )
                    nb += 1

                    if nb % 100 == 0:
                        print(
                            f"  {nb}/{len(produits)} "
                            f"embeddings générés..."
                        )

        print(f"  Embeddings produits : {nb} chunks ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR : {e}")
        raise
    finally:
        conn.close()

def generer_embeddings_changements_dose():
    """
    Génère les embeddings pour les changements de DOSE :
    - Nouveaux changements (jamais dans rag_chunks)
    - Changements modifiés (updated_at > chunk created_at)
    """
    print(f"\n{'='*50}")
    print("EMBEDDINGS : changements dose")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.*
                FROM changements_dose_dar c
                LEFT JOIN rag_chunks r
                    ON r.metadata->>'produit' = c.produit
                    AND r.source = 'ONSSA_DOSE'
                    AND r.metadata->>'no_homologation'
                        = c.no_homologation
                WHERE c.type_changement = 'dose'
                AND (
                    r.id IS NULL
                    OR r.created_at < c.created_at
                )
            """)
            changements = cur.fetchall()

        if not changements:
            print("  Aucun changement dose nouveau ou modifié ✓")
            return 0

        print(f"  {len(changements)} changements dose "
              f"à vectoriser...")

        with conn:
            with conn.cursor() as cur:
                for ch in changements:
                    texte   = construire_chunk_changement(ch)
                    vecteur = generer_embedding(texte)

                    # Supprimer l'ancien chunk si existe
                    cur.execute("""
                        DELETE FROM rag_chunks
                        WHERE source = 'ONSSA_DOSE'
                        AND metadata->>'produit' = %s
                        AND metadata->>'no_homologation' = %s
                    """, (
                        ch.get("produit"),
                        ch.get("no_homologation")
                    ))

                    # Insérer le nouveau chunk
                    stocker_chunk(
                        cur,
                        source   = "ONSSA_DOSE",
                        culture  = None,
                        texte    = texte,
                        vecteur  = vecteur,
                        metadata = {
                            "type"           : "changement_dose",
                            "type_changement": "dose",
                            "produit"        : ch.get("produit"),
                            "no_homologation": ch.get(
                                "no_homologation"
                            ),
                        }
                    )
                    nb += 1

        print(f"  Embeddings dose : {nb} chunks ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR embeddings dose : {e}")
        raise
    finally:
        conn.close()

def generer_embeddings_changements_dar():
    """
    Génère les embeddings pour les changements de DAR :
    - Nouveaux changements (jamais dans rag_chunks)
    - Changements modifiés (updated_at > chunk created_at)
    """
    print(f"\n{'='*50}")
    print("EMBEDDINGS : changements DAR")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.*
                FROM changements_dose_dar c
                LEFT JOIN rag_chunks r
                    ON r.metadata->>'produit' = c.produit
                    AND r.source = 'ONSSA_DAR'
                    AND r.metadata->>'no_homologation'
                        = c.no_homologation
                WHERE c.type_changement = 'dar'
                AND (
                    r.id IS NULL
                    OR r.created_at < c.created_at
                )
            """)
            changements = cur.fetchall()

        if not changements:
            print("  Aucun changement DAR nouveau ou modifié ✓")
            return 0

        print(f"  {len(changements)} changements DAR "
              f"à vectoriser...")

        with conn:
            with conn.cursor() as cur:
                for ch in changements:
                    texte   = construire_chunk_changement(ch)
                    vecteur = generer_embedding(texte)

                    # Supprimer l'ancien chunk si existe
                    cur.execute("""
                        DELETE FROM rag_chunks
                        WHERE source = 'ONSSA_DAR'
                        AND metadata->>'produit' = %s
                        AND metadata->>'no_homologation' = %s
                    """, (
                        ch.get("produit"),
                        ch.get("no_homologation")
                    ))

                    # Insérer le nouveau chunk
                    stocker_chunk(
                        cur,
                        source   = "ONSSA_DAR",
                        culture  = None,
                        texte    = texte,
                        vecteur  = vecteur,
                        metadata = {
                            "type"           : "changement_dar",
                            "type_changement": "dar",
                            "produit"        : ch.get("produit"),
                            "no_homologation": ch.get(
                                "no_homologation"
                            ),
                        }
                    )
                    nb += 1

        print(f"  Embeddings DAR : {nb} chunks ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR embeddings DAR : {e}")
        raise
    finally:
        conn.close()

def generer_tous_embeddings():
    """
    Génère uniquement les embeddings manquants ou obsolètes.
    Couvre les 3 sources :
      1. Index Phytosanitaire (produits_homologues)
      2. Changements de dose  (changements_dose_dar)
      3. Changements de DAR   (changements_dose_dar)
    """
    print("="*50)
    print("ÉTAPE 5 — GÉNÉRATION EMBEDDINGS (delta)")
    print("="*50)

    nb_produits = generer_embeddings_produits()
    nb_dose     = generer_embeddings_changements_dose()
    nb_dar      = generer_embeddings_changements_dar()
    total       = nb_produits + nb_dose + nb_dar

    print("\n" + "="*50)
    print("RÉSUMÉ EMBEDDINGS")
    print("="*50)
    print(f"  ✓ Produits homologués → {nb_produits} chunks")
    print(f"  ✓ Changements dose    → {nb_dose} chunks")
    print(f"  ✓ Changements DAR     → {nb_dar} chunks")
    print(f"  ✓ Total               → {total} nouveaux chunks")

    return total

if __name__ == "__main__":
    generer_tous_embeddings()