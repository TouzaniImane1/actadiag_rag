"""
Pipeline complet ONSSA — RAG actaDiag
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

import time
import hashlib
from datetime import datetime
from db.connection import get_connection

def log_pipeline(statut, message, nb_lignes=0):
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sync_log
                        (source, statut,
                         nb_lignes_importees,
                         message_erreur)
                    VALUES (%s, %s, %s, %s)
                """, (
                    "PIPELINE_COMPLET",
                    statut, nb_lignes, message
                ))
    finally:
        conn.close()

def mettre_a_jour_hash(source, nouveau_hash):
    """Met à jour le hash du contenu dans sync_log."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE sync_log
                    SET hash_fichier = %s
                    WHERE source = %s
                    AND statut = 'success'
                    AND date_sync = (
                        SELECT MAX(date_sync)
                        FROM sync_log
                        WHERE source = %s
                        AND statut = 'success'
                    )
                """, (nouveau_hash, source, source))
        print(f"  Hash sync_log mis à jour ✓")
    finally:
        conn.close()

def hash_dataframe(df):
    """
    Hash uniquement les numéros d'homologation triés.
    C'est la donnée la plus stable du site ONSSA.
    Change UNIQUEMENT si de nouveaux produits apparaissent.
    """
    if "numero_homologation" not in df.columns:
        # Fallback sur toutes les colonnes clés
        colonnes = ["nom_commercial", "culture", "usage"]
        df_trie = df[colonnes].fillna("").sort_values(
            by=colonnes
        ).reset_index(drop=True)
        contenu = df_trie.to_csv(index=False).encode()
        return hashlib.sha256(contenu).hexdigest()

    # Hash stable basé sur les numéros d'homologation
    numeros = sorted(
        df["numero_homologation"].fillna("").tolist()
    )
    contenu = "|".join(numeros).encode()
    return hashlib.sha256(contenu).hexdigest()

def verifier_hash(source, nouveau_hash):
    """Retourne True si changement détecté."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT hash_fichier
                FROM sync_log
                WHERE source = %s
                AND statut = 'success'
                ORDER BY date_sync DESC
                LIMIT 1
            """, (source,))
            result = cur.fetchone()

        if not result:
            return True
        return result.get("hash_fichier") != nouveau_hash

    finally:
        conn.close()

def run_pipeline(force=False):
    debut = time.time()
    print("\n" + "="*60)
    print("PIPELINE COMPLET ONSSA — actaDiag RAG")
    print(
        f"Heure : "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print("="*60)

    try:
        # =============================================
        # ÉTAPE 1 — Téléchargement ONSSA
        # =============================================
        print("\n[1/4] TÉLÉCHARGEMENT ONSSA...")
        from pipeline.onssa_downloader import telecharger_tout
        resultats_telechargement = telecharger_tout()

        nb_succes = sum(
            1 for r in resultats_telechargement.values()
            if r.get("statut") == "success"
        )
        if nb_succes == 0:
            raise Exception(
                "Aucun fichier téléchargé avec succès !"
            )
        print(f"  {nb_succes}/3 fichiers téléchargés ✓")

        # =============================================
        # ÉTAPE 2 — Parsing & Nettoyage
        # =============================================
        print("\n[2/4] PARSING & NETTOYAGE...")
        from pipeline.onssa_parser import parser_tous
        resultats_parsing = parser_tous()

        nb_produits = len(
            resultats_parsing.get("index_phyto", [])
        )
        nb_dose = len(
            resultats_parsing.get("changement_dose", [])
        )
        nb_dar = len(
            resultats_parsing.get("changement_dar", [])
        )
        print(f"  Produits    : {nb_produits} ✓")
        print(f"  Dose        : {nb_dose} ✓")
        print(f"  DAR         : {nb_dar} ✓")

        if nb_produits == 0:
            raise Exception(
                "Parsing échoué — 0 produits trouvés !"
            )

        # Hash stable du contenu trié
        hash_contenu = hash_dataframe(
            resultats_parsing.get("index_phyto")
        )
        print(f"  Hash contenu : {hash_contenu[:16]}...")

        # Vérifier si les données ont changé
        if not force and not verifier_hash(
            "ONSSA_INDEX", hash_contenu
        ):
            print(
                "\n  Aucun changement détecté "
                "dans les données ONSSA."
            )
            print("  Pipeline arrêté — base déjà à jour ✓")
            duree = time.time() - debut
            print(f"  Durée : {duree:.1f} secondes")
            log_pipeline(
                "success",
                "Aucun changement — base déjà à jour",
                0
            )
            return True

        print("  Changement détecté → mise à jour...")

        # =============================================
        # ÉTAPE 3 — Upsert en base
        # =============================================
        print("\n[3/4] UPSERT EN BASE...")
        from pipeline.onssa_upsert import upsert_tout
        bilan_upsert = upsert_tout(
            resultats_parsing,
            resultats_telechargement
        )
        total_upsert = sum(bilan_upsert.values())
        print(f"  Total inséré : {total_upsert} lignes ✓")

        # Enregistrer le hash stable dans sync_log
        mettre_a_jour_hash("ONSSA_INDEX", hash_contenu)

        # =============================================
        # ÉTAPE 4 — Génération Embeddings (delta)
        # =============================================
        print("\n[4/4] GÉNÉRATION EMBEDDINGS (delta)...")
        from pipeline.embeddings import generer_tous_embeddings
        total_chunks = generer_tous_embeddings()
        print(f"  Nouveaux chunks : {total_chunks} ✓")

        # =============================================
        # RÉSUMÉ FINAL
        # =============================================
        duree = time.time() - debut
        print("\n" + "="*60)
        print("PIPELINE TERMINÉ AVEC SUCCÈS ✓")
        print("="*60)
        print(f"  Produits homologués : {nb_produits}")
        print(f"  Changements dose    : {nb_dose}")
        print(f"  Changements DAR     : {nb_dar}")
        print(f"  Chunks RAG          : {total_chunks}")
        print(
            f"  Durée totale        : "
            f"{duree/60:.1f} minutes"
        )
        print(
            f"  Heure fin           : "
            f"{datetime.now().strftime('%H:%M:%S')}"
        )

        log_pipeline(
            "success",
            f"Pipeline OK : {nb_produits} produits, "
            f"{total_chunks} nouveaux chunks",
            total_upsert
        )
        return True

    except Exception as e:
        duree = time.time() - debut
        print(f"\nERREUR PIPELINE : {e}")
        import traceback
        traceback.print_exc()
        log_pipeline("error", str(e), 0)
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Pipeline ONSSA — RAG actaDiag"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force le retraitement même sans changement"
    )
    args = parser.parse_args()
    succes = run_pipeline(force=args.force)
    sys.exit(0 if succes else 1)