"""
Script principal du pipeline ONSSA.
Lance toutes les étapes dans l'ordre.
C'est ce script que le CRON exécute chaque semaine.

Cron : 0 8 * * 1 python /home/actadiag/actadiag_rag/pipeline/run_pipeline.py
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.onssa_downloader import (
    telecharger_excel_indpesticide,
    telecharger_toutes_modifications,
    sauvegarder_fichier,
)
from pipeline.onssa_parser import parser_excel
from pipeline.onssa_upsert import upsert_produits
from pipeline.embeddings import generer_tous_les_embeddings
from pipeline.sync_log import log_sync

def run():
    print("=" * 60)
    print("PIPELINE ONSSA — Démarrage")
    print("=" * 60)

    hash_fichier = None
    nb_lignes = 0

    try:
        # ================================================
        # PARTIE 1 — Index Phytosanitaire complet
        # ================================================
        print("\n--- PARTIE 1 : Index Phytosanitaire ---")
        contenu, hash_fichier = telecharger_excel_indpesticide()

        if contenu is not None:
            chemin = sauvegarder_fichier(
                contenu, "data/onssa_complet.xlsx"
            )
            df = parser_excel(chemin)
            nb_lignes = upsert_produits(df)
            log_sync(
                "ONSSA_INDEX", hash_fichier, nb_lignes, "success"
            )
            print(f"Index Phytosanitaire : {nb_lignes} produits traités.")
        else:
            log_sync("ONSSA_INDEX", None, 0, "no_change")
            print("Index Phytosanitaire : pas de changement.")

        # ================================================
        # PARTIE 2 — Modifications et retraits
        # Sections dans l'ordre de priorité :
        # 9=Retrait, 6=Dose, 7=DAR, 3=Extension usage...
        # ================================================
        print("\n--- PARTIE 2 : Modifications et retraits ---")
        resultats = telecharger_toutes_modifications()

        for nom_section, resultat in resultats.items():
            if resultat["statut"] == "success":
                log_sync(
                    f"ONSSA_{nom_section.upper()}",
                    resultat["hash"],
                    resultat["taille"],
                    "success"
                )
            else:
                log_sync(
                    f"ONSSA_{nom_section.upper()}",
                    None, 0, "error",
                    resultat.get("erreur")
                )

        nb_sections_ok = sum(
            1 for r in resultats.values()
            if r["statut"] == "success"
        )

        # ================================================
        # PARTIE 3 — Génération embeddings
        # ================================================
        print("\n--- PARTIE 3 : Génération embeddings ---")
        generer_tous_les_embeddings()

        print("\n" + "=" * 60)
        print("PIPELINE TERMINÉ AVEC SUCCÈS")
        print(f"  Index phytosanitaire : {nb_lignes} produits")
        print(f"  Sections modif OK    : {nb_sections_ok}/10")
        print("=" * 60)

    except Exception as e:
        print(f"\nERREUR PIPELINE : {e}")
        log_sync("ONSSA", hash_fichier, nb_lignes, "error", str(e))
        raise

if __name__ == "__main__":
    run()
