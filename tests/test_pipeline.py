"""
Tests du pipeline ONSSA.
Vérifie l'idempotence (0 doublon après N exécutions).
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import execute_query

def test_idempotence():
    """
    Vérifie qu'après 2 exécutions du pipeline,
    le nombre de lignes est identique (0 doublon).
    """
    # Compter avant
    avant = execute_query(
        "SELECT COUNT(*) as total FROM produits_homologues",
        fetch=True
    )[0]["total"]

    # Simuler une 2ème exécution du pipeline
    from pipeline.onssa_parser import parser_excel
    from pipeline.onssa_upsert import upsert_produits

    if os.path.exists("data/onssa_complet.xlsx"):
        df = parser_excel("data/onssa_complet.xlsx")
        upsert_produits(df)

    # Compter après
    apres = execute_query(
        "SELECT COUNT(*) as total FROM produits_homologues",
        fetch=True
    )[0]["total"]

    assert avant == apres, (
        f"ÉCHEC idempotence : {avant} lignes avant, {apres} après. "
        f"Doublons détectés : {apres - avant}"
    )
    print(f"Test idempotence OK — {avant} lignes stables.")

def test_couverture_cultures_pilotes():
    """
    Vérifie que les cultures pilotes sont couvertes à 100%.
    Critère qualité : 100% cultures pilotes dans actadiag_rag.
    """
    cultures_pilotes = ["Fraisier", "Tomate", "Agrumes"]
    cultures_manquantes = []

    for culture in cultures_pilotes:
        result = execute_query("""
            SELECT COUNT(*) as total FROM produits_homologues
            WHERE culture ILIKE %s
        """, (f"%{culture}%",), fetch=True)

        nb = result[0]["total"]
        if nb == 0:
            cultures_manquantes.append(culture)
        else:
            print(f"Culture {culture} : {nb} produits trouvés ✓")

    assert len(cultures_manquantes) == 0, (
        f"Cultures manquantes : {cultures_manquantes}"
    )
    print("Test couverture 100% OK.")

def test_sync_log_enregistre():
    """
    Vérifie que chaque pipeline est bien enregistré dans sync_log.
    """
    result = execute_query("""
        SELECT COUNT(*) as total FROM sync_log
        WHERE source = 'ONSSA'
    """, fetch=True)

    nb = result[0]["total"]
    assert nb > 0, "Aucune entrée dans sync_log pour ONSSA."
    print(f"Test sync_log OK — {nb} synchronisations enregistrées.")

if __name__ == "__main__":
    print("=== Tests Pipeline ONSSA ===")
    test_idempotence()
    test_couverture_cultures_pilotes()
    test_sync_log_enregistre()
    print("=== Tous les tests passés ✓ ===")
