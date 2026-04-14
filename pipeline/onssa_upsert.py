"""
Étape 4 — Insertion en base PostgreSQL (Upsert)
Insère ou met à jour les données dans les tables :
  - produits_homologues
  - changements_dose_dar
Garantit l'idempotence (0 doublon) via ON CONFLICT DO UPDATE
"""
import sys
import os
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from db.connection import get_connection

def upsert_produits_homologues(df):
    """
    Insère ou met à jour les produits dans produits_homologues.
    Retourne le nombre de lignes traitées.
    """
    print("\n" + "="*50)
    print("UPSERT : produits_homologues")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO produits_homologues (
                            nom_commercial,
                            detenteur,
                            fournisseur,
                            numero_homologation,
                            valable_jusqu_au,
                            tableau_toxicologique,
                            categorie,
                            formulation,
                            matiere_active,
                            teneur,
                            usage,
                            dose,
                            culture,
                            dar,
                            nb_applications,
                            updated_at
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,NOW()
                        )
                        ON CONFLICT (nom_commercial, culture, usage)
                        DO UPDATE SET
                            detenteur             = EXCLUDED.detenteur,
                            fournisseur           = EXCLUDED.fournisseur,
                            numero_homologation   = EXCLUDED.numero_homologation,
                            valable_jusqu_au      = EXCLUDED.valable_jusqu_au,
                            tableau_toxicologique = EXCLUDED.tableau_toxicologique,
                            categorie             = EXCLUDED.categorie,
                            formulation           = EXCLUDED.formulation,
                            matiere_active        = EXCLUDED.matiere_active,
                            teneur                = EXCLUDED.teneur,
                            dose                  = EXCLUDED.dose,
                            dar                   = EXCLUDED.dar,
                            nb_applications       = EXCLUDED.nb_applications,
                            updated_at            = NOW()
                    """, (
                        row.get("nom_commercial"),
                        row.get("detenteur"),
                        row.get("fournisseur"),
                        row.get("numero_homologation"),
                        row.get("valable_jusqu_au"),
                        row.get("tableau_toxicologique"),
                        row.get("categorie"),
                        row.get("formulation"),
                        row.get("matiere_active"),
                        row.get("teneur"),
                        row.get("usage"),
                        row.get("dose"),
                        row.get("culture"),
                        row.get("dar"),
                        row.get("nb_applications"),
                    ))
                    nb += 1

                    # Afficher la progression tous les 500 produits
                    if nb % 500 == 0:
                        print(f"  {nb} produits insérés...")

        print(f"  Upsert terminé : {nb} produits ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR upsert produits : {e}")
        raise
    finally:
        conn.close()

def upsert_changements_dose_dar(df, type_changement):
    """
    Insère ou met à jour les changements de dose ou DAR.
    Retourne le nombre de lignes traitées.
    """
    print(f"\n{'='*50}")
    print(f"UPSERT : changements_dose_dar ({type_changement})")
    print("="*50)

    conn = get_connection()
    nb   = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO changements_dose_dar (
                            type_changement,
                            produit,
                            composition,
                            no_homologation,
                            date_effet,
                            detenteur,
                            usage,
                            nouvelle_dose,
                            ancienne_dose,
                            nouveau_dar,
                            ancien_dar
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT
                        (produit, no_homologation,
                         date_effet, type_changement)
                        DO UPDATE SET
                            composition   = EXCLUDED.composition,
                            detenteur     = EXCLUDED.detenteur,
                            usage         = EXCLUDED.usage,
                            nouvelle_dose = EXCLUDED.nouvelle_dose,
                            ancienne_dose = EXCLUDED.ancienne_dose,
                            nouveau_dar   = EXCLUDED.nouveau_dar,
                            ancien_dar    = EXCLUDED.ancien_dar
                    """, (
                        type_changement,
                        row.get("produit"),
                        row.get("composition"),
                        row.get("no_homologation"),
                        row.get("date_effet"),
                        row.get("detenteur"),
                        row.get("usage"),
                        row.get("nouvelle_dose"),
                        row.get("ancienne_dose"),
                        row.get("nouveau_dar"),
                        row.get("ancien_dar"),
                    ))
                    nb += 1

        print(f"  Upsert terminé : {nb} entrées {type_changement} ✓")
        return nb

    except Exception as e:
        print(f"  ERREUR upsert {type_changement} : {e}")
        raise
    finally:
        conn.close()

def log_sync(source, hash_fichier, nb_lignes,
             statut, message_erreur=None):
    """Enregistre une synchronisation dans sync_log."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sync_log
                        (source, hash_fichier, statut,
                         nb_lignes_importees, message_erreur)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    source, hash_fichier, statut,
                    nb_lignes, message_erreur
                ))
        print(f"  Log sync : {source} — {statut} — {nb_lignes} lignes")
    finally:
        conn.close()

def upsert_tout(resultats_parsing, resultats_telechargement):
    """
    Insère tous les DataFrames dans PostgreSQL.
    resultats_parsing      : dict retourné par parser_tous()
    resultats_telechargement : dict retourné par telecharger_tout()
    """
    print("\n" + "="*50)
    print("ÉTAPE 4 — UPSERT EN BASE")
    print("="*50)

    bilan = {}

    # 1. Produits homologués
    if "index_phyto" in resultats_parsing:
        df = resultats_parsing["index_phyto"]
        nb = upsert_produits_homologues(df)
        hash_f = resultats_telechargement.get(
            "index_phyto", {}
        ).get("hash")
        log_sync("ONSSA_INDEX", hash_f, nb, "success")
        bilan["index_phyto"] = nb

    # 2. Changements de dose
    if "changement_dose" in resultats_parsing:
        df = resultats_parsing["changement_dose"]
        nb = upsert_changements_dose_dar(df, "dose")
        hash_f = resultats_telechargement.get(
            "changement_dose", {}
        ).get("hash")
        log_sync("ONSSA_DOSE", hash_f, nb, "success")
        bilan["changement_dose"] = nb

    # 3. Changements de DAR
    if "changement_dar" in resultats_parsing:
        df = resultats_parsing["changement_dar"]
        nb = upsert_changements_dose_dar(df, "dar")
        hash_f = resultats_telechargement.get(
            "changement_dar", {}
        ).get("hash")
        log_sync("ONSSA_DAR", hash_f, nb, "success")
        bilan["changement_dar"] = nb

    print("\n" + "="*50)
    print("RÉSUMÉ UPSERT")
    print("="*50)
    for nom, nb in bilan.items():
        print(f"  ✓ {nom:25} → {nb} lignes insérées")

    return bilan

if __name__ == "__main__":
    # Test direct — parse puis upsert
    import sys
    sys.path.append(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    from pipeline.onssa_parser import parser_tous

    print("Test Étape 4 — Upsert")
    resultats_parsing = parser_tous()
    resultats_telechargement = {
        "index_phyto"    : {"hash": "test"},
        "changement_dose": {"hash": "test"},
        "changement_dar" : {"hash": "test"},
    }
    upsert_tout(resultats_parsing, resultats_telechargement)