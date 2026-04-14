import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_connection
def upsert_produits(df):
    conn = get_connection()
    nb = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO produits_homologues (
                            nom_commercial, detenteur, fournisseur,
                            numero_homologation, valable_jusqu_au,
                            tableau_toxicologique, categorie,
                            formulation, matiere_active, teneur,
                            usage, dose, culture, dar,
                            nb_applications, updated_at
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,NOW()
                        )
                        ON CONFLICT (nom_commercial, culture, usage)
                        DO UPDATE SET
                            detenteur             = EXCLUDED.detenteur,
                            fournisseur           = EXCLUDED.fournisseur,
                            matiere_active        = EXCLUDED.matiere_active,
                            dose                  = EXCLUDED.dose,
                            dar                   = EXCLUDED.dar,
                            valable_jusqu_au      = EXCLUDED.valable_jusqu_au,
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
        print(f"Upsert OK : {nb} produits")
        return nb
    finally:
        conn.close()