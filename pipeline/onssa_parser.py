"""
Étape 3 — Parser & Nettoyage des fichiers XLS ONSSA
Lit les fichiers HTML déguisés en XLS et retourne
des DataFrames pandas propres prêts pour l'upsert.
"""
import re
import os
import pandas as pd

DOSSIER = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)

# Mapping colonnes Excel → colonnes BDD (produits_homologues)
MAPPING_INDEX = {
    "Produits (4687)" : "nom_commercial",
    "Produits (4686)" : "nom_commercial",
    "Produits (4685)" : "nom_commercial",
    "Détenteur"       : "detenteur",
    "Fournisseur"     : "fournisseur",
    "Numéro homologation" : "numero_homologation",
    "Valable jusqu'au"    : "valable_jusqu_au",
    "Tableau toxicologique": "tableau_toxicologique",
    "Catégorie"       : "categorie",
    "Formulation"     : "formulation",
    "Matière active"  : "matiere_active",
    "Teneur"          : "teneur",
    "Usage"           : "usage",
    "Dose"            : "dose",
    "Culture"         : "culture",
    "DAR"             : "dar",
    "Nbr d'application" : "nb_applications",
}

# Mapping colonnes Excel → colonnes BDD (changements_dose_dar)
MAPPING_DOSE = {
    "Produit"        : "produit",
    "Composition"    : "composition",
    "NoHomolog"      : "no_homologation",
    "Date Effet"     : "date_effet",
    "Détenteur"      : "detenteur",
    "Usage"          : "usage",
    "Dose"           : "nouvelle_dose",
    "Ancienne Dose"  : "ancienne_dose",
}

MAPPING_DAR = {
    "Produit"        : "produit",
    "Composition"    : "composition",
    "NoHomolog"      : "no_homologation",
    "Date Effet"     : "date_effet",
    "Détenteur"      : "detenteur",
    "Usage"          : "usage",
    "DAR"            : "nouveau_dar",
    "Ancien DAR"     : "ancien_dar",
}

def lire_xls_onssa(chemin):
    """
    Lit un fichier XLS HTML ONSSA.
    Gère les deux formats de headers.
    """
    import html as html_module

    with open(chemin, 'r', encoding='utf-8', errors='ignore') as f:
        contenu = f.read()

    # Décoder TOUTES les entités HTML (&#233; → é, &nbsp; → espace)
    contenu = html_module.unescape(contenu)

    # Chercher les headers dans <th>
    headers = re.findall(
        r'<th[^>]*>(.*?)</th>', contenu, re.DOTALL
    )
    headers = [
        re.sub(r'<[^>]+>', '', h).strip()
        for h in headers
    ]

    # Extraire toutes les lignes <tr>
    rows = re.findall(
        r'<tr[^>]*>(.*?)</tr>', contenu, re.DOTALL
    )

    # Si pas de <th> → première ligne <tr> contient les headers
    if not headers and rows:
        premiere = rows[0]
        headers = re.findall(
            r'<td[^>]*>(.*?)</td>', premiere, re.DOTALL
        )
        headers = [
            re.sub(r'<[^>]+>', '', h).strip()
            for h in headers
        ]
        rows = rows[1:]  # Le reste = données

    # Extraire les données
    donnees = []
    for row in rows:
        cells = re.findall(
            r'<td[^>]*>(.*?)</td>', row, re.DOTALL
        )
        cells_clean = [
            re.sub(r'<[^>]+>', '', c).strip()
            for c in cells
        ]
        if any(c for c in cells_clean if c):
            donnees.append(cells_clean)

    return headers, donnees

def nettoyer_df(df):
    """Nettoie un DataFrame."""
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            # Remplacer les valeurs parasites par None
            df[col] = df[col].replace(
                ['', '-', 'None', 'nan', '\xa0', '&nbsp;'],
                None
            )
    return df

def parser_index_phyto():
    """
    Parse le fichier Index Phytosanitaire.
    Retourne un DataFrame avec les colonnes BDD.
    """
    chemin = os.path.join(DOSSIER, "onssa_index_phyto.xls")
    print(f"\nParsing : {os.path.basename(chemin)}")

    headers, donnees = lire_xls_onssa(chemin)
    print(f"  Colonnes brutes : {headers}")
    print(f"  Lignes brutes   : {len(donnees)}")

    if not headers or not donnees:
        print("  ERREUR : fichier vide !")
        return None

    df = pd.DataFrame(donnees, columns=headers)

    # Renommer selon mapping — cherche la colonne Produits
    # (le nombre change selon la MAJ du site)
    mapping_actuel = {}
    for col_excel, col_bdd in MAPPING_INDEX.items():
        if col_excel in df.columns:
            mapping_actuel[col_excel] = col_bdd

    # Gérer la colonne "Produits (XXXX)" dynamiquement
    for col in df.columns:
        if col.startswith("Produits ("):
            mapping_actuel[col] = "nom_commercial"

    df = df.rename(columns=mapping_actuel)
    df = nettoyer_df(df)

    # Supprimer les lignes sans nom commercial
    df = df.dropna(subset=["nom_commercial"])
    df = df.drop_duplicates(
        subset=["nom_commercial", "culture", "usage"],
        keep="last"
    )

    print(f"  Lignes après nettoyage : {len(df)}")
    print(f"  Colonnes BDD : {list(df.columns)}")
    print(f"\n  Aperçu 3 premières lignes :")
    print(df.head(3).to_string())

    return df

def parser_changement_dose():
    """
    Parse le fichier Changement de dose.
    Retourne un DataFrame avec les colonnes BDD.
    """
    chemin = os.path.join(DOSSIER, "onssa_changement_dose.xls")
    print(f"\nParsing : {os.path.basename(chemin)}")

    headers, donnees = lire_xls_onssa(chemin)
    print(f"  Colonnes brutes : {headers}")
    print(f"  Lignes brutes   : {len(donnees)}")

    if not headers or not donnees:
        print("  ERREUR : fichier vide !")
        return None

    df = pd.DataFrame(donnees, columns=headers)

    # Renommer selon mapping
    mapping_actuel = {
        k: v for k, v in MAPPING_DOSE.items()
        if k in df.columns
    }
    df = df.rename(columns=mapping_actuel)
    df = nettoyer_df(df)
    df["type_changement"] = "dose"
    df = df.dropna(subset=["produit"])

    print(f"  Lignes après nettoyage : {len(df)}")
    print(f"  Colonnes BDD : {list(df.columns)}")
    print(f"\n  Aperçu 3 premières lignes :")
    print(df.head(3).to_string())

    return df

def parser_changement_dar():
    """
    Parse le fichier Changement de DAR.
    Retourne un DataFrame avec les colonnes BDD.
    """
    chemin = os.path.join(DOSSIER, "onssa_changement_dar.xls")
    print(f"\nParsing : {os.path.basename(chemin)}")

    headers, donnees = lire_xls_onssa(chemin)
    print(f"  Colonnes brutes : {headers}")
    print(f"  Lignes brutes   : {len(donnees)}")

    if not headers or not donnees:
        print("  ERREUR : fichier vide !")
        return None

    df = pd.DataFrame(donnees, columns=headers)

    # Renommer selon mapping
    mapping_actuel = {
        k: v for k, v in MAPPING_DAR.items()
        if k in df.columns
    }
    df = df.rename(columns=mapping_actuel)
    df = nettoyer_df(df)
    df["type_changement"] = "dar"
    df = df.dropna(subset=["produit"])

    print(f"  Lignes après nettoyage : {len(df)}")
    print(f"  Colonnes BDD : {list(df.columns)}")
    print(f"\n  Aperçu 3 premières lignes :")
    print(df.head(3).to_string())

    return df

def parser_tous():
    """
    Parse les 3 fichiers ONSSA.
    Retourne un dictionnaire de DataFrames.
    """
    print("="*50)
    print("ÉTAPE 3 — PARSING & NETTOYAGE")
    print("="*50)

    resultats = {}

    df_index = parser_index_phyto()
    if df_index is not None:
        resultats["index_phyto"] = df_index
        print(f"\n  index_phyto : {len(df_index)} produits ✓")

    df_dose = parser_changement_dose()
    if df_dose is not None:
        resultats["changement_dose"] = df_dose
        print(f"\n  changement_dose : {len(df_dose)} entrées ✓")

    df_dar = parser_changement_dar()
    if df_dar is not None:
        resultats["changement_dar"] = df_dar
        print(f"\n  changement_dar : {len(df_dar)} entrées ✓")

    print("\n" + "="*50)
    print("RÉSUMÉ PARSING")
    print("="*50)
    for nom, df in resultats.items():
        print(f"  ✓ {nom:25} → {len(df)} lignes")

    return resultats

if __name__ == "__main__":
    parser_tous()