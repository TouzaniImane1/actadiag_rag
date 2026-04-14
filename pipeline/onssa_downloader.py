"""
Étapes 1 & 2 du pipeline :
- Surveille l'URL ONSSA
- Télécharge le fichier Excel si changement détecté
"""
import hashlib
import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from db.connection import execute_query

load_dotenv()

ONSSA_URL = os.getenv(
    "ONSSA_URL",
    "https://eservice.onssa.gov.ma/IndPesticide.aspx"
)

URL_MODIF = "https://eservice.onssa.gov.ma/ModifHomolog.aspx"

# IDs exacts trouvés par inspection du site ONSSA
SECTIONS_MODIFICATIONS = {
    "modif_toutes"           : "ctl00$CPHCorps$lnkModifHomolog",
    "second_nom_commercial"  : "ctl00$CPHCorps$ln2Rech1",
    "transfert_homologation" : "ctl00$CPHCorps$ln2Rech2",
    "extension_usage"        : "ctl00$CPHCorps$ln2Rech3",
    "extension_usage_mineur" : "ctl00$CPHCorps$ln2Rech4",
    "changement_appellation" : "ctl00$CPHCorps$ln2Rech5",
    "changement_dose"        : "ctl00$CPHCorps$ln2Rech6",
    "changement_dar"         : "ctl00$CPHCorps$ln2Rech7",
    "changement_fournisseur" : "ctl00$CPHCorps$ln2Rech8",
    "retrait_homologation"   : "ctl00$CPHCorps$ln2Rech9",
}

# Sections critiques à traiter en priorité
SECTIONS_CRITIQUES = [
    "retrait_homologation",   # produit interdit → statut = Retiré
    "changement_dose",        # dose modifiée    → mettre à jour dose
    "changement_dar",         # DAR modifié      → mettre à jour delai_avant_recolte
    "extension_usage",        # nouvelle culture → nouveau chunk RAG
]

def get_last_hash():
    """Récupère le dernier hash connu depuis sync_log."""
    result = execute_query("""
        SELECT hash_fichier FROM sync_log
        WHERE source = 'ONSSA' AND statut = 'success'
        ORDER BY date_sync DESC
        LIMIT 1
    """, fetch=True)
    return result[0]["hash_fichier"] if result else None

def telecharger_fichier():
    """
    Télécharge le fichier Excel ONSSA via POST ASP.NET.
    Retourne (contenu_bytes, hash_sha256) ou (None, None) si pas de changement.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })

    # Étape 1 — Charger la page pour récupérer les tokens ASP.NET
    print("Chargement de la page ONSSA...")
    response = session.get(ONSSA_URL, timeout=30)
    soup = BeautifulSoup(response.text, "html.parser")

    viewstate = soup.find("input", {"id": "__VIEWSTATE"})
    eventvalidation = soup.find("input", {"id": "__EVENTVALIDATION"})

    if not viewstate or not eventvalidation:
        raise ValueError("Tokens ASP.NET introuvables sur la page ONSSA.")

    # Étape 2 — Lancer la Recherche Multicritère (Tous les produits)
    print("Lancement Recherche Multicritère...")
    data_filtre = {
        "__VIEWSTATE"      : viewstate["value"],
        "__EVENTVALIDATION": eventvalidation["value"],
        "ctl00$CPHCorps$ddlMatiereActive": "Tous",
        "ctl00$CPHCorps$ddlUsage"        : "Tous",
        "ctl00$CPHCorps$ddlCulture"      : "Tous",
        "ctl00$CPHCorps$btnFiltrer"      : "Filtrer",
    }
    response2 = session.post(ONSSA_URL, data=data_filtre, timeout=60)
    soup2 = BeautifulSoup(response2.text, "html.parser")

    viewstate2 = soup2.find("input", {"id": "__VIEWSTATE"})
    eventvalidation2 = soup2.find("input", {"id": "__EVENTVALIDATION"})

    # Étape 3 — Cliquer sur le bouton Excel
    print("Téléchargement du fichier Excel...")
    data_excel = {
        "__VIEWSTATE"         : viewstate2["value"],
        "__EVENTVALIDATION"   : eventvalidation2["value"],
        "ctl00$CPHCorps$epp.x": "10",
        "ctl00$CPHCorps$epp.y": "10",
    }
    excel_response = session.post(ONSSA_URL, data=data_excel, timeout=120)
    contenu = excel_response.content

    # Calculer le hash SHA-256
    hash_actuel = hashlib.sha256(contenu).hexdigest()

    # Comparer avec le dernier hash connu
    hash_connu = get_last_hash()
    if hash_actuel == hash_connu:
        print("Pas de changement détecté — aucune mise à jour nécessaire.")
        return None, None

    print(f"Nouveau fichier détecté ! Hash : {hash_actuel[:16]}...")
    return contenu, hash_actuel

def sauvegarder_fichier(contenu, chemin="data/onssa_complet.xlsx"):
    """Sauvegarde le contenu binaire dans un fichier Excel."""
    os.makedirs("data", exist_ok=True)
    with open(chemin, "wb") as f:
        f.write(contenu)
    print(f"Fichier sauvegardé : {chemin} ({len(contenu):,} octets)")
    return chemin

def get_last_hash():
    """Récupère le dernier hash connu depuis sync_log."""
    result = execute_query("""
        SELECT hash_fichier FROM sync_log
        WHERE source = 'ONSSA' AND statut = 'success'
        ORDER BY date_sync DESC
        LIMIT 1
    """, fetch=True)
    return result[0]["hash_fichier"] if result else None

def get_tokens(session, url):
    """Charge une page et retourne les tokens ASP.NET."""
    response = session.get(url, timeout=30)
    soup = BeautifulSoup(response.text, "html.parser")
    viewstate = soup.find("input", {"id": "__VIEWSTATE"})
    eventvalidation = soup.find(
        "input", {"id": "__EVENTVALIDATION"}
    )
    if not viewstate or not eventvalidation:
        raise ValueError(f"Tokens ASP.NET introuvables : {url}")
    return viewstate["value"], eventvalidation["value"]

def telecharger_excel_indpesticide():
    """
    Télécharge l'Index Phytosanitaire complet via Multicritère.
    Retourne (contenu_bytes, hash) ou (None, None) si pas de changement.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })

    print("Téléchargement Index Phytosanitaire ONSSA...")
    viewstate, eventvalidation = get_tokens(session, ONSSA_URL)

    # Filtrer avec Multicritère — tout sélectionner
    data_filtre = {
        "__VIEWSTATE"      : viewstate,
        "__EVENTVALIDATION": eventvalidation,
        "ctl00$CPHCorps$ddlMatiereActive": "Tous",
        "ctl00$CPHCorps$ddlUsage"        : "Tous",
        "ctl00$CPHCorps$ddlCulture"      : "Tous",
        "ctl00$CPHCorps$btnFiltrer"      : "Filtrer",
    }
    response2 = session.post(ONSSA_URL, data=data_filtre, timeout=60)
    soup2 = BeautifulSoup(response2.text, "html.parser")

    viewstate2 = soup2.find("input", {"id": "__VIEWSTATE"})["value"]
    eventvalidation2 = soup2.find(
        "input", {"id": "__EVENTVALIDATION"})["value"]

    # Cliquer sur le bouton Excel
    data_excel = {
        "__VIEWSTATE"         : viewstate2,
        "__EVENTVALIDATION"   : eventvalidation2,
        "ctl00$CPHCorps$epp.x": "10",
        "ctl00$CPHCorps$epp.y": "10",
    }
    excel_response = session.post(
        ONSSA_URL, data=data_excel, timeout=120
    )
    contenu = excel_response.content
    hash_actuel = hashlib.sha256(contenu).hexdigest()

    # Vérifier si changement
    hash_connu = get_last_hash()
    if hash_actuel == hash_connu:
        print("Index Phytosanitaire — pas de changement.")
        return None, None

    print(f"Nouveau fichier détecté ! Hash : {hash_actuel[:16]}...")
    return contenu, hash_actuel

def telecharger_section_modification(nom_section):
    """
    Télécharge une section spécifique de Modifications/Retraits.
    Utilise les IDs exacts trouvés par inspection du site.
    """
    if nom_section not in SECTIONS_MODIFICATIONS:
        raise ValueError(f"Section inconnue : {nom_section}")

    eventtarget = SECTIONS_MODIFICATIONS[nom_section]

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })

    print(f"Téléchargement section : {nom_section}...")

    # Charger la page principale
    viewstate, eventvalidation = get_tokens(session, URL_MODIF)

    # Cliquer sur le lien de la section
    data_section = {
        "__VIEWSTATE"      : viewstate,
        "__EVENTVALIDATION": eventvalidation,
        "__EVENTTARGET"    : eventtarget,
        "__EVENTARGUMENT"  : "",
    }
    response2 = session.post(URL_MODIF, data=data_section, timeout=30)
    soup2 = BeautifulSoup(response2.text, "html.parser")

    viewstate2 = soup2.find("input", {"id": "__VIEWSTATE"})["value"]
    eventvalidation2 = soup2.find(
        "input", {"id": "__EVENTVALIDATION"})["value"]

    # Cliquer sur le bouton Excel
    data_excel = {
        "__VIEWSTATE"         : viewstate2,
        "__EVENTVALIDATION"   : eventvalidation2,
        "ctl00$CPHCorps$epp.x": "10",
        "ctl00$CPHCorps$epp.y": "10",
    }
    excel_response = session.post(
        URL_MODIF, data=data_excel, timeout=60
    )
    contenu = excel_response.content
    hash_actuel = hashlib.sha256(contenu).hexdigest()

    print(f"  ✓ {nom_section} : {len(contenu):,} octets")
    return contenu, hash_actuel

def telecharger_toutes_modifications():
    """
    Télécharge toutes les sections de modifications.
    Priorité aux sections critiques.
    """
    os.makedirs("data", exist_ok=True)
    resultats = {}

    # D'abord les sections critiques
    sections_ordonnees = (
        SECTIONS_CRITIQUES +
        [s for s in SECTIONS_MODIFICATIONS
         if s not in SECTIONS_CRITIQUES]
    )

    for nom_section in sections_ordonnees:
        try:
            contenu, hash_fichier = telecharger_section_modification(
                nom_section
            )
            chemin = f"data/modif_{nom_section}.xlsx"
            with open(chemin, "wb") as f:
                f.write(contenu)

            resultats[nom_section] = {
                "chemin"     : chemin,
                "hash"       : hash_fichier,
                "taille"     : len(contenu),
                "statut"     : "success"
            }

        except Exception as e:
            print(f"  ✗ Erreur {nom_section} : {e}")
            resultats[nom_section] = {
                "statut": "error",
                "erreur": str(e)
            }

    return resultats

def sauvegarder_fichier(contenu, chemin="data/onssa_complet.xlsx"):
    """Sauvegarde le contenu binaire dans un fichier Excel."""
    os.makedirs("data", exist_ok=True)
    with open(chemin, "wb") as f:
        f.write(contenu)
    print(f"Fichier sauvegardé : {chemin} ({len(contenu):,} octets)")
    return chemin
