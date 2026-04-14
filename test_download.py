"""
Téléchargement ONSSA avec Selenium — gère .xls.gz
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import shutil
import gzip
import pandas as pd

ONSSA_URL = "https://eservice.onssa.gov.ma/IndPesticide.aspx"
DOSSIER_TELECHARGEMENT = os.path.abspath("data")

def configurer_driver():
    os.makedirs(DOSSIER_TELECHARGEMENT, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory"  : DOSSIER_TELECHARGEMENT,
        "download.prompt_for_download": False,
        "download.directory_upgrade"  : True,
        "safebrowsing.enabled"        : True
    })
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def attendre_telechargement(dossier, timeout=120):
    """Attend que le fichier soit téléchargé — gère .gz aussi."""
    print("  Attente du téléchargement...")
    debut = time.time()
    while time.time() - debut < timeout:
        fichiers = [
            f for f in os.listdir(dossier)
            if (
                f.endswith(".xlsx") or
                f.endswith(".xls")  or
                f.endswith(".gz")
            )
            and not f.endswith(".crdownload")
        ]
        if fichiers:
            time.sleep(2)
            return os.path.join(dossier, fichiers[0])
        time.sleep(1)
    return None

def decompresser_gz(chemin_gz):
    """Décompresse un fichier .xls.gz en .xls"""
    print(f"  Décompression de : {os.path.basename(chemin_gz)}")

    # Nom du fichier décompressé
    if chemin_gz.endswith(".gz"):
        chemin_xls = chemin_gz[:-3]  # Enlève le .gz
    else:
        chemin_xls = chemin_gz + ".xls"

    # Décompresser
    with gzip.open(chemin_gz, 'rb') as f_in:
        contenu = f_in.read()

    with open(chemin_xls, 'wb') as f_out:
        f_out.write(contenu)

    print(f"  Décompressé : {os.path.basename(chemin_xls)} ✓")
    print(f"  Taille décompressée : {len(contenu):,} octets")

    return chemin_xls

def lire_fichier(chemin):
    """Lit le tableau ONSSA depuis le fichier HTML/XLS."""
    print(f"\n  Lecture du fichier : {os.path.basename(chemin)}")

    with open(chemin, 'r', encoding='utf-8', errors='ignore') as f:
        contenu = f.read()

    # Extraire juste le tableau principal
    # Le tableau commence à <table id="ctl00_CPHCorps_tblProduits"
    # et finit au premier </table>
    debut_table = contenu.find('<table id="ctl00_CPHCorps_tblProduits"')
    if debut_table == -1:
        debut_table = contenu.find("<table id='ctl00_CPHCorps_tblProduits'")

    if debut_table == -1:
        print("  Tableau principal introuvable !")
        return None

    fin_table = contenu.find('</table>', debut_table)
    html_tableau = contenu[debut_table:fin_table + 8]
    print(f"  Tableau extrait : {len(html_tableau):,} caractères")

    # Parser avec BeautifulSoup
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_tableau, 'html.parser')
    tableau = soup.find('table')

    if not tableau:
        print("  Tableau introuvable après parsing !")
        return None

    # Extraire toutes les lignes
    lignes = tableau.find_all('tr')
    print(f"  Lignes trouvées : {len(lignes)}")

    if not lignes:
        return None

    # Première ligne = en-têtes
    entete = lignes[0]
    colonnes = [
        td.get_text(strip=True)
        for td in entete.find_all('td')
    ]
    print(f"  Colonnes : {colonnes}")

    # Reste = données
    donnees = []
    for tr in lignes[1:]:
        ligne = [
            td.get_text(strip=True)
            for td in tr.find_all('td')
        ]
        if ligne:
            donnees.append(ligne)

    # Créer DataFrame
    df = pd.DataFrame(donnees, columns=colonnes)
    print(f"  Lu avec succès ✓")
    return df

def telecharger_onssa():
    print("Démarrage téléchargement ONSSA avec Selenium...")

    os.makedirs(DOSSIER_TELECHARGEMENT, exist_ok=True)

    # Nettoyer anciens fichiers
    for f in os.listdir(DOSSIER_TELECHARGEMENT):
        if any(f.endswith(ext) for ext in [".xlsx", ".xls", ".gz"]):
            os.remove(os.path.join(DOSSIER_TELECHARGEMENT, f))
            print(f"  Supprimé : {f}")

    driver = configurer_driver()
    wait = WebDriverWait(driver, 30)

    try:
        # Étape 1 — Ouvrir la page
        print("\nÉtape 1 : Ouverture page ONSSA...")
        driver.get(ONSSA_URL)
        time.sleep(2)
        print(f"  Titre : {driver.title}")

        # Étape 2 — Recherche Multicritère
        print("\nÉtape 2 : Clic Recherche Multicritère...")
        lien_multi = wait.until(
            EC.element_to_be_clickable(
                (By.ID, "ctl00_CPHCorps_lnkrech9")
            )
        )
        lien_multi.click()
        time.sleep(3)
        print("  Cliqué ✓")

        # Étape 3 — Rechercher
        print("\nÉtape 3 : Clic Rechercher...")
        btn_rechercher = wait.until(
            EC.element_to_be_clickable(
                (By.NAME, "ctl00$CPHCorps$nfBtn")
            )
        )
        btn_rechercher.click()
        print("  Cliqué ✓")
        print("  Chargement résultats (~15 sec)...")
        time.sleep(15)

        # Étape 4 — Clic Excel
        print("\nÉtape 4 : Clic bouton Excel...")
        btn_excel = wait.until(
            EC.element_to_be_clickable(
                (By.NAME, "ctl00$CPHCorps$epp")
            )
        )
        btn_excel.click()
        print("  Cliqué ✓")

        # Étape 5 — Attendre téléchargement
        print("\nÉtape 5 : Attente téléchargement...")
        chemin = attendre_telechargement(DOSSIER_TELECHARGEMENT)

        if not chemin:
            print("  ERREUR : Téléchargement échoué !")
            return

        print(f"  Fichier reçu : {os.path.basename(chemin)} ✓")
        print(f"  Taille       : {os.path.getsize(chemin):,} octets")

        # Étape 6 — Décompresser si .gz
        if chemin.endswith(".gz"):
            chemin = decompresser_gz(chemin)

        # Renommer en onssa_complet
        ext = os.path.splitext(chemin)[1]
        nouveau_chemin = os.path.join(
            DOSSIER_TELECHARGEMENT,
            f"onssa_complet{ext}"
        )
        if os.path.exists(nouveau_chemin):
            os.remove(nouveau_chemin)
        shutil.move(chemin, nouveau_chemin)
        print(f"  Renommé : onssa_complet{ext} ✓")

        # Étape 7 — Lire et afficher
        df = lire_fichier(nouveau_chemin)

        if df is not None:
            print(f"\n  Lignes   : {len(df)}")
            print(f"  Colonnes :")
            for i, col in enumerate(df.columns, 1):
                print(f"    {i}. '{col}'")
            print(f"\n  3 premières lignes :")
            print(df.head(3).to_string())
        else:
            print("  Impossible de lire le fichier.")

    except Exception as e:
        print(f"\n  ERREUR : {e}")
        import traceback
        traceback.print_exc()

    finally:
        driver.quit()
        print("\nFermeture navigateur ✓")

    print("\nTest terminé !")

if __name__ == "__main__":
    telecharger_onssa()