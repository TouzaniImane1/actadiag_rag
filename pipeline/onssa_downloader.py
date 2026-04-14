"""
Pipeline ONSSA — Téléchargement automatique avec Selenium
Télécharge 3 fichiers :
  1. Index Phytosanitaire (4686 produits)
  2. Changement de dose   (section ln2Rech6)
  3. Changement de DAR    (section ln2Rech7)
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time, os, shutil, gzip, hashlib

ONSSA_URL = "https://eservice.onssa.gov.ma/IndPesticide.aspx"
MODIF_URL = "https://eservice.onssa.gov.ma/ModifHomolog.aspx"
DOSSIER = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)
SECTIONS = {
    "changement_dose": {
        "id_lien"    : "ctl00_CPHCorps_ln2Rech6",
        "nom_fichier": "onssa_changement_dose"
    },
    "changement_dar": {
        "id_lien"    : "ctl00_CPHCorps_ln2Rech7",
        "nom_fichier": "onssa_changement_dar"
    },
}

def configurer_driver():
    os.makedirs(DOSSIER, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory"  : DOSSIER,
        "download.prompt_for_download": False,
        "download.directory_upgrade"  : True,
        "safebrowsing.enabled"        : True
    })
    # Décommenter en production sur VM Debian :
    # options.add_argument("--headless")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def nettoyer_dossier():
    os.makedirs(DOSSIER, exist_ok=True)
    for f in os.listdir(DOSSIER):
        if f.endswith((".gz", ".xls", ".xlsx")):
            os.remove(os.path.join(DOSSIER, f))
            print(f"  Supprimé : {f}")

def attendre_nouveau_fichier(fichiers_avant, timeout=120):
    print("  Attente téléchargement...")
    debut = time.time()
    while time.time() - debut < timeout:
        fichiers_apres = set(
            f for f in os.listdir(DOSSIER)
            if f.endswith((".gz", ".xls", ".xlsx"))
            and not f.endswith(".crdownload")
        )
        nouveaux = fichiers_apres - fichiers_avant
        if nouveaux:
            time.sleep(2)
            return os.path.join(DOSSIER, list(nouveaux)[0])
        time.sleep(1)
    return None

def decompresser(chemin_gz):
    if not chemin_gz.endswith(".gz"):
        return chemin_gz
    chemin_xls = chemin_gz[:-3]
    with gzip.open(chemin_gz, 'rb') as f_in:
        with open(chemin_xls, 'wb') as f_out:
            f_out.write(f_in.read())
    os.remove(chemin_gz)
    print(f"  Décompressé : {os.path.basename(chemin_xls)}")
    return chemin_xls

def calculer_hash(chemin):
    with open(chemin, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def cliquer_bouton_excel(wait):
    """
    Attend que l'overlay disparaisse puis clique sur Excel.
    Robuste contre les délais de chargement du site ONSSA.
    """
    # Attendre que le WaitProgressOverLayer disparaisse
    print("  Attente disparition overlay...")
    try:
        wait.until(EC.invisibility_of_element_located(
            (By.CSS_SELECTOR, "div.WaitProgressOverLayer")
        ))
        print("  Overlay disparu ✓")
    except Exception:
        print("  Pas d'overlay détecté ✓")

    # Clic bouton Excel
    print("  Clic bouton Excel...")
    wait.until(EC.element_to_be_clickable(
        (By.NAME, "ctl00$CPHCorps$epp")
    )).click()

def telecharger_index_phytosanitaire(driver, wait):
    print("\n" + "="*50)
    print("TÉLÉCHARGEMENT 1/3 : Index Phytosanitaire")
    print("="*50)

    fichiers_avant = set(
        f for f in os.listdir(DOSSIER)
        if f.endswith((".gz", ".xls", ".xlsx"))
    )

    driver.get(ONSSA_URL)
    time.sleep(2)
    print(f"  Page ouverte : {driver.title}")

    print("  Clic Recherche Multicritère...")
    wait.until(EC.element_to_be_clickable(
        (By.ID, "ctl00_CPHCorps_lnkrech9")
    )).click()
    time.sleep(3)

    print("  Clic Rechercher...")
    wait.until(EC.element_to_be_clickable(
        (By.NAME, "ctl00$CPHCorps$nfBtn")
    )).click()
    print("  Chargement des 4686 produits (~25 sec)...")
    time.sleep(25)

    # Attendre overlay + cliquer Excel
    cliquer_bouton_excel(wait)

    chemin = attendre_nouveau_fichier(fichiers_avant)
    if not chemin:
        print("  ERREUR : Téléchargement échoué !")
        return None, None

    print(f"  Reçu : {os.path.basename(chemin)}")
    if chemin.endswith(".gz"):
        chemin = decompresser(chemin)

    final = os.path.join(DOSSIER, "onssa_index_phyto.xls")
    if os.path.exists(final):
        os.remove(final)
    shutil.move(chemin, final)

    hash_fichier = calculer_hash(final)
    print(f"  Sauvegardé : onssa_index_phyto.xls ✓")
    print(f"  Taille     : {os.path.getsize(final):,} octets")
    print(f"  Hash       : {hash_fichier[:16]}...")
    return final, hash_fichier

def telecharger_section_modification(
    driver, wait, nom_section, config
):
    print(f"\n{'='*50}")
    print(f"TÉLÉCHARGEMENT : {nom_section}")
    print("="*50)

    fichiers_avant = set(
        f for f in os.listdir(DOSSIER)
        if f.endswith((".gz", ".xls", ".xlsx"))
    )

    driver.get(MODIF_URL)
    time.sleep(2)

    print(f"  Clic section {nom_section}...")
    wait.until(EC.element_to_be_clickable(
        (By.ID, config["id_lien"])
    )).click()
    time.sleep(3)

    # Attendre overlay + cliquer Excel
    cliquer_bouton_excel(wait)

    chemin = attendre_nouveau_fichier(fichiers_avant)
    if not chemin:
        print("  ERREUR : Téléchargement échoué !")
        return None, None

    print(f"  Reçu : {os.path.basename(chemin)}")
    if chemin.endswith(".gz"):
        chemin = decompresser(chemin)

    nom_final = f"{config['nom_fichier']}.xls"
    final = os.path.join(DOSSIER, nom_final)
    if os.path.exists(final):
        os.remove(final)
    shutil.move(chemin, final)

    hash_fichier = calculer_hash(final)
    print(f"  Sauvegardé : {nom_final} ✓")
    print(f"  Taille     : {os.path.getsize(final):,} octets")
    print(f"  Hash       : {hash_fichier[:16]}...")
    return final, hash_fichier

def telecharger_tout():
    """
    Télécharge automatiquement les 3 fichiers ONSSA.
    Appelée par run_pipeline.py chaque lundi via cron.
    """
    print("DÉMARRAGE TÉLÉCHARGEMENT ONSSA COMPLET")
    print(f"Dossier : {DOSSIER}")
    print(f"Heure   : {time.strftime('%Y-%m-%d %H:%M:%S')}")

    nettoyer_dossier()
    driver   = configurer_driver()
    wait     = WebDriverWait(driver, 60)
    resultats = {}

    try:
        chemin, hash_f = telecharger_index_phytosanitaire(
            driver, wait
        )
        resultats["index_phyto"] = {
            "chemin": chemin,
            "hash"  : hash_f,
            "statut": "success" if chemin else "error"
        }

        for nom_section, config in SECTIONS.items():
            chemin, hash_f = telecharger_section_modification(
                driver, wait, nom_section, config
            )
            resultats[nom_section] = {
                "chemin": chemin,
                "hash"  : hash_f,
                "statut": "success" if chemin else "error"
            }

    except Exception as e:
        print(f"\nERREUR : {e}")
        import traceback
        traceback.print_exc()

    finally:
        driver.quit()
        print("\nNavigateur fermé ✓")

    print("\n" + "="*50)
    print("RÉSUMÉ TÉLÉCHARGEMENT")
    print("="*50)
    for nom, info in resultats.items():
        statut = "✓" if info["statut"] == "success" else "✗"
        chemin = info.get("chemin")
        nom_f  = os.path.basename(chemin) if chemin else "ÉCHEC"
        print(f"  {statut} {nom:25} → {nom_f}")

    return resultats

if __name__ == "__main__":
    telecharger_tout()