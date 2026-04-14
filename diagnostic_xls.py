# diagnostic_xls.py
import pandas as pd
from bs4 import BeautifulSoup
import os

chemin = "data/onssa_complet.xls"

with open(chemin, 'r', encoding='utf-8', errors='ignore') as f:
    contenu = f.read()

debut_table = contenu.find('<table id="ctl00_CPHCorps_tblProduits"')
fin_table = contenu.find('</table>', debut_table)
html_tableau = contenu[debut_table:fin_table + 8]

soup = BeautifulSoup(html_tableau, 'html.parser')
tableau = soup.find('table')
lignes = tableau.find_all('tr')

print(f"Lignes : {len(lignes)}")

# En-têtes
entete = lignes[0]
colonnes = [td.get_text(strip=True) for td in entete.find_all('td')]
print(f"Colonnes : {colonnes}")

# Données
donnees = []
for tr in lignes[1:]:
    ligne = [td.get_text(strip=True) for td in tr.find_all('td')]
    if ligne:
        donnees.append(ligne)

df = pd.DataFrame(donnees, columns=colonnes)
print(f"\nNombre de produits : {len(df)}")
print(f"\n3 premières lignes :")
print(df.head(3).to_string())