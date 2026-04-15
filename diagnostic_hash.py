# diagnostic_compare.py
import os, re, html, hashlib

DOSSIER = os.path.abspath("data")
chemin  = os.path.join(DOSSIER, "onssa_index_phyto.xls")

with open(chemin, 'r', encoding='utf-8', errors='ignore') as f:
    contenu = f.read()

contenu = html.unescape(contenu)

# Extraire toutes les cellules
rows = re.findall(r'<tr[^>]*>(.*?)</tr>', contenu, re.DOTALL)
print(f"Nombre de lignes : {len(rows)}")

# Extraire les numéros d'homologation (colonne 4)
homologations = []
for row in rows[1:6]:
    cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
    cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
    if len(cells) > 3:
        homologations.append(cells[3])  # Numéro homologation

print(f"\n5 premiers numéros d'homologation :")
for h in homologations:
    print(f"  '{h}' → repr: {repr(h)}")

# Hash de juste les numéros d'homologation
contenu_cle = "|".join(sorted(homologations))
hash_cle = hashlib.sha256(contenu_cle.encode()).hexdigest()
print(f"\nHash numéros homologation : {hash_cle[:16]}")