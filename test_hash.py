# test_hash.py
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import hashlib
from pipeline.onssa_parser import parser_index_phyto

df = parser_index_phyto()
contenu = df.to_csv(index=False).encode()
hash_contenu = hashlib.sha256(contenu).hexdigest()
print(f"Hash contenu parsé : {hash_contenu}")