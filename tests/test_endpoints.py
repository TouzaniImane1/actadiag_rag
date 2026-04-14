"""
Tests des endpoints FastAPI.
Vérifie que l'API répond correctement.
"""
from fastapi.testclient import TestClient
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.main import app

client = TestClient(app)

def test_health_check():
    """Vérifie que le serveur est en ligne."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["statut"] == "ok"
    print("Test /health OK ✓")

def test_get_produits():
    """Vérifie que l'endpoint produits retourne des données."""
    response = client.get("/produits?culture=Fraisier")
    assert response.status_code == 200
    data = response.json()
    assert "produits" in data
    assert "total" in data
    print(f"Test /produits OK ✓ — {data['total']} produits trouvés")

def test_get_produits_filtre():
    """Vérifie le filtre par matière active."""
    response = client.get(
        "/produits?matiere_active=Cyprodinil"
    )
    assert response.status_code == 200
    print("Test /produits avec filtre OK ✓")

def test_query_endpoint():
    """Vérifie que l'endpoint /query retourne une réponse."""
    payload = {
        "question": "Quels produits sont homologués pour la fraise ?",
        "culture": "Fraisier"
    }
    response = client.post("/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert len(data["answer"]) > 0
    print("Test /query OK ✓")

if __name__ == "__main__":
    print("=== Tests Endpoints FastAPI ===")
    test_health_check()
    test_get_produits()
    test_get_produits_filtre()
    test_query_endpoint()
    print("=== Tous les tests passés ✓ ===")
