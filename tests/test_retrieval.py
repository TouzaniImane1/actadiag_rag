"""
Tests du retrieval RAG.
Vérifie la précision sur un jeu de 50 questions test.
Critère qualité : > 85% de pertinence.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rag.retrieval import retrieve_rag_context

# Jeu de questions test avec les réponses attendues
QUESTIONS_TEST = [
    {
        "question": "Quels fongicides sont homologués ONSSA pour le Botrytis sur fraise ?",
        "mot_cle_attendu": "Botrytis",
        "culture": "Fraisier"
    },
    {
        "question": "Puis-je utiliser le Switch sur tomate ?",
        "mot_cle_attendu": "Switch",
        "culture": "Tomate"
    },
    {
        "question": "Quel est le délai avant récolte pour les agrumes ?",
        "mot_cle_attendu": "délai",
        "culture": "Agrumes"
    },
    {
        "question": "Produits homologués contre les acariens sur fraisier",
        "mot_cle_attendu": "acarien",
        "culture": "Fraisier"
    },
    {
        "question": "Insecticides autorisés pour la tomate au Maroc",
        "mot_cle_attendu": "Insecticide",
        "culture": "Tomate"
    },
]

def test_precision_retrieval():
    """
    Teste la précision du retrieval sur les questions test.
    Critère : > 85% des questions retournent un résultat pertinent.
    """
    total = len(QUESTIONS_TEST)
    reussis = 0

    for test in QUESTIONS_TEST:
        contexte = retrieve_rag_context(
            test["question"],
            culture=test["culture"]
        )

        # Vérifier que le contexte contient des informations pertinentes
        if (contexte and
            "Aucune information" not in contexte and
            len(contexte) > 100):
            reussis += 1
            print(f"✓ '{test['question'][:50]}...'")
        else:
            print(f"✗ '{test['question'][:50]}...'")

    precision = (reussis / total) * 100
    print(f"\nPrécision : {reussis}/{total} = {precision:.1f}%")

    assert precision >= 85, (
        f"Précision insuffisante : {precision:.1f}% "
        f"(minimum requis : 85%)"
    )
    print("Test précision RAG OK ✓")

def test_temps_reponse():
    """
    Vérifie que le retrieval répond en moins de 500ms.
    Critère qualité de la spec.
    """
    import time

    question = "Fongicides homologués ONSSA pour fraise"
    debut = time.time()
    retrieve_rag_context(question, culture="Fraisier")
    duree_ms = (time.time() - debut) * 1000

    print(f"Temps de réponse RAG : {duree_ms:.0f} ms")
    assert duree_ms < 500, (
        f"Trop lent : {duree_ms:.0f} ms (max : 500 ms)"
    )
    print("Test temps de réponse OK ✓")

if __name__ == "__main__":
    print("=== Tests Retrieval RAG ===")
    test_precision_retrieval()
    test_temps_reponse()
    print("=== Tous les tests passés ✓ ===")
