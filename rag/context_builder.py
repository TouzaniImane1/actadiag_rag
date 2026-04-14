"""
Formate les chunks RAG en bloc de contexte
prêt à être injecté dans le prompt du LLM.
"""

def formater_contexte_agridoc(chunks, culture, maladie):
    """
    Formate le contexte pour AgriDoc (diagnostic maladie par photo).
    La culture et la maladie sont déjà connues.
    """
    if not chunks:
        return (
            f"Aucun produit homologué ONSSA trouvé "
            f"pour {maladie} sur {culture}."
        )

    contexte = (
        f"=== PRODUITS HOMOLOGUÉS ONSSA ===\n"
        f"Culture : {culture} | Maladie : {maladie}\n\n"
    )

    for i, chunk in enumerate(chunks, 1):
        contexte += f"Produit {i} :\n"
        contexte += f"{chunk['contenu_texte']}\n"
        contexte += "-" * 30 + "\n"

    contexte += (
        "\nSource : Index Phytosanitaire ONSSA — "
        "eservice.onssa.gov.ma\n"
        "Cite toujours le nom commercial et la dose officielle.\n"
    )
    return contexte

def formater_contexte_agrosage(chunks, question):
    """
    Formate le contexte pour AgroSage (conseil conversationnel).
    La question est floue — on retourne tous les chunks pertinents.
    """
    if not chunks:
        return (
            "Aucune information réglementaire trouvée "
            "pour cette question."
        )

    contexte = "=== INFORMATIONS RÉGLEMENTAIRES OFFICIELLES ===\n\n"

    for i, chunk in enumerate(chunks, 1):
        score = chunk.get("score", 0)
        source = chunk.get("source", "ONSSA")
        contexte += f"[{source} — Pertinence : {score:.0%}]\n"
        contexte += f"{chunk['contenu_texte']}\n"
        contexte += "-" * 40 + "\n"

    contexte += (
        "\n=== FIN DES INFORMATIONS ===\n"
        "Important : Cite explicitement ta source "
        "(ONSSA ou GlobalG.A.P) dans ta réponse.\n"
        "Si une information concerne l'export, "
        "mentionne le pays destinataire et la LMR applicable.\n"
    )
    return contexte
