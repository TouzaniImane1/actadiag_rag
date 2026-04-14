"""
Service LLM — version enrichie avec RAG.
Ce fichier existait avant le stage — on y a ajouté
l'appel à retrieve_rag_context() avant chaque réponse.
"""
import os
import anthropic
from dotenv import load_dotenv
from rag.retrieval import retrieve_rag_context

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT_BASE = """
Tu es AgroSage, un conseiller agronomique expert pour les agriculteurs marocains.

Tu dois TOUJOURS :
- Baser tes réponses sur les informations réglementaires fournies
- Citer explicitement ta source (ONSSA ou GlobalG.A.P)
- Mentionner les doses et délais avant récolte officiels
- Avertir si un produit n'est pas homologué ou si la LMR est dépassée

Tu ne dois JAMAIS :
- Inventer des informations réglementaires
- Donner des recommandations sans base légale
- Ignorer les contraintes d'export si l'agriculteur exporte
"""

async def ask(question: str, culture: str = None) -> str:
    """
    Pose une question au LLM enrichi par le contexte RAG.
    
    Paramètres :
        question : la question de l'utilisateur
        culture  : culture concernée (optionnel, pour filtrer le RAG)
    
    Retourne la réponse du LLM.
    """
    # === AJOUT RAG : récupérer le contexte réglementaire ===
    contexte_rag = retrieve_rag_context(question, culture=culture)

    # Construire le system prompt avec le contexte RAG injecté
    system_prompt = f"""
    {SYSTEM_PROMPT_BASE}
    
    {contexte_rag}
    """

    # Appel au LLM (Claude)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system_prompt,
        messages=[
            {"role": "user", "content": question}
        ]
    )

    return response.content[0].text
