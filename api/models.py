"""
Modèles Pydantic — structure des requêtes et réponses de l'API.
"""
from pydantic import BaseModel
from typing import Optional

class QueryRequest(BaseModel):
    """Requête envoyée par AgriDoc ou AgroSage."""
    question: str
    culture: Optional[str] = None
    user_id: Optional[str] = None

class QueryResponse(BaseModel):
    """Réponse retournée à l'utilisateur."""
    answer: str
    sources: Optional[list] = []

class SyncResponse(BaseModel):
    """Réponse après déclenchement d'une sync manuelle."""
    statut: str
    message: str
