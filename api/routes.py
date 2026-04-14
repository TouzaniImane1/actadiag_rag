"""
Endpoints FastAPI — les portes d'entrée de l'API.
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException
from api.models import QueryRequest, QueryResponse, SyncResponse
from api.llm_service import ask
from pipeline.run_pipeline import run as run_onssa_pipeline

router = APIRouter()

@router.post("/query", response_model=QueryResponse)
async def query_rag(request: QueryRequest):
    """
    Endpoint principal — reçoit une question et retourne
    une réponse enrichie par le RAG réglementaire.
    
    Utilisé par AgriDoc et AgroSage.
    """
    try:
        reponse = await ask(
            question=request.question,
            culture=request.culture
        )
        return QueryResponse(answer=reponse)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/produits")
def get_produits(culture: str = None, matiere_active: str = None):
    """
    Retourne la liste des produits homologués ONSSA.
    Filtrable par culture et/ou matière active.
    """
    from db.connection import execute_query

    sql = "SELECT * FROM produits_homologues WHERE 1=1"
    params = []

    if culture:
        sql += " AND culture ILIKE %s"
        params.append(f"%{culture}%")

    if matiere_active:
        sql += " AND matiere_active ILIKE %s"
        params.append(f"%{matiere_active}%")

    sql += " ORDER BY nom_commercial LIMIT 100"

    produits = execute_query(sql, params, fetch=True)
    return {"produits": produits, "total": len(produits)}

@router.post("/sync/onssa", response_model=SyncResponse)
def sync_onssa(background_tasks: BackgroundTasks):
    """
    Déclenche manuellement le pipeline ONSSA en arrière-plan.
    """
    background_tasks.add_task(run_onssa_pipeline)
    return SyncResponse(
        statut="started",
        message="Pipeline ONSSA démarré en arrière-plan."
    )

@router.get("/health")
def health_check():
    """Vérifie que le serveur est en ligne."""
    return {"statut": "ok", "service": "actadiag_rag"}
