"""
Point d'entrée FastAPI — démarre le serveur actadiag_rag.

Lancement :
    uvicorn api.main:app --reload --port 8000

Sur la VM Debian :
    systemctl start actadiag_api
"""
from fastapi import FastAPI
from api.routes import router

app = FastAPI(
    title="actadiag_rag API",
    description="Base de connaissance réglementaire ONSSA pour actaDiag",
    version="1.0.0"
)

# Enregistrer les routes
app.include_router(router)

# Point de départ
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
