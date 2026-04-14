# actadiag_rag — Base de Connaissance Réglementaire

Projet RAG pour actaDiag — e-acta 2026

## Description

Ce projet construit une base de connaissance réglementaire pour enrichir les services AgriDoc et AgroSage d'actaDiag avec les données officielles ONSSA et les normes d'export GlobalG.A.P.

## Structure du projet

```
actadiag_rag/
├── db/               → Base de données PostgreSQL + pgvector
├── pipeline/         → Pipeline d'ingestion ONSSA
├── rag/              → Retrieval sémantique
├── api/              → Serveur FastAPI
├── tests/            → Tests automatisés
└── data/             → Fichiers Excel téléchargés (non versionné)
```

## Installation

```bash
# 1. Cloner le projet
git clone <url_du_repo>
cd actadiag_rag

# 2. Créer l'environnement virtuel
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env
# Remplir les valeurs dans .env

# 5. Initialiser la base de données
python db/init_db.py
```

## Démarrer le serveur

```bash
uvicorn api.main:app --reload --port 8000
```

## Lancer le pipeline ONSSA manuellement

```bash
python pipeline/run_pipeline.py
```

## Sprints

- Sprint 1 : Schéma BDD + Pipeline ONSSA + Embeddings
- Sprint 2 : Retrieval RAG + Intégration llm_service.py
- Sprint 3 : Pipeline GlobalG.A.P + LMR
- Sprint 4 : Flux sync actaERP®
