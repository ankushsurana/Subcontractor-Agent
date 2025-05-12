# Subcontractor Research Agent

A FastAPI-based agent for domain discovery, extraction, license checking, and scoring, with background job support and modular business logic.

## Features
- REST API (FastAPI)
- Background jobs (Celery + Redis)
- Modular business logic
- Async HTTP utilities

## install Dependiencies
- pip install -r requirements.txt

## Quickstart (Docker)

1. Build and start all services (API, Redis, Postgres):
   ```sh
   docker-compose up --build
   ```

2. The API will be available at [http://localhost:8000](http://localhost:8000)


## Environment Variables

See `.env` for connection settings. Example:
```
REDIS_URL=redis://redis:6379/0
MONGO_URL=postgresql://user:password@db:5432/agentdb
```
