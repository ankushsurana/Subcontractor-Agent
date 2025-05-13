# Subcontractor Research Agent

A FastAPI-based agent for domain discovery, extraction, license checking, and scoring, with background job support and modular business logic.

## Features
- REST API (FastAPI)
- Background jobs (Celery + Redis)
- MongoDB for persistence
- Modular, extensible business logic
- Async HTTP utilities (httpx, BeautifulSoup)
- Robust error handling (404, timeouts, connection errors)
- Scoring and ranking with customizable weights
- License verification and project history enrichment
- NLP/transformers support (optional)

## Installation

1. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

2. **(Recommended) Use Docker for full stack:**
   ```sh
   docker-compose up --build
   ```

   - This will start the API, Celery worker, Redis, and MongoDB containers.

3. **API available at:** [http://localhost:8000](http://localhost:8000)

## Environment Variables

See `.env` for connection settings. Example:
```
REDIS_URL=redis://redis:6379/0
MONGO_URL=mongodb://mongo:27017
SERPAPI_KEY=your_serpapi_key
BING_API_KEY=your_bing_key
```

## Usage

- **API:** Submit research requests via the FastAPI endpoint.
- **Background Jobs:** Long-running research is handled by Celery workers.
- **Persistence:** Results and job metadata are stored in MongoDB.
- **Error Handling:** 404s and network errors are logged and skipped, not fatal.
- **Scoring:** Candidates are scored and ranked using a flexible, extensible scoring engine.

## Notes
- TensorFlow and transformers are optional; if not needed, remove from `requirements.txt` for faster builds.
- The agent is designed for extensibility: add new extractors, scoring logic, or data sources as needed.
- For best performance, use the CPU-only version of TensorFlow unless GPU is required.

## Troubleshooting
- If you see repeated TensorFlow or CUDA warnings, switch to `tensorflow-cpu` in `requirements.txt`.
- If you encounter 404 errors during extraction, the agent will log and skip those URLs automatically.
- For MongoDB connection issues, the agent will retry and log errors.

---

For more details, see the code in the `core/`, `api/`, and `workers/` directories.
