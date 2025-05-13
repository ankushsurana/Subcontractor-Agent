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

## Scoring Mechanism

The Subcontractor Research Agent uses a robust, multi-factor scoring system to rank subcontractor candidates. The scoring is handled by the `SubcontractorScorer` class in `core/scoring.py`.

### How Scoring Works

Each candidate is evaluated on the following factors, each with a configurable weight:

- **Experience (30%)**: Based on the number and quality of Texas projects completed in the last 5 years. More projects and higher project quality increase the score.
- **License (25%)**: Checks if the business has an active license and how close it is to expiration. Active, long-validity licenses score higher.
- **Bonding (20%)**: Measures bonding capacity relative to the minimum required. Higher bonding capacity increases the score.
- **Geography (15%)**: Rewards candidates in the target city/state, with a distance decay for those further away.
- **Reputation (10%)**: Considers years in business, positive reviews, awards, and union membership.

### Score Calculation

- Each factor is normalized to a 0-1 range.
- The final score is a weighted sum of all factors, scaled to 0-100.
- Example formula:
  ```python
  total_score = (
      0.30 * experience_score +
      0.25 * license_score +
      0.20 * bonding_score +
      0.15 * geography_score +
      0.10 * reputation_score
  ) * 100
  ```
- Candidates are ranked by total score, with tie-breakers on experience, license, bonding, and years in business.

### Output
- Each result includes a `score` and a `score_breakdown` for transparency.
- The scoring system is extensible: you can adjust weights or add new factors as needed.

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
