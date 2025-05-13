from fastapi import FastAPI, Request, requests
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from api.routes import router
import logging
import time
import os
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    if not os.path.exists(DATASET_PATH):
        os.makedirs(os.path.dirname(DATASET_PATH), exist_ok=True)
        logging.info("Downloading dataset...")
        try:
            response = requests.get(DATASET_URL, timeout=60)
            response.raise_for_status()
            with open(DATASET_PATH, "wb") as f:
                f.write(response.content)
            logging.info("Dataset downloaded successfully.")
        except Exception as e:
            logging.error(f"Failed to download dataset: {e}")
    else:
        logging.info("Dataset already exists. Skipping download.")
    yield
    # (Optional) Shutdown code here

app = FastAPI(
    title="Subcontractor Research Agent",
    description="API for finding and scoring subcontractors",
    version="1.0.0",
    lifespan=lifespan
)

# Add middleware for CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add middleware for request timing
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "An internal server error occurred"},
    )

# Include API routes
app.include_router(router, prefix="/api/v1")

DATASET_URL = "https://drive.google.com/file/d/1ZD3yt4FMyWxqoCqwd5nIrHvBVkwVNSc9/view?usp=drive_link"
DATASET_PATH = "dataset/TDLR_All_Licenses.csv"