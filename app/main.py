import os
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.services.probability_calculator import ProbabilityCalculator
from typing import Optional

app = FastAPI()

frontend_urls = os.getenv("FRONTEND_URL")
cors_origins = [url.strip() for url in frontend_urls.split(",") if url.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key validation
def verify_api_key(x_api_key: str = Header()):
    """Verify the API key from request headers"""
    expected_api_key = os.getenv("BACKEND_API_KEY")
    if not expected_api_key:
        raise HTTPException(status_code=500, detail="API key not configured")
    if x_api_key != expected_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

@app.get("/health")
async def health():
    try:
        # Health check doesn't require authentication
        return JSONResponse(
            content={"status": "ok", "message": "Service is running"},
            status_code=200
        )
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=503
        )
        
@app.get("/predict")
async def predict(api_key: str = Depends(verify_api_key)):
    """Generate probabilities for all players with props today"""
    try:
        prob_calculator = ProbabilityCalculator()
        
        # Try to get cached probabilities first
        cached_probabilities = prob_calculator.get_cached_probabilities()
        
        if cached_probabilities:
            return JSONResponse(
                content={"Probabilities": cached_probabilities, "source": "cache"},
                status_code=200
            )
        
        # Fallback to real-time calculation if no cache available
        probabilities = prob_calculator.get_all_todays_probabilities()
        return JSONResponse(
            content={"Probabilities": probabilities},
            status_code=200
        )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Failed to calculate probabilities: {str(e)}"},
            status_code=500
        )