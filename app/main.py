from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.services import player_stats_and_props_collector as pspc
from app.services.rolling_stats_calculator import RollingStatsCalculator
from app.services.probability_calculator import ProbabilityCalculator
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://nfl-bets-backend.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    try:
        # Simple health check without initializing heavy services
        return JSONResponse(
            content={"status": "ok", "message": "Service is running"},
            status_code=200
        )
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=503
        )

@app.get("/run-jobs")
async def run_jobs():
    try:
        data_collector = pspc.PlayerStatsAndPropsCollector()
        calculator = RollingStatsCalculator()
        
        week_info = data_collector.get_week_of_season()
        if week_info is None:
            return JSONResponse(
                content={"error": "NFL season is not currently active. No data to process."},
                status_code=200
            )
        else:
            year, week = week_info
            for week_num in range(1, week + 1):
                data_collector.process_nfl_season_data(year, week_num)
            data_collector.update_today_player_props()
            calculator.update_all_rolling_stats()
            return JSONResponse(
                content={"status": "jobs completed"},
                status_code=200
            )
    except Exception as e:
        return JSONResponse(
            content={"error": f"Failed to run jobs: {str(e)}"},
            status_code=500
        )
        
@app.get("/predict")
async def predict():
    """Generate probabilities for all players with props today"""
    try:
        prob_calculator = ProbabilityCalculator()
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