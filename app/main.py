from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.predict import get_prediction_message
from app.utils import player_stats_collector

app = FastAPI()

@app.get("/predict")
async def predict():
    return JSONResponse(
        content={"message": get_prediction_message()},
        status_code=200
    )
    
@app.get("/health")
async def health():
    return JSONResponse(
        content={"status": "ok"},
        status_code=200
    )

@app.api_route("/run-jobs", methods=["GET", "HEAD"])
async def run_jobs():
    result = player_stats_collector.get_week_of_season()
    if result is None:
        return JSONResponse(
            content={"error": "NFL season is not currently active. No data to process."},
            status_code=500
        )
    else:
        year, week = result
        player_stats_collector.process_nfl_season_data(year, week)
        return JSONResponse(
            content={"status": "jobs are running"},
            status_code=200
        )
