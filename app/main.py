from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.predict import get_prediction_message

app = FastAPI()

@app.get("/predict")
async def predict():
    return JSONResponse(
        content={"message": get_prediction_message()},
        status_code=200
    )
    
@app.get("/health")
async def health():
    return {"status": "ok"}
