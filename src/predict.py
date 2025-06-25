from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/predict")
async def predict():
    return JSONResponse(
        content={
            "message": "This is a placeholder for the prediction endpoint."
        },
        status_code=200
    )
