FROM python:3.13.5-slim

WORKDIR /app

COPY ./src /app

RUN pip install --no-cache-dir fastapi uvicorn

EXPOSE 8080

CMD ["uvicorn", "predict:app", "--host", "0.0.0.0", "--port", "8080"]