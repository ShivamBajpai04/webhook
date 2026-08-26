FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "fastapi[standard]" confluent-kafka

COPY main.py consumer.py ./
COPY routers ./routers
COPY repositories ./repositories

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
