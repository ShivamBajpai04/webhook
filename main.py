from fastapi import APIRouter, FastAPI

from routers.webhooks.urls import router as webhook_router

app = FastAPI()

api = APIRouter(prefix="/api")
api.include_router(webhook_router)

app.include_router(api)


@app.get("/")
async def root():
    return {"message": "Hello World"}
