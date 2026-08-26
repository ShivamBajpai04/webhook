from fastapi import APIRouter
from repositories.kafka import publish

router = APIRouter(prefix="/webhook", tags=["webhooks"])


@router.post("/gand_masti")
async def handel_gandmasti(payload: dict):
    publish("webhooks", payload)
    return {"status": "SUCCESS", "status_code": 200, "message": "Message recieved"}

@router.get("/gand_masti")
async def get_gandmasti():
    return {"hello": 123}
