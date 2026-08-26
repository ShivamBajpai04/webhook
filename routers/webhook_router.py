from fastapi import APIRouter

from repositories.kafka import publish

router = APIRouter(prefix="/webhook", tags=["webhooks"])


@router.get("/gand_masti")
async def handel_gandmasti(payload: dict):
    publish("webhook", payload)
    return {"status": "SUCCESS", "status_code": 200, "message": "Message recieved"}
