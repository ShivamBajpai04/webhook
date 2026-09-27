import logging
import os
import secrets

from fastapi import APIRouter, Header, HTTPException

from repositories.kafka import KafkaPublishError, publish
from .webhook_payload import WebhookPayload

router = APIRouter(prefix="/webhook", tags=["webhooks"])
logger = logging.getLogger(__name__)
MAX_EVENT_BYTES = 64 * 1024




@router.post("/gand_masti", status_code=202)
def handle_gandmasti(
    payload: WebhookPayload,
    x_webhook_token: str | None = Header(default=None),
) -> dict[str, str]:
    expected_token = os.getenv("WEBHOOK_TOKEN")
    if not expected_token:
        logger.error("WEBHOOK_TOKEN is not configured")
        raise HTTPException(status_code=503, detail="Webhook unavailable")
    if not x_webhook_token or not secrets.compare_digest(x_webhook_token, expected_token):
        raise HTTPException(status_code=401, detail="Invalid webhook token")
    if len(payload.model_dump_json().encode()) > MAX_EVENT_BYTES:
        raise HTTPException(status_code=413, detail="Event too large")

    try:
        publish(
            topic=os.getenv("KAFKA_TOPIC", "webhooks"),
            payload=payload.model_dump(mode="json"),
            key=str(payload.event_id),
        )
    except KafkaPublishError:
        logger.exception("Kafka delivery failed for event %s", payload.event_id)
        raise HTTPException(status_code=503, detail="Webhook unavailable") from None

    return {"status": "accepted", "event_id": str(payload.event_id)}
