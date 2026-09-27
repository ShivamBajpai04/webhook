import json
import logging
import os
import signal
from threading import Lock

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

from routers.webhooks.webhook_payload import WebhookPayload

load_dotenv()
logger = logging.getLogger(__name__)
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "webhooks")
_producer: Producer | None = None
_producer_lock = Lock()


class KafkaPublishError(RuntimeError):
    pass


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        with _producer_lock:
            if _producer is None:
                broker = os.getenv("KAFKA_URL")
                if not broker:
                    raise KafkaPublishError("KAFKA_URL is not configured")
                _producer = Producer({"bootstrap.servers": broker})
    return _producer


def publish(topic: str, payload: dict, key: str) -> None:
    outcome: dict[str, object] = {"called": False, "error": None}

    def report(error, _message):
        outcome["called"] = True
        outcome["error"] = error

    try:
        producer = get_producer()
        producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(payload).encode(),
            callback=report,
        )
        remaining = producer.flush(10)
    except Exception as exc:
        raise KafkaPublishError("Could not send event to Kafka") from exc

    if remaining or not outcome["called"]:
        raise KafkaPublishError("Timed out waiting for Kafka delivery")
    if outcome["error"] is not None:
        raise KafkaPublishError(f"Kafka rejected event: {outcome['error']}")


def process_message(raw: bytes) -> None:
    payload = WebhookPayload.model_validate_json(raw)
    logger.info("received event_id=%s event=%s", payload.event_id, payload.event)


def consume(topic: str = DEFAULT_TOPIC, group_id: str = "webhook-workers") -> None:
    broker = os.getenv("KAFKA_URL")
    if not broker:
        raise RuntimeError("KAFKA_URL is not configured")

    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([topic])
    running = True

    def stop(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka consumer error: %s", message.error())
                continue
            try:
                process_message(message.value())
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                logger.exception(
                    "Failed to process event at %s[%s] offset %s; leaving it uncommitted",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                raise
    finally:
        consumer.close()
