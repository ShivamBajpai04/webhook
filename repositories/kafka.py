import json
import os

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

producer = Producer(
    {
        "bootstrap.servers": os.getenv("KAFKA_URL", "localhost:9092"),
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def publish(topic: str, payload: dict, key: str | None = None):
    producer.produce(
        topic,
        key=key.encode() if key else None,
        value=json.dumps(payload).encode(),
        callback=delivery_report,
    )
    producer.poll(0)
    producer.flush()
