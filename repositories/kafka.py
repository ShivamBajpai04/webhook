import json
import os
import signal
import sys

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "webhooks")

producer = Producer(
    {
        "bootstrap.servers": os.getenv("KAFKA_URL"),
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", flush=True)
    else:
        print(
            f"delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}",
            flush=True,
        )


def publish(topic: str, payload: dict, key: str | None = None):
    producer.produce(
        topic,
        key=key.encode() if key else None,
        value=json.dumps(payload).encode(),
        callback=delivery_report,
    )
    producer.poll(0)
    producer.flush()


def consume(topic: str = DEFAULT_TOPIC, group_id: str = "webhook-workers"):
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_URL"),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])
    print(f"consuming topic={topic} group={group_id}", flush=True)

    running = True

    def _stop(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"error: {message.error()}", flush=True)
                continue

            key = message.key().decode() if message.key() else None
            try:
                value = json.loads(message.value().decode())
            except (TypeError, json.JSONDecodeError):
                value = message.value().decode() if message.value() else None

            print(
                json.dumps(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "key": key,
                        "value": value,
                    }
                ),
                flush=True,
            )
    finally:
        consumer.close()
        print("consumer closed", flush=True)


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TOPIC
    consume(topic)
