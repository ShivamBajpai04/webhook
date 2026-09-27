import os
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

from main import app
from repositories.kafka import KafkaPublishError, publish


class FakeProducer:
    def __init__(self, error=None, remaining=0):
        self.error = error
        self.remaining = remaining
        self.callback = None

    def produce(self, *_args, **kwargs):
        self.callback = kwargs["callback"]

    def flush(self, _timeout):
        if self.remaining == 0:
            self.callback(self.error, None)
        return self.remaining


class KafkaPublishTests(unittest.TestCase):
    def test_success_requires_delivery_callback(self):
        with patch("repositories.kafka.get_producer", return_value=FakeProducer()):
            publish("webhooks", {"event": "example"}, "event-1")

    def test_broker_rejection_is_not_success(self):
        with patch("repositories.kafka.get_producer", return_value=FakeProducer(error="rejected")):
            with self.assertRaises(KafkaPublishError):
                publish("webhooks", {"event": "example"}, "event-1")

    def test_timeout_is_not_success(self):
        with patch("repositories.kafka.get_producer", return_value=FakeProducer(remaining=1)):
            with self.assertRaises(KafkaPublishError):
                publish("webhooks", {"event": "example"}, "event-1")


class WebhookRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.payload = {"event_id": str(uuid4()), "event": "example", "data": {"value": 1}}

    def test_rejects_missing_token(self):
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-secret"}):
            response = self.client.post("/api/webhook/gand_masti", json=self.payload)
        self.assertEqual(response.status_code, 401)

    def test_rejects_invalid_payload(self):
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-secret"}):
            response = self.client.post(
                "/api/webhook/gand_masti",
                json={"event": "example", "data": {}},
                headers={"X-Webhook-Token": "test-secret"},
            )
        self.assertEqual(response.status_code, 422)

    def test_rejects_oversized_event(self):
        self.payload["data"] = {"value": "x" * (64 * 1024)}
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-secret"}):
            response = self.client.post(
                "/api/webhook/gand_masti",
                json=self.payload,
                headers={"X-Webhook-Token": "test-secret"},
            )
        self.assertEqual(response.status_code, 413)

    def test_reports_kafka_failure(self):
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-secret"}):
            with patch("routers.webhooks.urls.publish", side_effect=KafkaPublishError("offline")):
                response = self.client.post(
                    "/api/webhook/gand_masti",
                    json=self.payload,
                    headers={"X-Webhook-Token": "test-secret"},
                )
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("offline", response.text)

    def test_accepts_confirmed_delivery(self):
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "test-secret"}):
            with patch("routers.webhooks.urls.publish") as send:
                response = self.client.post(
                    "/api/webhook/gand_masti",
                    json=self.payload,
                    headers={"X-Webhook-Token": "test-secret"},
                )
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["event_id"], self.payload["event_id"])
        self.assertEqual(send.call_args.kwargs["key"], self.payload["event_id"])
