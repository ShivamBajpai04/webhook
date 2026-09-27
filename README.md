# Webhook ingestion example

This service accepts a JSON event, waits for Kafka's delivery report, then returns HTTP 202. The consumer validates the event and logs its ID and type before committing its offset. Logging is the only current consumer action; replace `process_message` with the actual business operation before treating the event as fully processed.

Set `KAFKA_URL`, `KAFKA_TOPIC`, and a long random `WEBHOOK_TOKEN` in `.env` before rebuilding or starting the service. Clients send that token in `X-Webhook-Token`. An event must contain a UUID `event_id`, a nonempty `event` name, and a `data` object. The serialized event may be at most 64 KiB. The event ID is used as the Kafka key; it does not, by itself, prevent duplicate processing.

Once deployed, a request looks like this (substitute the configured token):

```sh
curl -X POST http://100.124.143.7:8000/api/webhook/gand_masti \
  -H 'Content-Type: application/json' \
  -H 'X-Webhook-Token: YOUR_TOKEN' \
  -d '{"event_id":"95c44c2a-43a1-49e7-a8e0-a4897e793ddb","event":"gand_masti","data":{"hello":123}}'
```

Run the unit checks with `python -m unittest discover -s tests`. They cover delivery success, rejection, timeout, authentication, payload validation, and HTTP failure mapping.

The delivery wait has a ten-second limit. A timeout means the outcome is unknown: Kafka may still deliver the event later. Callers should retry with the same event ID, and any real consumer operation should be idempotent. A malformed Kafka event currently stops the consumer without committing it so it can be investigated rather than silently skipped.
