"""Run a bounded load test against the webhook endpoint."""

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv(
            "WEBHOOK_URL", "http://100.124.143.7:8000/api/webhook/gand_masti"
        ),
    )
    parser.add_argument("--requests", type=positive_int, default=100)
    parser.add_argument("--concurrency", type=positive_int, default=5)
    parser.add_argument("--timeout", type=positive_float, default=15.0)
    args = parser.parse_args()

    if args.requests > 10_000 or args.concurrency > 100:
        parser.error("limits are 10,000 requests and 100 concurrent workers")
    token = os.getenv("WEBHOOK_TOKEN")
    if not token:
        parser.error("set WEBHOOK_TOKEN in the environment before running")

    def send_one(sequence: int) -> tuple[int | str, float]:
        payload = {
            "event_id": str(uuid4()),
            "event": "stress_test",
            "data": {"sequence": sequence},
        }
        request = Request(
            args.url,
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Token": token,
            },
            method="POST",
        )
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=args.timeout) as response:
                status: int | str = response.status
        except HTTPError as error:
            status = error.code
        except (URLError, TimeoutError, OSError):
            status = "network_error"
        return status, time.perf_counter() - started

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(send_one, range(args.requests)))
    elapsed = time.perf_counter() - started

    statuses = Counter(status for status, _ in results)
    latencies_ms = [duration * 1000 for _, duration in results]
    accepted = statuses[202]
    print(f"sent={args.requests} accepted={accepted} failed={args.requests - accepted}")
    print(f"status_codes={dict(statuses)}")
    print(f"elapsed={elapsed:.2f}s throughput={args.requests / elapsed:.1f} requests/s")
    print(
        "latency_ms "
        f"mean={statistics.mean(latencies_ms):.1f} "
        f"p50={percentile(latencies_ms, 0.50):.1f} "
        f"p95={percentile(latencies_ms, 0.95):.1f} "
        f"p99={percentile(latencies_ms, 0.99):.1f}"
    )
    return 0 if accepted == args.requests else 1


if __name__ == "__main__":
    sys.exit(main())
