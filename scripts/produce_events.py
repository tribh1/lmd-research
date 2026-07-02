"""Clickstream event producer for the streaming analytics pipeline (Experiment 3).

Publishes synthetic application events to the app-events Kafka topic at a
configurable rate. Each event embeds its creation timestamp (event_time, ms
precision) so the streaming consumer can measure true end-to-end latency from
event creation to Iceberg visibility (thesis Section 2.7).

    python scripts/produce_events.py --bootstrap localhost:9092 \
        --rate 5000 --duration 300
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

EVENT_TYPES = ["page_view", "click", "add_to_cart", "checkout", "search"]
CHANNELS = ["web", "mobile", "agent", "api"]
PAGES = ["/home", "/product", "/cart", "/checkout", "/search", "/promo"]


def make_event(customer_max: int) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "customer_id": random.randint(1, customer_max),
        "session_id": str(uuid.uuid4())[:8],
        "event_type": random.choice(EVENT_TYPES),
        "channel": random.choice(CHANNELS),
        "page": random.choice(PAGES),
        "amount": round(random.uniform(0, 2000000), 2) if random.random() < 0.1 else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="app-events")
    ap.add_argument("--rate", type=int, default=5000, help="target events per second")
    ap.add_argument("--duration", type=int, default=300, help="seconds to run (thesis: 5-minute window)")
    ap.add_argument("--customer-max", type=int, default=10000)
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap,
                         "linger.ms": 5, "batch.num.messages": 10000})
    sent = 0
    start = time.time()
    end = start + args.duration
    # Send in 100ms slices to hold the target rate without busy-waiting.
    slice_size = max(1, args.rate // 10)
    while time.time() < end:
        slice_start = time.time()
        for _ in range(slice_size):
            producer.produce(args.topic, json.dumps(make_event(args.customer_max)).encode())
            sent += 1
        producer.poll(0)
        sleep_left = 0.1 - (time.time() - slice_start)
        if sleep_left > 0:
            time.sleep(sleep_left)
    producer.flush(30)
    elapsed = time.time() - start
    print({"sent": sent, "elapsed_sec": round(elapsed, 1),
           "actual_rate_per_sec": round(sent / elapsed, 1)})


if __name__ == "__main__":
    main()
