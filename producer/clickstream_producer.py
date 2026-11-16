"""Synthetic e-commerce clickstream producer with controlled fault injection.

Run from the repository root:
    python -m producer.clickstream_producer --rate 10

Injected faults (each is a known, labelled scenario the pipeline must handle):
  malformed_json, missing_user_id, type_mismatch, unknown_event_type,
  negative_price      -> must land in the DLQ
  late_event          -> valid, arrives 90 s late (exercises the watermark)
  duplicate           -> same event sent twice (client retry, deduplicated)
  bot burst           -> hundreds of events from one user (BOT_SUSPECTED alert)
"""

import argparse
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

EVENT_TYPES = ("view_offer", "add_to_cart", "purchase")
EVENT_WEIGHTS = (0.75, 0.18, 0.07)  # a realistic funnel: views >> carts >> purchases
CATEGORIES = ("electronics", "fashion", "home_garden", "automotive")

# (fault name, probability) - evaluated in order on a single random roll
FAULTS = (
    ("malformed_json", 0.03),
    ("missing_user_id", 0.03),
    ("type_mismatch", 0.02),
    ("unknown_event_type", 0.01),
    ("negative_price", 0.02),
    ("late_event", 0.02),
)
DUPLICATE_PROBABILITY = 0.02


def iso_now(seconds_ago: float = 0) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


def build_event(rng: random.Random, user_id: str | None = None, category: str | None = None,
                event_type: str | None = None) -> dict:
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "user_id": user_id or f"user_{rng.randint(1, 500)}",
        "session_id": f"{rng.getrandbits(32):08x}",
        "event_type": event_type or rng.choices(EVENT_TYPES, EVENT_WEIGHTS)[0],
        "category": category or rng.choice(CATEGORIES),
        "price": round(rng.uniform(10.0, 2500.0), 2),
        "event_timestamp": iso_now(),
    }


def apply_fault(event: dict, fault: str | None) -> str:
    """Return the serialized payload with the given fault applied."""
    if fault == "malformed_json":
        return json.dumps(event)[:-15]  # truncated payload
    if fault == "missing_user_id":
        del event["user_id"]
    elif fault == "type_mismatch":
        event["price"] = "free"
    elif fault == "unknown_event_type":
        event["event_type"] = "hover"
    elif fault == "negative_price":
        event["price"] = -abs(event["price"])
    elif fault == "late_event":
        event["event_timestamp"] = iso_now(seconds_ago=90)
    return json.dumps(event)


def pick_fault(rng: random.Random) -> str | None:
    roll, cumulative = rng.random(), 0.0
    for name, probability in FAULTS:
        cumulative += probability
        if roll < cumulative:
            return name
    return None


def generate(rng: random.Random) -> tuple[str, str, str | None]:
    """Return (key, payload, fault_label) for one regular event."""
    event = build_event(rng)
    fault = pick_fault(rng)
    return event["user_id"], apply_fault(event, fault), fault


def bot_burst(rng: random.Random, size: int) -> list[tuple[str, str]]:
    bot_id, category = f"bot_{rng.randint(1, 99)}", rng.choice(CATEGORIES)
    return [
        (bot_id, json.dumps(build_event(rng, user_id=bot_id, category=category, event_type="view_offer")))
        for _ in range(size)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="clickstream-raw")
    parser.add_argument("--rate", type=float, default=10.0, help="regular events per second")
    parser.add_argument("--bot-burst-interval", type=float, default=120.0,
                        help="seconds between bot bursts (0 disables)")
    parser.add_argument("--bot-burst-size", type=int, default=400)
    parser.add_argument("--seed", type=int, default=None, help="for reproducible runs")
    return parser.parse_args()


def main() -> None:
    from kafka import KafkaProducer  # imported here so the generator is testable without Kafka

    args = parse_args()
    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        linger_ms=20,
    )
    stats: Counter[str] = Counter()
    started = last_burst = last_report = time.monotonic()

    def send(key: str, payload: str, label: str) -> None:
        producer.send(args.topic, key=key.encode(), value=payload.encode())
        stats[label] += 1

    print(f"Producing to '{args.topic}' at {args.rate}/s (Ctrl+C to stop)")
    try:
        while True:
            key, payload, fault = generate(rng)
            send(key, payload, fault or "valid")
            if fault is None and rng.random() < DUPLICATE_PROBABILITY:
                send(key, payload, "duplicate")

            now = time.monotonic()
            if args.bot_burst_interval and now - last_burst >= args.bot_burst_interval:
                for bot_key, bot_payload in bot_burst(rng, args.bot_burst_size):
                    send(bot_key, bot_payload, "bot_burst")
                last_burst = now
            if now - last_report >= 10:
                print(f"[{now - started:6.0f}s] {dict(sorted(stats.items()))}")
                last_report = now

            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        producer.close()
        print(f"Sent: {dict(sorted(stats.items()))}")


if __name__ == "__main__":
    main()
