"""Simulate Digipay transactions and produce them to Kafka (phase 1).

Phase 2 will consume the topic and write it to Iceberg (via Polaris).

Every event is a flat JSON object with a stable set of fields, so it maps
cleanly onto one Iceberg table later. Type-specific fields are null when they
do not apply. Amounts are integer IRR (rials).

Usage:
    pip install confluent-kafka
    python ingestion.py                       # 10 events/s until Ctrl+C
    python ingestion.py --rate 200 --count 50000
    python ingestion.py --dry-run --count 5   # print events, no Kafka
"""
import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

DEFAULT_BOOTSTRAP = "localhost:9094,localhost:9095"  # host listeners in docker-compose.yml
DEFAULT_TOPIC = "digipay.transactions"

# type -> (weight, (min_amount, max_amount) in IRR, failure probability)
TX_TYPES = {
    "WALLET_TOPUP":     (15, (500_000, 50_000_000), 0.03),
    "P2P_TRANSFER":     (20, (100_000, 20_000_000), 0.02),
    "QR_PAYMENT":       (20, (50_000, 10_000_000), 0.02),
    "BILL_PAYMENT":     (10, (200_000, 30_000_000), 0.04),
    "MOBILE_TOPUP":     (10, (100_000, 2_000_000), 0.03),
    "BNPL_PURCHASE":    (10, (2_000_000, 100_000_000), 0.08),
    "BNPL_REPAYMENT":   (7,  (1_000_000, 25_000_000), 0.05),
    "WITHDRAWAL":       (5,  (1_000_000, 50_000_000), 0.03),
    "REFUND":           (3,  (50_000, 20_000_000), 0.01),
}
FAILURE_REASONS = ["INSUFFICIENT_FUNDS", "BANK_TIMEOUT", "LIMIT_EXCEEDED",
                   "CARD_DECLINED", "RISK_REJECTED"]
BILL_KINDS = ["ELECTRICITY", "GAS", "WATER", "PHONE", "MUNICIPALITY", "TRAFFIC_FINE"]
OPERATORS = ["MCI", "IRANCELL", "RIGHTEL"]
MERCHANT_CATEGORIES = ["GROCERY", "RESTAURANT", "FUEL", "PHARMACY", "FASHION",
                       "ELECTRONICS", "TRANSPORT", "ONLINE_SHOP"]
CHANNELS = ["ANDROID", "IOS", "WEB", "PWA"]
CHANNEL_WEIGHTS = [55, 30, 10, 5]
BNPL_TERMS = [3, 6, 12]
CITIES = ["Tehran", "Mashhad", "Isfahan", "Shiraz", "Tabriz", "Karaj", "Qom", "Ahvaz"]


class Simulator:
    def __init__(self, users: int, merchants: int, seed: int | None):
        self.rng = random.Random(seed)
        self.users = [f"u_{i:07d}" for i in range(users)]
        # Zipf-ish skew: a few users are much more active than the rest.
        self.user_weights = [1 / (i + 1) ** 0.6 for i in range(users)]
        self.merchants = [
            {"id": f"m_{i:05d}",
             "category": self.rng.choice(MERCHANT_CATEGORIES),
             "city": self.rng.choice(CITIES)}
            for i in range(merchants)
        ]
        self.types = list(TX_TYPES)
        self.type_weights = [TX_TYPES[t][0] for t in self.types]

    def _amount(self, lo: int, hi: int) -> int:
        # Log-uniform so small payments dominate, rounded to 1000 IRR.
        value = lo * (hi / lo) ** self.rng.random()
        return max(1000, int(round(value, -3)))

    def event(self) -> dict:
        rng = self.rng
        tx_type = rng.choices(self.types, self.type_weights)[0]
        _, (lo, hi), fail_p = TX_TYPES[tx_type]
        user = rng.choices(self.users, self.user_weights)[0]
        failed = rng.random() < fail_p

        ev = {
            "transaction_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "transaction_type": tx_type,
            "status": "FAILED" if failed else "SUCCESS",
            "failure_reason": rng.choice(FAILURE_REASONS) if failed else None,
            "user_id": user,
            "counterparty_user_id": None,
            "merchant_id": None,
            "merchant_category": None,
            "city": rng.choice(CITIES),
            "amount": self._amount(lo, hi),
            "currency": "IRR",
            "fee": 0,
            "channel": rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0],
            "bill_kind": None,
            "operator": None,
            "bnpl_installments": None,
            "original_transaction_id": None,
        }

        if tx_type == "P2P_TRANSFER":
            other = rng.choice(self.users)
            while other == user:
                other = rng.choice(self.users)
            ev["counterparty_user_id"] = other
        elif tx_type in ("QR_PAYMENT", "BNPL_PURCHASE"):
            m = rng.choice(self.merchants)
            ev.update(merchant_id=m["id"], merchant_category=m["category"], city=m["city"])
            if tx_type == "BNPL_PURCHASE":
                ev["bnpl_installments"] = rng.choice(BNPL_TERMS)
            else:
                ev["fee"] = int(ev["amount"] * 0.005)  # merchant MDR
        elif tx_type == "BILL_PAYMENT":
            ev["bill_kind"] = rng.choice(BILL_KINDS)
        elif tx_type == "MOBILE_TOPUP":
            ev["operator"] = rng.choice(OPERATORS)
        elif tx_type == "WITHDRAWAL":
            ev["fee"] = 5_000
        elif tx_type == "REFUND":
            m = rng.choice(self.merchants)
            ev.update(merchant_id=m["id"], merchant_category=m["category"])
            ev["original_transaction_id"] = str(uuid.uuid4())  # simulated reference
        return ev


def ensure_topic(bootstrap: str, topic: str, partitions: int, rf: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    # Waits for the brokers; raises if the cluster is unreachable.
    md = admin.list_topics(timeout=15)
    if topic in md.topics and md.topics[topic].error is None:
        print(f"topic '{topic}' exists ({len(md.topics[topic].partitions)} partitions)")
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=rf)])[topic]
    try:
        fut.result()
        print(f"created topic '{topic}' (partitions={partitions}, rf={rf})")
    except Exception as e:  # lost a creation race is fine
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers")
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--rate", type=float, default=10, help="events per second (default 10)")
    p.add_argument("--count", type=int, default=0, help="stop after N events (0 = run until Ctrl+C)")
    p.add_argument("--users", type=int, default=10_000, help="simulated user population")
    p.add_argument("--merchants", type=int, default=500, help="simulated merchant population")
    p.add_argument("--partitions", type=int, default=6, help="partitions if the topic is created")
    p.add_argument("--replication-factor", type=int, default=2)
    p.add_argument("--seed", type=int, default=None, help="seed for reproducible data")
    p.add_argument("--dry-run", action="store_true", help="print events instead of producing")
    args = p.parse_args()

    sim = Simulator(args.users, args.merchants, args.seed)

    producer = None
    stats = {"sent": 0, "failed": 0}
    if not args.dry_run:
        try:
            from confluent_kafka import Producer
        except ImportError:
            sys.exit("missing dependency: pip install confluent-kafka")
        ensure_topic(args.bootstrap, args.topic, args.partitions, args.replication_factor)
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,   # no duplicates on retry
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
        })

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"delivery failed: {err}", file=sys.stderr)
        else:
            stats["sent"] += 1

    stop = False

    def handle_signal(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    interval = 1.0 / args.rate if args.rate > 0 else 0
    produced = 0
    start = next_tick = last_report = time.monotonic()
    print(f"producing to '{args.topic}' at ~{args.rate:g} events/s (Ctrl+C to stop)")

    while not stop and (args.count == 0 or produced < args.count):
        ev = sim.event()
        if producer is None:
            print(json.dumps(ev, ensure_ascii=False))
        else:
            # Keyed by user: all events of one user land on one partition, in order.
            while True:
                try:
                    producer.produce(args.topic, key=ev["user_id"],
                                     value=json.dumps(ev).encode(), on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full: let delivery callbacks drain it
                    producer.poll(0.5)
            producer.poll(0)
        produced += 1

        now = time.monotonic()
        if now - last_report >= 5:
            print(f"produced={produced} acked={stats['sent']} errors={stats['failed']} "
                  f"rate={produced / (now - start):.1f}/s")
            last_report = now

        next_tick += interval
        delay = next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_tick = time.monotonic()  # fell behind; don't burst to catch up

    if producer is not None:
        remaining = producer.flush(30)
        if remaining:
            print(f"warning: {remaining} messages not delivered", file=sys.stderr)
    print(f"done: produced={produced} acked={stats['sent']} errors={stats['failed']}")
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
