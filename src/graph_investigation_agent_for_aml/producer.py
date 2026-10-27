"""Replay IBM AML Small transaction CSV files to Kafka."""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaError, Producer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, type=Path, help="Path to an IBM AML transaction CSV")
    parser.add_argument("--topic", default="aml-transactions")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=100, help="Transactions per second")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to send")
    args = parser.parse_args()
    if args.rate <= 0 or (args.limit is not None and args.limit <= 0):
        parser.error("--rate and --limit must be greater than zero")

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    delivered = 0
    failures: list[str] = []
    interval = 1 / args.rate
    next_send = time.monotonic()

    def on_delivery(error: KafkaError | None, message: object) -> None:
        if error is not None:
            failures.append(str(error))

    # The repeated Account header is mapped by column position to preserve both parties.
    with args.file.open(newline="", encoding="utf-8") as source:
        rows = csv.reader(source)
        header = next(rows, None)
        if header is None or len(header) != 11:
            raise ValueError("Expected an IBM AML transaction CSV with 11 columns")

        for row_number, row in enumerate(rows, start=1):
            if len(row) != 11:
                raise ValueError(f"Unexpected column count at CSV row {row_number + 1}")

            event = {
                "transaction_id": f"{args.file.stem}:{row_number}",
                "event_time": row[0],
                "source_bank": row[1],
                "source_account": row[2],
                "destination_bank": row[3],
                "destination_account": row[4],
                "amount_received": row[5],
                "receiving_currency": row[6],
                "amount_paid": row[7],
                "payment_currency": row[8],
                "payment_format": row[9],
                "is_laundering": int(row[10]),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(
                args.topic,
                key=event["transaction_id"],
                value=json.dumps(event).encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            delivered += 1
            if args.limit is not None and delivered >= args.limit:
                break

            # Keep a steady rate while retaining the CSV's original row order.
            next_send += interval
            time.sleep(max(0, next_send - time.monotonic()))

    remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(f"Kafka delivery failed: {remaining} messages pending; {failures[:3]}")
    print(f"Delivered {delivered} transactions to {args.topic}")


if __name__ == "__main__":
    main()
