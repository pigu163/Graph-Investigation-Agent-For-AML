"""Read Kafka transactions and save them to PostgreSQL.

Start from the repository root:
    docker compose up -d --build raw-writer

Inspect saved transactions:
    docker compose exec postgres psql -U aml_user -d aml_db \
      -c "SELECT count(*) FROM transactions;"
"""

import json
import logging
import os
import signal
from datetime import datetime
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer, KafkaException
from psycopg.types.json import Jsonb


# Source event timestamps have no timezone. Keep them as supplied by the dataset.
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    source_bank TEXT NOT NULL,
    source_account TEXT NOT NULL,
    destination_bank TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount_received NUMERIC NOT NULL CHECK (amount_received >= 0),
    receiving_currency TEXT NOT NULL,
    amount_paid NUMERIC NOT NULL CHECK (amount_paid >= 0),
    payment_currency TEXT NOT NULL,
    payment_format TEXT NOT NULL,
    is_laundering SMALLINT NOT NULL CHECK (is_laundering IN (0, 1)),
    ingested_at TIMESTAMPTZ NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    raw_event JSONB NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

# Fixed, parameterized SQL keeps event values separate from SQL instructions.
# Duplicate replays keep the first stored record and its original provenance.
INSERT_TRANSACTION = """
INSERT INTO transactions (
    transaction_id, event_time, source_bank, source_account,
    destination_bank, destination_account, amount_received,
    receiving_currency, amount_paid, payment_currency, payment_format,
    is_laundering, ingested_at, kafka_topic, kafka_partition, kafka_offset,
    raw_event
) VALUES (
    %(transaction_id)s, %(event_time)s, %(source_bank)s, %(source_account)s,
    %(destination_bank)s, %(destination_account)s, %(amount_received)s,
    %(receiving_currency)s, %(amount_paid)s, %(payment_currency)s,
    %(payment_format)s, %(is_laundering)s, %(ingested_at)s,
    %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s, %(raw_event)s
)
ON CONFLICT (transaction_id) DO NOTHING
"""


def transaction_values(event: dict) -> dict:
    """Validate the message and convert JSON values to database values."""
    values = dict(event)
    for name in (
        "transaction_id", "source_bank", "source_account", "destination_bank",
        "destination_account", "receiving_currency", "payment_currency",
        "payment_format",
    ):
        if not isinstance(event[name], str) or not event[name].strip():
            raise ValueError(f"{name} must be a nonempty string")

    for name in ("amount_received", "amount_paid"):
        # Decimal preserves the exact amount instead of using binary floats.
        if not isinstance(event[name], str):
            raise ValueError(f"{name} must be a decimal string")
        amount = Decimal(event[name])
        if not amount.is_finite() or amount < 0:
            raise ValueError(f"{name} must be finite and nonnegative")
        values[name] = amount

    if type(event["is_laundering"]) is not int or event["is_laundering"] not in (0, 1):
        raise ValueError("is_laundering must be 0 or 1")
    values["event_time"] = datetime.strptime(event["event_time"], "%Y/%m/%d %H:%M")
    values["ingested_at"] = datetime.fromisoformat(event["ingested_at"])
    if values["ingested_at"].utcoffset() is None:
        raise ValueError("ingested_at must include a timezone")
    values["raw_event"] = Jsonb(event)
    return values


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    topic = os.getenv("KAFKA_TOPIC", "aml-transactions")
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "aml-raw-writer-v1"),
        # A new consumer group also reads messages already present in Kafka.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    running = True
    processed = inserted = 0

    def stop(signum: int, frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        # Psycopg reads PGHOST, PGDATABASE, PGUSER and PGPASSWORD from the environment.
        with psycopg.connect(autocommit=True, connect_timeout=10) as connection:
            connection.execute(CREATE_TABLE)
            consumer.subscribe([topic])
            logging.info("Reading %s into PostgreSQL transactions", topic)
            while running:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())

                try:
                    event = json.loads(message.value())
                    if not isinstance(event, dict):
                        raise ValueError("The Kafka message must be a JSON object")
                    values = transaction_values(event)
                    values.update(
                        kafka_topic=message.topic(),
                        kafka_partition=message.partition(),
                        kafka_offset=message.offset(),
                    )
                    # ponytail: one transaction per message; batch if throughput needs it.
                    with connection.transaction():
                        result = connection.execute(INSERT_TRANSACTION, values)
                    inserted += result.rowcount

                    # Confirm Kafka progress only after PostgreSQL has committed.
                    # A crash between these steps may replay a row; the primary key deduplicates it.
                    for partition in consumer.commit(message=message, asynchronous=False):
                        if partition.error:
                            raise KafkaException(partition.error)
                except Exception:
                    # Stop on bad data or failed writes; never silently skip the message.
                    logging.exception(
                        "Failed at %s partition %s offset %s",
                        message.topic(), message.partition(), message.offset(),
                    )
                    raise

                processed += 1
                if processed % 1000 == 0:
                    logging.info("Processed %s messages; inserted %s rows", processed, inserted)
    finally:
        consumer.close()
        logging.info("Stopped: processed %s messages; inserted %s rows", processed, inserted)


if __name__ == "__main__":
    main()
