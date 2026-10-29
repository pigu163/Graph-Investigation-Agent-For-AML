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
    source_dataset TEXT,
    source_file TEXT,
    source_row BIGINT,
    event_time_kind TEXT,
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

# Existing rows keep their original JSON; new messages fill these columns.
ADD_SOURCE_COLUMNS = """
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source_dataset TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source_row BIGINT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS event_time_kind TEXT;
"""

# Retain the first conflicting message at each Kafka position for investigation.
CREATE_CONFLICT_TABLE = """
CREATE TABLE IF NOT EXISTS transaction_conflicts (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    transaction_id TEXT NOT NULL,
    original_event JSONB NOT NULL,
    conflicting_event JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
)
"""

BUSINESS_FIELDS = (
    "source_dataset", "source_file", "source_row", "event_time_kind", "event_time",
    "source_bank", "source_account", "destination_bank", "destination_account",
    "amount_received", "receiving_currency", "amount_paid", "payment_currency",
    "payment_format", "is_laundering",
)


def business_values(values: dict) -> tuple:
    """Exclude ingestion metadata when comparing two validated transactions."""
    return tuple(values[field] for field in BUSINESS_FIELDS)

# Fixed, parameterized SQL keeps event values separate from SQL instructions.
# Duplicate replays keep the first stored record and its original provenance.
INSERT_TRANSACTION = """
INSERT INTO transactions (
    transaction_id, event_time, source_bank, source_account,
    destination_bank, destination_account, amount_received,
    receiving_currency, amount_paid, payment_currency, payment_format,
    is_laundering, ingested_at, kafka_topic, kafka_partition, kafka_offset,
    raw_event, source_dataset, source_file, source_row, event_time_kind
) VALUES (
    %(transaction_id)s, %(event_time)s, %(source_bank)s, %(source_account)s,
    %(destination_bank)s, %(destination_account)s, %(amount_received)s,
    %(receiving_currency)s, %(amount_paid)s, %(payment_currency)s,
    %(payment_format)s, %(is_laundering)s, %(ingested_at)s,
    %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s, %(raw_event)s,
    %(source_dataset)s, %(source_file)s, %(source_row)s, %(event_time_kind)s
)
ON CONFLICT (transaction_id) DO NOTHING
"""


def transaction_values(event: dict) -> dict:
    """Validate the message and convert JSON values to database values."""
    values = dict(event)
    for name in (
        "transaction_id", "source_bank", "source_account", "destination_bank",
        "destination_account", "receiving_currency", "payment_currency",
        "payment_format", "source_dataset", "source_file",
    ):
        if not isinstance(event[name], str) or not event[name].strip():
            raise ValueError(f"{name} must be a nonempty string")

    if type(event["source_row"]) is not int or event["source_row"] < 1:
        raise ValueError("source_row must be a positive integer")
    if event["transaction_id"] != f"{event['source_dataset']}:{event['source_row']}":
        raise ValueError("transaction_id must match source_dataset and source_row")
    if event["event_time_kind"] != "local_unknown_timezone":
        raise ValueError("IBM event_time_kind must be local_unknown_timezone")

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
            connection.execute(ADD_SOURCE_COLUMNS)
            connection.execute(CREATE_CONFLICT_TABLE)
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
                        if result.rowcount == 0:
                            row = connection.execute(
                                "SELECT " + ", ".join(BUSINESS_FIELDS) + ", raw_event "
                                "FROM transactions WHERE transaction_id = %s",
                                (values["transaction_id"],),
                            ).fetchone()
                            if row is None:
                                raise RuntimeError("Duplicate transaction disappeared during comparison")
                            if row[:-1] != business_values(values):
                                connection.execute(
                                    """INSERT INTO transaction_conflicts
                                       (kafka_topic, kafka_partition, kafka_offset, transaction_id,
                                        original_event, conflicting_event)
                                       VALUES (%s, %s, %s, %s, %s, %s)
                                       ON CONFLICT DO NOTHING""",
                                    (message.topic(), message.partition(), message.offset(),
                                     values["transaction_id"], Jsonb(row[-1]), values["raw_event"]),
                                )
                                logging.warning("Conflicting transaction ID %s at %s/%s/%s",
                                                values["transaction_id"], message.topic(),
                                                message.partition(), message.offset())
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
