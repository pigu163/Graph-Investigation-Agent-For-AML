"""Exercise queries against transactions replayed through the real pipeline.

Set AML_TEST_DATABASE_URL to a disposable PostgreSQL database containing both
the 5,000-row LI sample and tests/fixtures/streaming-boundaries.csv.
"""

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from graph_investigation_agent_for_aml.investigation import (
    get_account_profile,
    get_counterparties,
    get_transaction_details,
    get_transactions,
)


@pytest.fixture
def connection():
    dsn = os.environ.get("AML_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set AML_TEST_DATABASE_URL for PostgreSQL integration tests")
    with psycopg.connect(dsn) as conn:
        yield conn
        # Test-created rows never persist; pipeline-loaded rows remain for cleanup.
        conn.rollback()


def test_known_movements_evidence_and_filters(connection):
    start, end = datetime(2022, 9, 1), datetime(2022, 9, 1, 2)
    args = (connection, "streaming-boundaries", "02", "X", start, end)

    profile = get_account_profile(*args)
    directions = {row["direction"]: row for row in profile["result"]["directions"]}
    assert directions["in"]["transaction_count"] == 6
    assert directions["in"]["distinct_counterparty_count"] == 3
    assert directions["out"]["transaction_count"] == 2
    assert directions["out"]["distinct_counterparty_count"] == 1
    amounts = {(row["direction"], row["currency"]): Decimal(row["amount_sum"])
               for row in profile["result"]["currencies"]}
    assert amounts == {("in", "US Dollar"): Decimal("39.00000001"),
                       ("in", "Euro"): Decimal("2.00"),
                       ("out", "US Dollar"): Decimal("12.00")}

    transactions = get_transactions(*args, limit=2)
    assert len(transactions["result"]) == 2
    assert transactions["pagination"]["has_more"] is True
    remaining = get_transactions(*args, limit=10, offset=2)
    combined = transactions["result"] + remaining["result"]
    assert len(combined) == 7
    assert len({row["transaction_id"] for row in combined}) == 7
    assert [row["direction"] for row in combined].count("self") == 1
    assert len(get_transactions(*args, direction="in")["result"]) == 6
    assert len(get_transactions(*args, direction="out")["result"]) == 2
    assert len(get_transactions(*args, currency="Euro")["result"]) == 1
    assert len(get_transactions(connection, "streaming-boundaries", "02", "X", start,
                                datetime(2022, 9, 1, 1))["result"]) == 3
    assert get_transactions(connection, "streaming-boundaries", "99", "X", start, end)["result"] == []
    assert get_transactions(connection, "streaming-boundaries", "02' OR true --", "X", start, end)["result"] == []

    counterparties = get_counterparties(*args)["result"]
    assert {(row["direction"], row["counterparty_account"], row["currency"]) for row in counterparties} == {
        ("in", "A", "US Dollar"), ("in", "B", "US Dollar"),
        ("in", "D", "Euro"), ("out", "C", "US Dollar"),
    }
    a = next(row for row in counterparties if row["counterparty_account"] == "A")
    assert a["transaction_count"] == 3
    assert Decimal(a["amount_sum"]) == Decimal("12.00000001")
    assert len(a["transaction_ids"]) == 3
    assert get_counterparties(*args, limit=1)["pagination"]["has_more"] is True

    details = get_transaction_details(connection, "streaming-boundaries", "streaming-boundaries:6")
    assert details["result"]["source_row"] == 6
    assert details["result"]["amount_paid"] == "3.00"
    assert details["result"]["amount_received"] == "2.00"
    assert details["result"]["payment_currency"] == "US Dollar"
    assert details["result"]["receiving_currency"] == "Euro"
    assert get_transaction_details(connection, "other-dataset", "streaming-boundaries:6")["result"] is None
    for output in (profile, transactions, counterparties, details):
        encoded = json.dumps(output)
        assert "is_laundering" not in encoded
        assert "raw_event" not in encoded
    with pytest.raises(ValueError):
        get_transactions(*args, limit=1001)
    with pytest.raises(ValueError):
        get_transactions(connection, "streaming-boundaries", "02", "X", end, start)


def test_evidence_limit_and_actual_ibm_records(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, source_dataset, source_file, source_row, event_time_kind,
                event_time, source_bank, source_account, destination_bank, destination_account,
                amount_received, receiving_currency, amount_paid, payment_currency, payment_format,
                is_laundering, ingested_at, kafka_topic, kafka_partition, kafka_offset, raw_event
            )
            SELECT 'evidence-test:' || n, 'evidence-test', source_file, n, event_time_kind,
                event_time, source_bank, source_account, destination_bank, destination_account,
                amount_received, receiving_currency, amount_paid, payment_currency, payment_format,
                is_laundering, ingested_at, kafka_topic, kafka_partition, kafka_offset, raw_event
            FROM transactions CROSS JOIN generate_series(1, 25) AS n
            WHERE transaction_id = 'streaming-boundaries:1'
        """)
    start, end = datetime(2022, 9, 1), datetime(2022, 9, 2)
    args = (connection, "evidence-test", "02", "X", start, end)
    row = get_counterparties(*args)["result"][0]
    assert row["transaction_count"] == 25
    assert len(row["transaction_ids"]) == 20
    assert row["evidence_truncated"] is True
    group = get_account_profile(*args)["result"]["currencies"][0]
    assert group["transaction_count"] == 25
    assert len(group["transaction_ids"]) == 20
    assert group["evidence_truncated"] is True

    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM transactions WHERE source_dataset = %s", ("LI-Small_Trans",))
        assert cursor.fetchone()[0] == 5000
        cursor.execute("""SELECT transaction_id, source_bank, source_account, event_time
                          FROM transactions WHERE source_dataset = %s ORDER BY source_row LIMIT 1""",
                       ("LI-Small_Trans",))
        transaction_id, bank, account, event_time = cursor.fetchone()
    details = get_transaction_details(connection, "LI-Small_Trans", transaction_id)
    assert details["result"]["transaction_id"] == transaction_id
    result = get_transactions(connection, "LI-Small_Trans", bank, account,
                              event_time, event_time + timedelta(minutes=1), direction="out")
    assert transaction_id in {item["transaction_id"] for item in result["result"]}
