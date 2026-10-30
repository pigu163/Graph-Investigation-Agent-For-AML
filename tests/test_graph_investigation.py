"""Local graph cases plus opt-in tests against a disposable PostgreSQL database."""

import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from graph_investigation_agent_for_aml.graph_investigation import (
    compute_graph_features,
    detect_cycles,
    detect_fan_in,
    detect_fan_out,
    detect_rapid_pass_through,
    detect_reciprocal_flows,
    expand_transaction_graph,
    trace_fund_flow,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/graph-investigation.csv"
START = datetime(2022, 9, 1, 9)
END = datetime(2022, 9, 1, 10)


@pytest.fixture
def graph_data():
    with FIXTURE.open(newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))[1:]
    transactions = []
    nodes = {("01", "A")}
    for number, row in enumerate(rows[:-1], 1):
        event_time, from_bank, from_account, to_bank, to_account, received, rc, paid, pc, fmt, _ = row
        nodes.update(((from_bank, from_account), (to_bank, to_account)))
        transactions.append({"transaction_id": f"graph-investigation:{number}",
                             "source_dataset": "graph-investigation", "source_bank": from_bank,
                             "source_account": from_account, "destination_bank": to_bank,
                             "destination_account": to_account,
                             "event_time": datetime.strptime(event_time, "%Y/%m/%d %H:%M").isoformat(),
                             "amount_received": received, "receiving_currency": rc,
                             "amount_paid": paid, "payment_currency": pc, "payment_format": fmt})
    return {"query": {"direction": "all", "dataset": "graph-investigation"},
            "scope": {"target": {"bank_id": "01", "account_id": "A"}, "truncated": False},
            "result": {"nodes": [{"bank_id": b, "account_id": a} for b, a in sorted(nodes)],
                       "transactions": transactions}}


def test_graph_features_fan_and_parallel_transactions(graph_data):
    features = compute_graph_features(graph_data)["result"]
    assert features["in_transaction_count"] == 5
    assert features["out_transaction_count"] == 3
    assert features["in_neighbor_count"] == 4
    assert features["out_neighbor_count"] == 1
    assert features["mutual_neighbor_count"] == 1
    assert "graph-investigation:1" in features["in_transaction_ids"]
    assert "graph-investigation:2" in features["out_transaction_ids"]
    assert detect_fan_in(graph_data, min_counterparties=4)["result"]["detected"] is True
    assert detect_fan_out(graph_data, min_counterparties=2)["result"]["detected"] is False
    fan = detect_fan_out(graph_data)["result"]
    assert len(fan["transactions"]) == 2
    assert {row["transaction_id"] for row in fan["transactions"]} == {
        "graph-investigation:2", "graph-investigation:3"}
    graph_data["scope"]["truncated"] = True
    assert detect_fan_out(graph_data)["result"]["detected"] is None
    graph_data["query"]["direction"] = "out"
    with pytest.raises(ValueError):
        detect_fan_in(graph_data)


def test_temporal_paths_cycles_pairs_and_limits(graph_data):
    paths = trace_fund_flow(graph_data, max_hops=3)["result"]
    assert any([row["transaction_id"] for row in item["transactions"]] ==
               ["graph-investigation:2", "graph-investigation:5"] and
               item["equal_timestamp_order_unknown"] for item in paths)
    cycles = detect_cycles(graph_data, max_hops=3)["result"]
    assert any([row["transaction_id"] for row in item["transactions"]] ==
               ["graph-investigation:1", "graph-investigation:2", "graph-investigation:5"] and
               item["temporal_status"] == "chronological" and item["equal_timestamp_order_unknown"]
               for item in cycles)
    assert any(len(item["transactions"]) == 2 for item in cycles)
    reciprocal = detect_reciprocal_flows(graph_data, max_delay_seconds=600)["result"]
    assert len(reciprocal) == 2
    assert all(item["time_difference_seconds"] == -120 for item in reciprocal)
    rapid = detect_rapid_pass_through(graph_data, max_delay_seconds=600)["result"]
    assert len(rapid) == 2
    assert all(item["amount_comparison"]["currency_comparable"] for item in rapid)
    assert trace_fund_flow(graph_data, max_results=1)["search_truncated"] is True
    assert detect_rapid_pass_through(graph_data, max_states=1)["search_truncated"] is True
    encoded = json.dumps([paths, cycles, reciprocal, rapid])
    assert "is_laundering" not in encoded
    assert "raw_event" not in encoded

    # A graph connection is not a chronological flow when B's payment precedes A's.
    graph_data["result"]["transactions"] = [row for row in graph_data["result"]["transactions"]
                                             if row["transaction_id"] in {
                                                 "graph-investigation:2", "graph-investigation:5"}]
    graph_data["result"]["transactions"][1]["event_time"] = "2022-09-01T09:04:00"
    assert trace_fund_flow(graph_data)["result"] == []


def test_structural_cycle_can_fail_timing(graph_data):
    times = {"graph-investigation:1": "2022-09-01T09:10:00",
             "graph-investigation:2": "2022-09-01T09:05:00",
             "graph-investigation:5": "2022-09-01T09:00:00"}
    graph_data["result"]["transactions"] = [row for row in graph_data["result"]["transactions"]
                                             if row["transaction_id"] in times]
    for row in graph_data["result"]["transactions"]:
        row["event_time"] = times[row["transaction_id"]]
    cycles = detect_cycles(graph_data)["result"]
    assert len(cycles) == 1
    assert cycles[0]["temporal_status"] == "not_chronological"
    assert trace_fund_flow(graph_data)["result"] == []


def test_currency_mismatch_and_equal_timestamp_are_explicit(graph_data):
    for row in graph_data["result"]["transactions"]:
        if row["transaction_id"] == "graph-investigation:2":
            row["event_time"] = "2022-09-01T09:07:00"
    pairs = detect_rapid_pass_through(graph_data, max_delay_seconds=600)["result"]
    cross_currency = next(item for item in pairs
                          if item["incoming"]["transaction_id"] == "graph-investigation:6"
                          and item["outgoing"]["transaction_id"] == "graph-investigation:2")
    assert cross_currency["amount_comparison"]["currency_comparable"] is False
    assert cross_currency["amount_comparison"]["outgoing_not_greater_than_incoming"] is None
    simultaneous = next(item for item in pairs
                        if item["incoming"]["transaction_id"] == "graph-investigation:4"
                        and item["outgoing"]["transaction_id"] == "graph-investigation:2")
    assert simultaneous["equal_timestamp_order_unknown"] is True


@pytest.fixture(scope="module")
def postgres_connection():
    dsn = os.environ.get("AML_GRAPH_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set AML_GRAPH_TEST_DATABASE_URL to the disposable test database")
    connection = psycopg.connect(dsn)
    if connection.info.dbname != "aml_graph_test":
        connection.close()
        pytest.fail("Graph integration tests require the disposable aml_graph_test database")
    from graph_investigation_agent_for_aml.producer import build_event, read_rows
    from graph_investigation_agent_for_aml.raw_writer import (
        CREATE_TABLE,
        INSERT_TRANSACTION,
        transaction_values,
    )

    connection.execute(CREATE_TABLE)
    fixture_rows = list(read_rows(FIXTURE, None, False))
    real_file = ROOT / "dataset/LI-Small/LI-Small_Trans.csv"
    sources = [(FIXTURE, fixture_rows)]
    if real_file.exists():
        sources.append((real_file, read_rows(real_file, 1000, False)))
    with connection.cursor() as cursor:
        for path, rows in sources:
            values = []
            for number, row in rows:
                event = transaction_values(build_event(path, number, row))
                event.update(kafka_topic="synthetic-fixture", kafka_partition=0, kafka_offset=number)
                values.append(event)
            cursor.executemany(INSERT_TRANSACTION, values)
    connection.commit()
    yield connection
    connection.close()


def test_database_expansion_bounds_direction_and_evidence(postgres_connection):
    args = (postgres_connection, "graph-investigation", "01", "A", START, END)
    one = expand_transaction_graph(*args, hops=1)
    assert one["scope"]["truncated"] is False
    assert one["scope"]["transaction_count"] == 7
    assert one["scope"]["node_count"] == 5
    ids = {row["transaction_id"] for row in one["result"]["transactions"]}
    assert "graph-investigation:2" in ids and "graph-investigation:3" in ids
    assert "graph-investigation:5" not in ids
    two = expand_transaction_graph(*args, hops=2)
    assert two["scope"]["transaction_count"] == 11
    assert two["scope"]["node_count"] == 7
    assert {"bank_id": "02", "account_id": "A"} in two["result"]["nodes"]
    assert "graph-investigation:12" not in {row["transaction_id"] for row in two["result"]["transactions"]}
    assert len(detect_cycles(two)["result"]) >= 3
    assert trace_fund_flow(two)["result"]
    assert "is_laundering" not in json.dumps(two)
    assert "raw_event" not in json.dumps(two)
    assert expand_transaction_graph(*args, max_edges=1)["scope"]["truncation_reason"] == "max_edges"
    assert expand_transaction_graph(*args, max_nodes=1)["scope"]["truncation_reason"] == "max_nodes"
    assert len(expand_transaction_graph(postgres_connection, "graph-investigation", "01", "A",
                                        START, datetime(2022, 9, 1, 9, 5), hops=1)
               ["result"]["transactions"]) == 1
    outgoing = expand_transaction_graph(*args, hops=1, direction="out")
    assert outgoing["scope"]["transaction_count"] == 3
    incoming = expand_transaction_graph(*args, hops=1, direction="in")
    assert incoming["scope"]["transaction_count"] == 5
    with pytest.raises(ValueError):
        compute_graph_features(outgoing)
    assert expand_transaction_graph(postgres_connection, "graph-investigation", "99", "A", START, END)["scope"]["transaction_count"] == 0
    assert expand_transaction_graph(postgres_connection, "other", "01", "A", START, END)["scope"]["transaction_count"] == 0
    with pytest.raises(ValueError):
        expand_transaction_graph(*args, hops=6)


def test_database_expansion_real_sample(postgres_connection):
    with postgres_connection.cursor() as cursor:
        cursor.execute("""SELECT source_bank, source_account, event_time, transaction_id
                          FROM transactions WHERE source_dataset = 'LI-Small_Trans'
                          ORDER BY source_row LIMIT 1""")
        row = cursor.fetchone()
    if row is None:
        pytest.skip("LI-Small CSV is not present locally")
    bank, account, event_time, transaction_id = row
    graph = expand_transaction_graph(postgres_connection, "LI-Small_Trans", bank, account,
                                     event_time, event_time + timedelta(minutes=1), hops=1)
    assert transaction_id in {item["transaction_id"] for item in graph["result"]["transactions"]}


def test_optional_account_indexes(postgres_connection):
    postgres_connection.execute((ROOT / "investigation/indexes.sql").read_text())
    rows = postgres_connection.execute("""SELECT indexname FROM pg_indexes
                                          WHERE tablename = 'transactions'
                                          AND indexname LIKE 'idx_transactions_%'""").fetchall()
    assert {row[0] for row in rows} >= {"idx_transactions_source_time",
                                        "idx_transactions_destination_time"}
    plan = postgres_connection.execute("""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT transaction_id FROM transactions
        WHERE source_dataset = %s AND source_bank = %s AND source_account = %s
          AND event_time >= %s AND event_time < %s""",
        ("graph-investigation", "01", "A", START, END)).fetchone()[0]
    assert "Plan" in plan[0]
    postgres_connection.rollback()
