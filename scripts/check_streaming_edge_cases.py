"""Exercise lateness and duplicate handling in a separate Compose project.

Run: uv run --extra streaming python scripts/check_streaming_edge_cases.py
Requires aml-python-datastream:dev and aml-flink-datastream:dev images and free
ports 5432, 8081 and 9092. On success, isolated test data is cleared and
containers are stopped, not removed. On failure, data is retained for diagnosis.
"""

import json
import subprocess
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import psycopg
from confluent_kafka import Producer

from graph_investigation_agent_for_aml.producer import build_event


ROOT = Path(__file__).resolve().parents[1]


def main():
    directory = Path(tempfile.mkdtemp(prefix="aml-streaming-edge-", dir="/tmp"))
    project = "aml-edge-" + uuid.uuid4().hex[:10]
    override = directory / "compose.override.json"
    override.write_text(json.dumps({"services": {
        "kafka": {"environment": {"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1"}},
        "raw-writer": {"image": "aml-python-datastream:dev"},
        "flink-jobmanager": {"image": "aml-flink-datastream:dev"},
        "flink-taskmanager": {"image": "aml-flink-datastream:dev"},
        "flink-state-init": {"image": "aml-flink-datastream:dev"},
        "flink-submit": {"image": "aml-flink-datastream:dev"},
    }}))
    command = ["docker", "compose", "--project-name", project,
               "--project-directory", str(directory), "-f", str(ROOT / "compose.yaml"),
               "-f", str(override)]
    print(f"Isolated project: {project}; data directory: {directory}", flush=True)

    def compose(*args, input=None):
        return subprocess.run(command + list(args), input=input, check=True, text=True,
                              timeout=180, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    producer = None
    conn = None
    try:
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120",
                "kafka", "postgres", "raw-writer", "flink-jobmanager", "flink-taskmanager")
        compose("exec", "-T", "postgres", "psql", "-U", "aml_user", "-d", "aml_db",
                "-v", "ON_ERROR_STOP=1", "-f", "-", input=(ROOT / "flink/schema.sql").read_text())
        compose("run", "--rm", "--no-deps", "flink-submit")
        conn = psycopg.connect("host=localhost port=5432 dbname=aml_db user=aml_user password=aml_local_dev", autocommit=True)
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        def send(row_number, timestamp, account, amount="100"):
            row = [timestamp, "S", "sender", "B", account, amount, "USD", amount,
                   "USD", "Wire", "0"]
            event = build_event(Path("edge_fixture.csv"), row_number, row)
            errors = []
            producer.produce("aml-transactions", json.dumps(event).encode(),
                             on_delivery=lambda error, message: errors.append(error) if error else None)
            if producer.flush(15) or errors:
                raise RuntimeError(f"Kafka delivery failed: {errors}")

        def query(sql, params=()):
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

        def wait_for(label, sql, params, predicate):
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                rows = query(sql, params)
                if predicate(rows):
                    print(f"{label}: {rows}", flush=True)
                    return rows
                time.sleep(1)
            raise TimeoutError(f"{label}: last result {rows}")

        feature_sql = """SELECT in_tx_count, in_amount_sum FROM account_window_features
            WHERE source_dataset='edge_fixture' AND bank_id='B' AND account_id=%s
            AND window_start=%s AND currency='USD' AND feature_version='hourly-v1'"""
        raw_sql = """SELECT source_row, event_time, amount_received FROM transactions
            WHERE source_dataset='edge_fixture' ORDER BY source_row"""

        send(1, "2022/09/01 10:05", "late")
        send(2, "2022/09/01 10:40", "late")
        send(3, "2022/09/01 10:20", "late")
        wait_for("Raw before window close", raw_sql, (), lambda rows: len(rows) == 3)
        assert not query(feature_sql, ("late", "2022-09-01 10:00:00"))
        send(4, "2022/09/01 11:03", "marker")
        before = wait_for("Out-of-order before close", feature_sql, ("late", "2022-09-01 10:00:00"), bool)
        assert before == [(3, 300)], before
        send(5, "2022/09/01 10:30", "late")
        wait_for("Late update", feature_sql, ("late", "2022-09-01 10:00:00"),
                 lambda rows: rows == [(4, 400)])
        send(11, "2022/09/01 11:08", "marker")
        time.sleep(3)
        send(12, "2022/09/01 10:35", "late")
        wait_for("Side output", "SELECT count(*) FROM late_transaction_movements "
                 "WHERE transaction_id='edge_fixture:12'", (), lambda rows: rows == [(2,)])

        # Same event delivered twice: only ingestion metadata differs.
        send(6, "2022/09/01 11:10", "duplicate")
        send(6, "2022/09/01 11:10", "duplicate")
        # Conflicts retain the first observed event, regardless of event time.
        send(7, "2022/09/01 12:10", "conflict")
        send(7, "2022/09/01 12:10", "conflict", "900")
        # Same ID with different timestamps in one window.
        send(10, "2022/09/01 12:20", "same-window", "900")
        send(10, "2022/09/01 12:15", "same-window")
        # Same ID in different windows: the second event must not be counted.
        send(8, "2022/09/01 13:10", "cross")
        send(8, "2022/09/01 14:10", "cross", "900")
        send(9, "2022/09/01 16:03", "marker")

        raw = wait_for("Raw after duplicates and late event", raw_sql, (), lambda rows: len(rows) == 12)
        assert raw[6][2] == 100 and raw[7][2] == 100 and raw[9][2] == 900, raw
        wait_for("Closed first cross window", feature_sql, ("cross", "2022-09-01 13:00:00"), bool)
        wait_for("Raw conflicts", "SELECT count(*) FROM transaction_conflicts "
                 "WHERE transaction_id LIKE %s", ("edge_fixture:%",), lambda rows: rows == [(3,)])
        wait_for("Flink conflicts", "SELECT count(*) FROM flink_transaction_conflicts "
                 "WHERE transaction_id LIKE %s", ("edge_fixture:%",), lambda rows: rows == [(3,)])
        late = query(feature_sql, ("late", "2022-09-01 10:00:00"))
        duplicate = query(feature_sql, ("duplicate", "2022-09-01 11:00:00"))
        conflict = query(feature_sql, ("conflict", "2022-09-01 12:00:00"))
        same_window = query(feature_sql, ("same-window", "2022-09-01 12:00:00"))
        cross_first = query(feature_sql, ("cross", "2022-09-01 13:00:00"))
        cross_second = query(feature_sql, ("cross", "2022-09-01 14:00:00"))
        print(f"After close: {late}; duplicate: {duplicate}; same-time conflict: {conflict}; "
              f"same-window different times: {same_window}; cross-window conflict: "
              f"{cross_first}, {cross_second}", flush=True)
        assert late == [(4, 400)] and duplicate == [(1, 100)]
        assert conflict == [(1, 100)] and same_window == [(1, 900)]
        assert cross_first == [(1, 100)] and cross_second == []

        def flink(path):
            with urllib.request.urlopen("http://localhost:8081" + path, timeout=5) as response:
                return json.load(response)

        jobs = flink("/jobs/overview")["jobs"]
        job_id = next(job["jid"] for job in jobs if job["name"] == "aml-account-features-v2")
        deadline = time.monotonic() + 90
        while flink(f"/jobs/{job_id}/checkpoints")["counts"]["completed"] < 1:
            if time.monotonic() >= deadline:
                raise TimeoutError("Flink checkpoint did not complete")
            time.sleep(1)
        compose("restart", "flink-taskmanager")
        deadline = time.monotonic() + 120
        while flink(f"/jobs/{job_id}/checkpoints")["counts"]["restored"] < 1:
            if time.monotonic() >= deadline:
                raise TimeoutError("Flink did not restore a checkpoint")
            time.sleep(1)
        send(1, "2022/09/01 17:15", "late", "900")
        send(13, "2022/09/01 18:03", "marker")
        wait_for("Conflict after checkpoint recovery", "SELECT count(*) FROM flink_transaction_conflicts "
                 "WHERE transaction_id LIKE %s", ("edge_fixture:%",), lambda rows: rows == [(4,)])
        wait_for("Watermark after checkpoint recovery", feature_sql,
                 ("marker", "2022-09-01 16:00:00"), bool)
        assert query(feature_sql, ("late", "2022-09-01 17:00:00")) == []
        assert query(feature_sql, ("late", "2022-09-01 10:00:00")) == [(4, 400)]
        print("Checkpoint recovery preserved deduplication and window results.", flush=True)
        print("All edge-case checks passed.", flush=True)

        # Only this unique test project is cleared; user project volumes are untouched.
        compose("stop", "raw-writer", "flink-taskmanager", "flink-jobmanager")
        kafka_cli = ("exec", "-T", "kafka")
        groups = compose(*kafka_cli, "/opt/kafka/bin/kafka-consumer-groups.sh",
                         "--bootstrap-server", "localhost:29092", "--list").stdout.splitlines()
        for group in ("aml-raw-writer-v1", "aml-flink-features-v1", "aml-flink-features-v2"):
            if group in groups:
                compose(*kafka_cli, "/opt/kafka/bin/kafka-consumer-groups.sh",
                        "--bootstrap-server", "localhost:29092", "--delete", "--group", group)
        remaining = set(compose(*kafka_cli, "/opt/kafka/bin/kafka-consumer-groups.sh",
                                "--bootstrap-server", "localhost:29092", "--list").stdout.splitlines())
        assert not remaining.intersection({"aml-raw-writer-v1", "aml-flink-features-v1", "aml-flink-features-v2"})
        compose(*kafka_cli, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092",
                "--delete", "--topic", "aml-transactions")
        deadline = time.monotonic() + 60
        while "aml-transactions" in compose(*kafka_cli, "/opt/kafka/bin/kafka-topics.sh",
                                             "--bootstrap-server", "localhost:29092", "--list").stdout.splitlines():
            if time.monotonic() >= deadline:
                raise TimeoutError("Test Kafka topic was not deleted")
            time.sleep(1)
        conn.execute("TRUNCATE transaction_conflicts, flink_transaction_conflicts, late_transaction_movements, "
                     "transactions, account_window_features")
        assert query("SELECT (SELECT count(*) FROM transactions), "
                     "(SELECT count(*) FROM account_window_features), "
                     "(SELECT count(*) FROM transaction_conflicts), "
                     "(SELECT count(*) FROM flink_transaction_conflicts), "
                     "(SELECT count(*) FROM late_transaction_movements)") == [(0, 0, 0, 0, 0)]
        compose("run", "--rm", "--no-deps", "--user", "0", "--entrypoint", "sh", "flink-submit",
                "-c", "find /opt/flink/state -mindepth 1 -delete && "
                "test -z \"$(find /opt/flink/state -mindepth 1 -print -quit)\"")
        print("Isolated test topics, consumer groups, SQL rows and Flink state cleared.", flush=True)
    finally:
        if conn is not None:
            conn.close()
        producer = None
        try:
            compose("stop")
        finally:
            print(f"Test containers stopped and retained: {project}; data: {directory}", flush=True)


if __name__ == "__main__":
    main()
