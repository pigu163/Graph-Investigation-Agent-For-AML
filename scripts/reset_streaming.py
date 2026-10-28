"""Clear project test data while keeping the service containers.

Run from the repository root: python scripts/reset_streaming.py --yes
Stop any producer first. This preserves datasets, code, configuration and SQL tables.
"""

import argparse
import shutil
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def compose(*args, input=None):
    result = subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, input=input,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
    )
    return result.stdout.strip()


def kafka(tool, *args):
    return compose("exec", "-T", "kafka", f"/opt/kafka/bin/{tool}.sh",
                   "--bootstrap-server", "localhost:29092", *args)


def sql(statement):
    return compose("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
                   "-U", "aml_user", "-d", "aml_db", "-At", input=statement)


def wait_for_transactions_table():
    deadline = time.monotonic() + 60
    while sql("SELECT to_regclass('public.transactions');") != "transactions":
        if time.monotonic() >= deadline:
            raise TimeoutError("Raw Writer has not created the transactions table")
        time.sleep(1)


def reset(stop=False):
    # Stop readers first: neither Kafka offsets nor checkpoints may restore old data.
    compose("stop", "raw-writer", "kafka-ui", "flink-taskmanager", "flink-jobmanager")
    compose("up", "-d", "--wait", "kafka", "postgres")
    groups = set(kafka("kafka-consumer-groups", "--list").splitlines())
    for group in ("aml-raw-writer-v1", "aml-flink-features-v1"):
        if group in groups:
            kafka("kafka-consumer-groups", "--delete", "--group", group)

    topics = set(kafka("kafka-topics", "--list").splitlines())
    targets = topics & {"aml-transactions", "aml-risk-alerts", "aml-feature-updates"}
    for topic in sorted(targets):
        kafka("kafka-topics", "--delete", "--topic", topic)
    deadline = time.monotonic() + 60
    while targets & set(kafka("kafka-topics", "--list").splitlines()):
        if time.monotonic() >= deadline:
            raise TimeoutError("Kafka topic deletion has not completed")
        time.sleep(1)

    # Flink is stopped, so its local bind-mount state can now be removed.
    state_directory = ROOT / "volume" / "flink"
    state_directory.mkdir(parents=True, exist_ok=True)
    for item in state_directory.iterdir():
        if item.is_dir() and not item.is_symlink():
            shutil.rmtree(item)
        else:
            item.unlink()
    sql((ROOT / "flink/schema.sql").read_text())
    tables = set(sql("SELECT tablename FROM pg_tables WHERE schemaname='public';").splitlines())
    existing_tables = [
        table for table in ("transactions", "account_window_features")
        if table in tables
    ]
    if existing_tables:
        sql("TRUNCATE TABLE " + ", ".join(existing_tables) + ";")
    if stop:
        for table in existing_tables:
            assert sql("SELECT count(*) FROM " + table + ";") == "0"
        assert not any(state_directory.iterdir()), "Flink state is not empty"
        assert not ({"aml-transactions", "aml-risk-alerts", "aml-feature-updates"}
                    & set(kafka("kafka-topics", "--list").splitlines()))
        assert not ({"aml-raw-writer-v1", "aml-flink-features-v1"}
                    & set(kafka("kafka-consumer-groups", "--list").splitlines()))
        compose("stop")
        print("Reset verified: SQL test rows cleared; AML topics and consumer groups removed; "
              "Flink state removed. Containers stopped and kept.", flush=True)
        return

    kafka("kafka-topics", "--create", "--topic", "aml-transactions",
          "--partitions", "1", "--replication-factor", "1")
    compose("up", "-d", "--wait", "raw-writer", "kafka-ui",
            "flink-jobmanager", "flink-taskmanager")

    wait_for_transactions_table()
    assert sql("SELECT (SELECT count(*) FROM transactions), "
               "(SELECT count(*) FROM account_window_features);") == "0|0"
    for timestamp in ("-2", "-1"):
        assert kafka("kafka-get-offsets", "--topic", "aml-transactions", "--time", timestamp) == "aml-transactions:0:0"
    print("Reset verified: Kafka offsets 0/0; transactions 0; features 0; "
          "Flink state removed. Services ready; no Flink job submitted.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="Confirm deletion of project streaming data")
    parser.add_argument("--stop", action="store_true", help="Stop and keep containers after clearing data")
    args = parser.parse_args()
    if not args.yes:
        parser.error("Stop producers, then pass --yes to confirm deletion")
    reset(stop=args.stop)


if __name__ == "__main__":
    main()
