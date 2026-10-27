"""Delete this project's streaming test data, then leave services ready and Flink idle.

Run from the repository root: python scripts/reset_streaming.py --yes
Stop any producer first. This preserves datasets, code, configuration and SQL tables.
"""

import argparse
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


def reset():
    # Stop readers first: neither Kafka offsets nor checkpoints may restore old data.
    compose("stop", "raw-writer", "kafka-ui", "flink-taskmanager", "flink-jobmanager")
    compose("rm", "-f", "flink-taskmanager", "flink-jobmanager")
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

    # The bind mount confines removal to this project's Flink state directory.
    compose("run", "--rm", "--no-deps", "--user", "0", "--entrypoint", "sh",
            "flink-submit", "-c", "find /opt/flink/state -mindepth 1 -delete")
    sql((ROOT / "flink/schema.sql").read_text())
    sql("TRUNCATE TABLE transactions, account_window_features;")
    kafka("kafka-topics", "--create", "--topic", "aml-transactions",
          "--partitions", "1", "--replication-factor", "1")
    compose("up", "-d", "--wait", "raw-writer", "kafka-ui",
            "flink-jobmanager", "flink-taskmanager")

    assert sql("SELECT (SELECT count(*) FROM transactions), "
               "(SELECT count(*) FROM account_window_features);") == "0|0"
    for timestamp in ("-2", "-1"):
        assert kafka("kafka-get-offsets", "--topic", "aml-transactions", "--time", timestamp) == "aml-transactions:0:0"
    print("Reset verified: Kafka offsets 0/0; transactions 0; features 0; Flink test state removed. Services ready; submit a new Flink job when needed.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="Confirm deletion of project streaming data")
    args = parser.parse_args()
    if not args.yes:
        parser.error("Stop producers, then pass --yes to confirm deletion")
    reset()


if __name__ == "__main__":
    main()
