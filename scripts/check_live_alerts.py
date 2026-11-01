"""Run an isolated Flink -> PostgreSQL -> API -> frontend alert integration check.

Build aml-alert-test-{flink,python,api,frontend}:local first. This uses ports
3000, 5432, 8000, 8081 and 9092, a temporary Compose project and a fake model.
Pass --keep-open to inspect the frontend before automatic cleanup.
"""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import psycopg
from confluent_kafka import Producer

from graph_investigation_agent_for_aml.producer import build_event

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "isolated-test-token"
STUB = '''import uvicorn
from graph_investigation_agent_for_aml import api
def fake_model(key, max_calls=3):
    def decide(system, context):
        decide.calls += 1
        return {"action": "finish", "purpose": "Complete the isolated integration test."}
    decide.calls = 0
    decide.model = "test-stub"
    decide.usage = {"prompt_tokens": 0, "completion_tokens": 0}
    return decide
api.load_key = lambda: "test-only"
api.model_decision = fake_model
uvicorn.run(api.app, host="0.0.0.0", port=8000)
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-open", action="store_true")
    args = parser.parse_args()
    directory = Path(tempfile.mkdtemp(prefix="aml-live-alerts-", dir="/tmp"))
    project = "aml-live-alerts-" + uuid.uuid4().hex[:10]
    (directory / "stub_api.py").write_text(STUB)
    for name in ("postgres", "kafka", "kafka-secrets", "kafka-config", "flink"):
        (directory / name).mkdir()
    def mount(name, target):
        return {"type": "bind", "source": str(directory / name), "target": target}
    kafka_mounts = [mount("kafka", "/var/lib/kafka/data"), mount("kafka-secrets", "/etc/kafka/secrets"),
                    mount("kafka-config", "/mnt/shared/config")]
    state_mount = [mount("flink", "/opt/flink/state")]
    services = {
        "postgres": {"volumes": [mount("postgres", "/var/lib/postgresql/data")]},
        "kafka": {"volumes": kafka_mounts, "environment": {"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1"}},
        "kafka-storage-init": {"volumes": kafka_mounts},
        "raw-writer": {"image": "aml-alert-test-python:local"},
        "api": {"image": "aml-alert-test-api:local", "command": ["python", "/tmp/stub_api.py"],
                "environment": {"AML_API_TOKEN": TOKEN, "DS_key": ""},
                "volumes": [{"type": "bind", "source": str(directory / "stub_api.py"), "target": "/tmp/stub_api.py", "read_only": True}]},
        "frontend": {"image": "aml-alert-test-frontend:local", "environment": {"AML_API_TOKEN": TOKEN}},
    }
    for service in ("flink-jobmanager", "flink-taskmanager", "flink-state-init", "flink-submit"):
        services[service] = {"image": "aml-alert-test-flink:local", "volumes": state_mount}
    services["flink-submit"]["environment"] = {"AML_FAN_IN_THRESHOLD": "2", "AML_FAN_OUT_THRESHOLD": "2",
        "AML_LARGE_AMOUNT_THRESHOLD": "1000", "AML_LARGE_AMOUNT_CURRENCY": "USD", "AML_ALERT_RULE_VERSION": "test-v1"}
    override = directory / "override.json"
    override.write_text(json.dumps({"services": services}))
    command = ["docker", "compose", "--env-file", "/dev/null", "--project-name", project,
               "--project-directory", str(directory), "-f", str(ROOT / "compose.yaml"), "-f", str(override)]
    env = os.environ.copy()
    env.pop("DS_key", None)
    print(f"Isolated project: {project}; data: {directory}", flush=True)

    def compose(*params, data=None, check=True):
        return subprocess.run(command + list(params), input=data, text=True, capture_output=True,
                              check=check, timeout=180, env=env)

    def get(url, *, token=True):
        headers = {"Authorization": f"Bearer {TOKEN}"} if token else {}
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=5) as response:
            return json.load(response)

    def wait_for(label, fn, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = fn()
                if value:
                    print(f"{label}: {len(value) if isinstance(value, list) else value}", flush=True)
                    return value
            except (OSError, ValueError, KeyError, psycopg.Error):
                pass
            time.sleep(1)
        raise TimeoutError(label)

    producer = None
    try:
        # Verify the merged Compose paths before touching any data.
        config = json.loads(compose("config", "--format", "json").stdout)
        for service, target in (("postgres", "/var/lib/postgresql/data"), ("kafka", "/var/lib/kafka/data"),
                                ("flink-jobmanager", "/opt/flink/state")):
            source = next(v["source"] for v in config["services"][service]["volumes"] if v["target"] == target)
            assert Path(source).resolve().is_relative_to(directory.resolve()), (service, source)
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "raw-writer",
                "flink-jobmanager", "flink-taskmanager", "api", "frontend")
        compose("exec", "-T", "postgres", "psql", "-U", "aml_user", "-d", "aml_db", "-v", "ON_ERROR_STOP=1", "-f", "-",
                data=(ROOT / "flink/schema.sql").read_text())
        compose("run", "--rm", "--no-deps", "flink-submit")
        wait_for("Flink running", lambda: get("http://127.0.0.1:8000/api/status")["flink"] == "running")
        wait_for("Frontend proxy ready", lambda: get("http://127.0.0.1:3000/api/backend/status", token=False)["database"] == "ready")
        try:
            get("http://127.0.0.1:8000/api/alerts", token=False)
            raise AssertionError("Missing API token was accepted")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401

        stream = urllib.request.urlopen("http://127.0.0.1:3000/api/backend/alerts/stream", timeout=20)
        assert stream.readline().decode().strip() == "event: snapshot"
        assert json.loads(stream.readline().decode().removeprefix("data: "))["alerts"] == []
        stream.readline()
        producer = Producer({"bootstrap.servers": "127.0.0.1:9092"})
        def send(number, sender, receiver, amount="100", currency="USD", timestamp=None):
            row = [timestamp or f"2022/09/01 09:{number:02d}", "01", sender, "01", receiver, amount, currency, amount, currency, "Wire", "0"]
            event = build_event(Path("live-alerts.csv"), number, row)
            producer.produce("aml-transactions", key=event["transaction_id"], value=json.dumps(event).encode())
            assert producer.flush(15) == 0
        send(1, "S1", "R")
        send(2, "S2", "R")
        send(3, "S2", "R2")
        send(4, "X", "Y", "1500")
        send(4, "X", "Y", "1500")
        send(5, "S2", "S2", "1500", "EUR")
        send(6, "S3", "R")

        def ready_alerts():
            alerts = get("http://127.0.0.1:8000/api/alerts")["alerts"]
            return alerts if len(alerts) == 3 and all(a["transaction_available"] for a in alerts) else None
        alerts = wait_for("Three early alerts with archived trigger transactions", ready_alerts)
        assert {a["alert_type"] for a in alerts} == {"fan_in", "fan_out", "large_transaction"}
        assert all(a["rule_version"] == "test-v1" for a in alerts)
        fan_in = next(a for a in alerts if a["alert_type"] == "fan_in")
        fan_out = next(a for a in alerts if a["alert_type"] == "fan_out")
        large = next(a for a in alerts if a["alert_type"] == "large_transaction")
        assert (fan_in["account_id"], fan_in["transaction_id"], fan_in["observed"]) == ("R", "live-alerts:2", "2")
        assert (fan_out["account_id"], fan_out["transaction_id"], fan_out["observed"]) == ("S2", "live-alerts:3", "2")
        assert large["transaction_id"] == "live-alerts:4" and large["threshold"] == "1000"
        assert "is_laundering" not in json.dumps(alerts) and "raw_event" not in json.dumps(alerts)
        with psycopg.connect("host=127.0.0.1 dbname=aml_db user=aml_user password=aml_local_dev") as conn:
            assert conn.execute("SELECT count(*) FROM account_window_features").fetchone()[0] == 0
        # The stream must deliver the saved alert while the event-time hour is still open.
        while True:
            line = stream.readline().decode()
            if line.startswith("data: ") and len(json.loads(line[6:])["alerts"]) == 3:
                break
        stream.close()

        def query(sql, params=()):
            with psycopg.connect("host=127.0.0.1 dbname=aml_db user=aml_user password=aml_local_dev") as conn:
                return conn.execute(sql, params).fetchone()[0]
        send(7, "S4", "R_LATE")
        send(8, "MARK1", "MARK2", timestamp="2022/09/01 10:02")
        wait_for("First hour closed", lambda: query("SELECT count(*) FROM account_window_features") > 0)
        send(9, "S5", "R_LATE")
        wait_for("Allowed-late fan-in crossing", lambda: query(
            "SELECT count(*) FROM risk_alerts WHERE payload->>'alert_type'='fan_in' AND payload->>'account_id'='R_LATE'") == 1)
        send(10, "S6", "R_EXPIRED")
        send(11, "MARK3", "MARK4", timestamp="2022/09/01 10:08")
        wait_for("Later watermark", lambda: query("SELECT count(*) FROM transactions WHERE transaction_id='live-alerts:11'") == 1)
        time.sleep(3)
        send(12, "S7", "R_EXPIRED")
        wait_for("Too-late movements recorded", lambda: query("SELECT count(*) FROM late_transaction_movements WHERE transaction_id='live-alerts:12'") == 2)
        assert query("SELECT count(*) FROM risk_alerts WHERE payload->>'alert_type'='fan_in' AND payload->>'account_id'='R_EXPIRED'") == 0

        jobs = get("http://127.0.0.1:8081/jobs/overview", token=False)["jobs"]
        job_id = next(j["jid"] for j in jobs if j["name"] == "aml-account-features-v2" and j["state"] == "RUNNING")
        checkpoints = f"http://127.0.0.1:8081/jobs/{job_id}/checkpoints"
        wait_for("Completed checkpoint", lambda: get(checkpoints, token=False)["counts"]["completed"] > 0)
        compose("restart", "flink-taskmanager")
        wait_for("Checkpoint restored", lambda: get(checkpoints, token=False)["counts"]["restored"] > 0)
        send(4, "X", "Y", "1500")
        time.sleep(3)
        assert query("SELECT count(*) FROM risk_alerts WHERE payload->>'alert_type'='large_transaction'") == 1
        for party, account in (("sender", "X"), ("recipient", "Y")):
            payload = json.dumps({"alert_id": large["alert_id"], "party": party}).encode()
            request = urllib.request.Request("http://127.0.0.1:3000/api/backend/investigations", data=payload,
                                             headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(request, timeout=30) as response:
                report = json.load(response)
            assert report["case"]["account"] == account and report["case"]["selected_party"] == party
            assert report["stop_reason"] == "agent_finished" and report["model"]["model_id"] == "test-stub"
            assert "live-alerts:4" in json.dumps(report["evidence"])
        compose("stop", "raw-writer", "flink-taskmanager", "flink-jobmanager", "kafka")
        assert get("http://127.0.0.1:8000/api/status")["flink"] == "unavailable"
        request = urllib.request.Request("http://127.0.0.1:3000/api/backend/investigations",
            data=json.dumps({"alert_id": large["alert_id"], "party": "sender"}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=30) as response:
            assert json.load(response)["case"]["account"] == "X"
        print("SSE, early/late alerts, checkpoint recovery, deduplication, currency, self-transfer and both Agent account selections verified; on-demand investigation works with Kafka/Flink stopped; no paid model calls.", flush=True)
        if args.keep_open:
            input("READY_FOR_UI: press Enter to remove isolated containers and data.\n")
    finally:
        producer = None
        result = compose("down", "--volumes", "--remove-orphans", check=False)
        if result.returncode:
            print(result.stderr, flush=True)
            raise RuntimeError(f"Failed to remove isolated Docker project {project}; retained {directory}")
        # The directory is unique to this run. A root container clears DB-owned files on Linux too.
        subprocess.run(["docker", "run", "--rm", "--network", "none", "--user", "0:0", "-v", f"{directory}:/cleanup",
                        "--entrypoint", "sh", "postgres:17-alpine", "-c", "find /cleanup -mindepth 1 -delete"], check=True, timeout=60)
        shutil.rmtree(directory)
        print(f"Removed isolated containers, network and data: {project}", flush=True)


if __name__ == "__main__":
    main()
