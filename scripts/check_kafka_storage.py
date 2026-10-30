"""Check Kafka startup with fresh and recreated root-owned Linux storage.

Run from the repository root: python scripts/check_kafka_storage.py
Requires Docker Compose and apache/kafka:4.0.1. Uses only temporary Docker volumes.
"""

import json
import subprocess
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    config = json.loads(subprocess.check_output(
        ["docker", "compose", "config", "--no-interpolate", "--format", "json"],
        cwd=ROOT, text=True,
    ))
    services = {name: config["services"][name]
                for name in ("kafka", "kafka-storage-init")}
    services["kafka"].pop("ports", None)
    mounts = {
        "data": "/var/lib/kafka/data",
        "secrets": "/etc/kafka/secrets",
        "config": "/mnt/shared/config",
    }
    for service in services.values():
        service["volumes"] = [
            {"type": "volume", "source": name, "target": target}
            for name, target in mounts.items()
        ]

    with tempfile.TemporaryDirectory(prefix="aml-kafka-storage-") as directory:
        compose_file = Path(directory) / "compose.json"
        compose_file.write_text(json.dumps({
            "services": services, "volumes": {name: {} for name in mounts},
        }))
        command = ["docker", "compose", "-p", "aml-kafka-check-" + uuid.uuid4().hex[:10],
                   "-f", str(compose_file)]

        def compose(*args, capture=False):
            return subprocess.run(command + list(args), check=True, text=True,
                                  capture_output=capture, timeout=180)

        try:
            for state in ("fresh", "recreated"):
                if state == "recreated":
                    compose("down", "-v")
                # Docker volumes live in the Linux VM, preserving real Unix ownership.
                compose("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh",
                        "kafka-storage-init", "-ec",
                        "chown -R 0:0 " + " ".join(mounts.values()) + "; chmod 755 "
                        + " ".join(mounts.values()))
                compose("up", "-d", "--wait", "--wait-timeout", "120", "kafka")
                uid = compose("exec", "-T", "kafka", "id", "-u", capture=True).stdout.strip()
                assert uid != "0", "Kafka must run as its non-root image user"
                for target in mounts.values():
                    compose("exec", "-T", "kafka", "test", "-w", target)
                print(f"Verified {state} root-owned storage: Kafka healthy as UID {uid}", flush=True)
        finally:
            compose("down", "-v", "--remove-orphans")


if __name__ == "__main__":
    main()
