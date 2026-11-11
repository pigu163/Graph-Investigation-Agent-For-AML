# Flink jobs

Compose runs a JobManager and TaskManager. `flink-submit` submits a job and exits; starting the cluster alone does not submit one. Kafka is reachable at `kafka:29092`, PostgreSQL at `postgres:5432`, and the [Flink UI](http://localhost:8081) on the host.

```bash
docker compose up -d --build --wait flink-jobmanager flink-taskmanager raw-writer
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql
docker compose run --rm flink-submit
```

The default Java DataStream job reads transactions continuously, computes hourly features and alerts, and records late movements and duplicate conflicts in PostgreSQL. It uses a one-minute watermark, five minutes of allowed lateness and checkpoint storage in `volume/flink/`. Deduplication state grows with unique IDs.

For a finite scan of the Kafka offsets present at submission, use:

```bash
docker compose run --rm flink-submit latest-offset
```

This mode uses `account_features.sql` and batch execution. `Dockerfile.datastream` builds both modes, `schema.sql` defines PostgreSQL tables, and `submit.sh` selects the mode. See the [streaming guide](../streaming/README.md) for replay, event-time limits and reset instructions.
