# Streaming operations

The local pipeline replays IBM AML CSV rows into Kafka. Raw Writer archives transactions in PostgreSQL; Flink computes hourly account features and emits rule alerts. The frontend displays persisted alerts and lets an analyst investigate the sender or recipient. No model is called automatically.

## Start and replay

Place the CSV at `dataset/LI-Small/LI-Small_Trans.csv` and start Docker Desktop. From the repository root:

```bash
docker compose up -d --build --wait raw-writer kafka-ui flink-jobmanager flink-taskmanager api frontend
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql
docker compose run --rm flink-submit
docker compose run --rm producer \
  --file /dataset/LI-Small/LI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 --sort-event-time --rate 500 --limit 5000
```

Open [monitoring](http://localhost:3000), [Kafka UI](http://localhost:8080), or [Flink UI](http://localhost:8081). Submit only one live job. `--limit` selects the first source rows before sorting; IDs preserve the original row numbers. Full-file sorting uses temporary disk space. IBM event timestamps have no timezone, and same-minute events do not have a known order.

Alerts are stored in `risk_alerts`. The UI displays the trigger transaction and enables an investigation after Raw Writer archives it. Each manually initiated investigation covers the preceding 24 hours, allows up to three model calls, and returns a synchronous report without saving a case. Set `DS_key` in the root `.env` to enable it. Optionally set the same `AML_API_TOKEN` for the API and server-side frontend proxy; this local arrangement is not multi-user authentication.

Default rules are illustrative: ten distinct senders, ten distinct receivers, or 100000 USD paid. Configure these before submitting the job, and change the rule version when changing a rule:

```text
AML_FAN_IN_THRESHOLD=10
AML_FAN_OUT_THRESHOLD=10
AML_LARGE_AMOUNT_THRESHOLD=100000
AML_LARGE_AMOUNT_CURRENCY=USD
AML_ALERT_RULE_VERSION=demo-v1
```

Fan alerts fire at threshold crossing; their count is not continuously updated and the displayed transaction is the crossing event. The API streams the latest 100 persisted alerts, polling roughly once per second per client. Resubmitting a fresh job reads retained Kafka history and may regenerate historical alerts.

## Event time and recovery

Live windows are one hour, with a one-minute watermark and five minutes of allowed lateness. Eligible late events update results; later movements are recorded in `late_transaction_movements` and do not automatically rebuild windows. A short replay may not advance the watermark enough to close a window. Raw Writer keeps the first transaction per ID, and both processing paths record conflicting duplicates. Flink's first-seen state grows with unique IDs; window state also grows with distinct counterparties.

Checkpoints are stored in `volume/flink/`. The local cluster has no JobManager high availability: after a full cluster restart, submit a fresh job. Complete retained Kafka history is required to reconstruct state. Idempotent writes do not establish end-to-end exactly-once delivery.

For a finite Kafka snapshot with unordered historical input, submit the bounded SQL job instead:

```bash
docker compose run --rm flink-submit latest-offset
```

It finishes after processing the captured offsets. See [Flink](../flink/README.md) for the components and job modes.

## Inspect and stop

```bash
docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT count(*) FROM transactions; SELECT count(*) FROM account_window_features; SELECT count(*) FROM risk_alerts;"
docker compose logs --tail 50 raw-writer
docker compose stop
```

Stopping preserves containers and data under `volume/`. To delete this project's transaction data, alert and feature rows, Kafka progress and Flink state, stop producers first and run `python scripts/reset_streaming.py --yes`; add `--stop` to leave services stopped. This reset is destructive. It preserves the source CSVs and database schema. Submit a new Flink job explicitly when resuming.
