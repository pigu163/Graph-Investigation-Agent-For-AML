# Graph Investigation Agent for AML

An evidence-grounded investigation prototype for transaction monitoring and temporal graph analysis. Analytical tools compute findings; the LangGraph Agent chooses bounded investigative steps. An indicator is not proof of financial crime.

## Current capabilities

- Replay IBM AML CSV transactions through Kafka; archive raw events and duplicate conflicts in PostgreSQL.
- Compute hourly account features and illustrative alerts with Flink. The web UI shows each alert's triggering transaction and lets an analyst choose the sender or recipient for investigation.
- Query account transactions and investigate bounded graph neighborhoods, fan-in/out, reciprocal flows, cycles and chronological paths. On-demand investigation works with Kafka and Flink stopped.
- Run an experimental LangGraph investigation through the CLI or manually from an alert. Evidence includes transaction references and limitations.

Trained ML scoring, durable case management, automatic alert dispatch, production graph views and MLflow integration remain planned. The graph preview is explicitly synthetic. See the [Agent guide](investigation/AGENT.md) and [investigation tools](investigation/README.md).

## Run the streaming demo

With Docker Desktop running, place the IBM CSV at `dataset/LI-Small/LI-Small_Trans.csv`, then run:

```bash
docker compose up -d --build --wait raw-writer kafka-ui flink-jobmanager flink-taskmanager api frontend
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql
docker compose run --rm flink-submit
docker compose run --rm producer \
  --file /dataset/LI-Small/LI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 --sort-event-time --rate 500 --limit 5000
```

Open [monitoring](http://localhost:3000), [Kafka UI](http://localhost:8080) or [Flink UI](http://localhost:8081). Set `DS_key` in the root `.env` for analyst-initiated investigations. The rules use illustrative thresholds; event-time windows, replay and reset instructions are in the [streaming guide](streaming/README.md).

Run `docker compose stop` to stop services while preserving their data in `volume/`. To run the frontend alone, use `docker compose up -d --build --wait frontend`; see the [frontend guide](frontend/README.md) for local development.

## On-demand investigation

Configure `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` and `DS_key`, then use existing account identifiers and a half-open event-time range:

```bash
uv run python -m graph_investigation_agent_for_aml.agent.cli \
  --dataset your_dataset --bank your_bank --account your_account \
  --start 2022-09-01T00:00:00 --end 2022-09-02T00:00:00 \
  --question "Investigate unusual fund movement" \
  --format markdown --output report.md
```

The [Agent guide](investigation/AGENT.md) covers budgets and output. Streaming results may be revised for eligible late events; very late events are recorded separately. Flink deduplication state grows with unique IDs, and the pipeline does not claim end-to-end exactly-once delivery.

## Layout

`src/` contains the Python producer, persistence, investigation tools, Agent and API; `flink/` contains streaming jobs and schema; `frontend/` contains the Next.js application; `scripts/` contains the development reset utility. The [architecture diagram](image/aml-investigation-architecture.png) includes planned components.
