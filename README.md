# Graph Investigation Agent for AML

## Web frontend foundation

The separate [`frontend/`](frontend/README.md) configures Next.js, React,
TypeScript, Tailwind CSS, shadcn/ui, Framer Motion, Recharts, and Sigma.js +
Graphology. It includes an Apple-inspired glass shell, five route placeholders,
accessibility preferences, and an opt-in synthetic graph preview. It is not yet
connected to FastAPI, an Agent, or MLflow; no live metrics are claimed.
See the frontend README for Node 22 setup and local development commands.

The frontend is also included in Docker Compose. Start the complete existing
stack (frontend, Kafka, PostgreSQL, Raw Writer, Kafka UI and Flink cluster):

```bash
docker compose up -d --build --wait
```

Open the frontend at [http://localhost:3000](http://localhost:3000), Kafka UI at
[http://localhost:8080](http://localhost:8080), and Flink UI at
[http://localhost:8081](http://localhost:8081). To run only the frontend, use
`docker compose up -d --build --wait frontend`. Producer and Flink Submit are
on-demand commands in the same Compose file, not automatic background jobs.
Starting the stack does not replay data or submit a Flink job.

```bash
docker compose down
```

This stops/removes this project's containers and network while preserving
`volume/` data and built images.

## Replay IBM AML transactions to Kafka

The local IBM AML Small archives and extracted CSV files belong in `dataset/`. They are ignored by Git and are not included in the Docker image.

Kafka messages are stored under `volume/kafka/`, and PostgreSQL files under `volume/postgres/`. Kafka's empty auxiliary `secrets` and shared-config mount points are mapped to `volume/kafka-secrets/` and `volume/kafka-config/`. These folders are ignored by Git. Flink checkpoints live under `volume/flink/`. Kafka UI has no persistent data of its own. See the development reset instructions below to clear transactions or completely rebuild local storage.

### Start Kafka and Kafka UI

Make sure Docker Desktop is running, then start Kafka and its browser UI from the repository root:

```bash
docker compose up -d kafka-ui
```

Compose starts Kafka first and waits for it to become healthy before starting Kafka UI. Open [http://localhost:8080](http://localhost:8080). Select **Topics → `aml-transactions` → Messages** to inspect records.

Before Kafka starts, the one-shot `kafka-storage-init` service assigns its three storage/configuration bind mounts to the image's `appuser`. This also runs when starting after a full storage reset, so root-owned directories on Linux are writable without running the broker as root. Check fresh and recreated storage with `python scripts/check_kafka_storage.py`; this uses isolated Docker volumes and does not access project data.

PostgreSQL is available on `localhost:5432` with database `aml_db`, user `aml_user`, and local development password `aml_local_dev`. For example, connect from the host with `psql -h localhost -p 5432 -U aml_user -d aml_db`.

### Save Kafka transactions to PostgreSQL

Start Raw Writer and its Kafka/PostgreSQL dependencies:

```bash
docker compose up -d --build raw-writer
docker compose logs -f raw-writer
```

Raw Writer creates the `transactions` table on startup, including when PostgreSQL was already initialized. A new consumer group reads existing messages from the beginning; later starts resume from its committed Kafka offsets. Producer and Raw Writer share the same Python Docker image but run different Python modules.

The table stores transaction fields, exact numeric amounts, the original JSON, and the Kafka topic/partition/offset of the first saved message. Source event timestamps have no timezone in this dataset and are stored without inventing one. `is_laundering` is the dataset's evaluation label.

Each database insert is committed before acknowledging the Kafka message. Repeated transaction IDs keep the first stored row. This permits safe replay after a crash between the two commits; it does not claim end-to-end exactly-once delivery. Invalid messages or failed writes stop the writer without acknowledging that message. Inspect its logs, correct the problem, then restart it with `docker compose up -d raw-writer`.

After replaying a sample, check the database directly:

```bash
docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT count(*) FROM transactions;"
docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT transaction_id, event_time, amount_paid, payment_currency FROM transactions ORDER BY transaction_id LIMIT 5;"
```

Replay the same sample again and rerun the count query: Kafka gains messages, while the database's unique transaction count stays the same. The first version writes one message at a time for clarity; larger replays may need batching for better throughput.

To compare the first 1,000 saved LI records directly with the original CSV:

```bash
PGHOST=localhost PGDATABASE=aml_db PGUSER=aml_user PGPASSWORD=aml_local_dev \
  uv run --extra streaming python scripts/check_raw_writer.py \
  dataset/LI-Small/LI-Small_Trans.csv
```

### Run a short replay

This command sends 1,000 LI Small records at a target rate of 500 records per second:

```bash
docker compose run --rm producer \
  --file /dataset/LI-Small/LI-Small_Trans.csv \
  --topic aml-transactions \
  --bootstrap-servers kafka:29092 \
  --rate 500 \
  --limit 1000
```

The producer prints `Delivered 1000 transactions to aml-transactions` after Kafka confirms delivery. The temporary producer container then exits and is removed. Replaying the same rows appends them again; Kafka does not deduplicate messages by `transaction_id`.

### Replay a full dataset

Remove `--limit 1000` to replay the whole file. For example, HI Small has about 5.1 million rows, so `--rate 1000` takes roughly 85 minutes:

```bash
docker compose run --rm producer \
  --file /dataset/HI-Small/HI-Small_Trans.csv \
  --topic aml-transactions \
  --bootstrap-servers kafka:29092 \
  --rate 1000
```

By default, the producer preserves the CSV row order and source event timestamp. The dataset rows are not sorted chronologically, so event times can move backwards during replay. Use `--sort-event-time` for ordered live Flink replay as described below.

## Flink account features and Web UI

Flink runs as two permanent services: `flink-jobmanager` (coordination and Web UI) and `flink-taskmanager` (computation). The temporary `flink-submit` service submits SQL and exits. This version pins Flink 1.20.3, Kafka/JDBC connectors 3.3.0-1.20, and PostgreSQL JDBC driver 42.7.5.

### Start the cluster and submit a live job

```bash
docker compose up -d --build --wait flink-jobmanager flink-taskmanager raw-writer kafka-ui

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql

docker compose run --rm flink-submit
```

Compose runs a one-shot `flink-state-init` service before the JobManager. It prepares `volume/flink/` for the Flink user so fresh Linux bind mounts are writable for checkpoints.

Open [Flink Web UI](http://localhost:8081). Under **Running Jobs**, select `aml-account-features-v1` to inspect its operators, records, watermarks and checkpoints. **Task Managers** shows the worker. Kafka UI remains at [http://localhost:8080](http://localhost:8080). A successful cluster startup alone does not submit a job; check that exactly one feature job is running. Cancel an existing job in the UI before submitting another.

The job reads `aml-transactions` independently of Raw Writer, deduplicates transactions inside one-hour event-time windows, and upserts these features into `account_window_features`:

- Incoming/outgoing transaction counts and amounts; net inflow.
- Distinct external senders (Fan-in) and receivers (Fan-out).
- Separate results for each source dataset, bank/account, currency and hour; version `hourly-v1`.

Self-transfers contribute to incoming/outgoing counts and amounts, but not external counterparties. Incoming amounts use the receiving currency; outgoing amounts use the payment currency. Inputs use DECIMAL(28,8), outputs DECIMAL(38,8), suitable for the tested IBM data; this is a defined numeric range, not arbitrary-precision arithmetic. The evaluation label is not a detection feature.

For live replay, sort the selected CSV prefix by event time:

```bash
docker compose run --rm producer \
  --file /dataset/LI-Small/LI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 --sort-event-time --rate 500 --limit 5000
```

`--limit` selects the first N original rows, then `--sort-event-time` sorts those rows while preserving their original transaction IDs. Full-file sorting uses temporary SQLite disk storage inside the producer container. It must finish before sending begins; a full replay requires temporary disk space. Same-minute order does not establish a real chronological order within that minute.

### Event time and replay limits

Windows use the dataset's timezone-free event timestamp, not today's ingestion time. A one-minute watermark closes hourly windows after event time advances past their end. Final results are emitted when windows close, not for every transaction. The first 5,000 IBM rows span only part of an hour, so a short live replay may correctly produce no closed-window output yet.

Live mode assumes approximately ordered input. Events arriving after their window has closed are dropped from feature computation, although Raw Writer still archives them. This version does not automatically correct closed windows for very late data; inspect Flink's late-record metrics and use a controlled bounded rebuild when needed. Replaying an older dataset into an already advanced live job is not a valid way to merge histories.

Checkpointing runs every 10 seconds, using the shared project bind mount `volume/flink/`. TaskManager failure can recover through the running JobManager and checkpoints. This session cluster does not configure JobManager high availability: after a full cluster restart, manually submit a new job, which reads Kafka from the earliest retained offset. Rebuilding unordered history requires the bounded mode below; live replay must respect the watermark assumptions. Incomplete Kafka retention cannot rebuild complete history. No end-to-end exactly-once claim is made.

### Verify IBM HI-Small and LI-Small samples

Use an empty development environment and no active producers/live Flink jobs. The bounded mode below captures Kafka's end offsets and uses **batch execution** over that finite snapshot; it handles the unordered combination of historical datasets and closes the final window without fake transactions. Continuous monitoring uses streaming execution instead.

```bash
docker compose run --rm producer \
  --file /dataset/HI-Small/HI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 --sort-event-time --rate 500 --limit 5000

docker compose run --rm producer \
  --file /dataset/LI-Small/LI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 --sort-event-time --rate 500 --limit 5000

docker compose run --rm flink-submit latest-offset
```

Wait for the job to be **FINISHED** in Flink UI and for Raw Writer to catch up. Use its consumer lag or the SQL count to check progress. Then compare every output key and feature value with both the original CSV and independent PostgreSQL aggregation:

```bash
PGHOST=localhost PGDATABASE=aml_db PGUSER=aml_user PGPASSWORD=aml_local_dev \
  uv run --extra streaming python scripts/check_flink.py \
  dataset/HI-Small/HI-Small_Trans.csv --limit 5000

PGHOST=localhost PGDATABASE=aml_db PGUSER=aml_user PGPASSWORD=aml_local_dev \
  uv run --extra streaming python scripts/check_flink.py \
  dataset/LI-Small/LI-Small_Trans.csv --limit 5000

docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT source_dataset, count(*) FROM account_window_features GROUP BY source_dataset;"
```

Appending duplicate messages before a bounded scan should not change either unique transactions or computed features. Rerunning the same bounded job should upsert the same results. These commands test samples, not all millions of records.

Local validation on 2026-10-27 checked 5,000 source transactions per IBM dataset: HI-Small produced 4,190 account/currency/hour rows, LI-Small 4,316. Every feature matched both the source-CSV reference calculation and PostgreSQL recomputation, and all original transaction fields were checked separately. The input included another 500 duplicate messages per dataset. These are functional sample checks, not a full-dataset evaluation or throughput benchmark.

Repeating the bounded job preserved all results. The live boundary fixture produced eight finalized account/currency/hour rows, retained correct results after TaskManager checkpoint recovery, and produced ten rows including the final window in bounded mode.

The small `tests/fixtures/streaming-boundaries.csv` additionally covers hour boundaries, repeated counterparties, self-transfers, different receiving/payment currencies, and eight decimal places. It can be replayed through `/fixtures/streaming-boundaries.csv`; `scripts/check_flink.py tests/fixtures/streaming-boundaries.csv --closed-only` checks only finalized windows of a live job. Producer ordering checks run with `uv run --extra streaming python -m unittest discover -s tests`.

### Stop the services

```bash
docker compose down
```

This stops Kafka, both UIs, PostgreSQL, Raw Writer and the Flink cluster. Their data remains in the matching folders under `volume/`.

## Reset data during development

Run these commands from the repository root. Both reset options delete stored data. The original files under `dataset/` and the project source code are preserved. Choose one option; clearing test transactions normally does not require deleting `volume/`.

### Clear transactions, features and computation progress (usual reset)

Stop any running Producer first (Ctrl+C in its terminal), then run:

```bash
python scripts/reset_streaming.py --yes
```

This explicitly deletes this project's test data. It stops Raw Writer and Flink first, deletes the `aml-raw-writer-v1` and `aml-flink-features-v1` consumer groups if present, deletes the known AML topics, truncates `transactions` and `account_window_features`, and removes files under `volume/flink/`. It recreates an empty transaction topic and restarts services. PostgreSQL table structures, CSV files, code and configuration remain.

The script verifies both SQL tables are empty and Kafka's earliest/latest offsets are zero. Flink containers are recreated so previous test job history is cleared. Flink UI stays available with **no submitted job**; run `docker compose run --rm flink-submit` when ready to resume monitoring.

Do not truncate only the feature table while leaving a live Flink job or its old checkpoint state running: those components can restore old results. Reset all relevant progress together.

```bash
docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT (SELECT count(*) FROM transactions) AS transactions, (SELECT count(*) FROM account_window_features) AS features;"

docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic aml-transactions --time -1
```

### Completely rebuild local storage

Use this option to start Kafka, PostgreSQL and Flink from their initial state. It deletes all local Kafka topics and consumer progress, and all PostgreSQL databases, tables, and custom database users stored by this project. PostgreSQL recreates the database and credentials configured in Compose at the next startup.

Stop any running Producer, then stop and remove the project's containers before deleting their data folders:

```bash
docker compose down

rm -rf -- ./volume/kafka ./volume/postgres ./volume/kafka-secrets ./volume/kafka-config ./volume/flink

mkdir -p ./volume/kafka ./volume/postgres ./volume/kafka-secrets ./volume/kafka-config ./volume/flink

docker compose up -d --wait kafka postgres
```

Create the empty transaction topic, then start Raw Writer and Kafka UI. Raw Writer recreates the SQL table automatically:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --create --topic aml-transactions \
  --partitions 1 --replication-factor 1

docker compose up -d --build --wait raw-writer kafka-ui flink-jobmanager flink-taskmanager
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql
docker compose logs --tail 20 raw-writer
```

Once the writer logs `Reading aml-transactions into PostgreSQL transactions`, use the offset and SQL count checks above to verify the empty state. Submit a Flink job separately when needed. `docker compose down -v` does not remove these host bind-mount folders; deleting their contents explicitly is what resets local storage.
