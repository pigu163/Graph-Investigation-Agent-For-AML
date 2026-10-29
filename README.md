# Graph Investigation Agent for AML

## Replay IBM AML transactions to Kafka

The local IBM AML Small archives and extracted CSV files belong in `dataset/`. They are ignored by Git and are not included in the Docker image.

Current development uses IBM AML for transaction replay, basic Flink features and PostgreSQL persistence. Cross-source schema migration and PaySim integration are deferred.

Kafka messages are stored under `volume/kafka/`, and PostgreSQL files under `volume/postgres/`. Kafka's empty auxiliary `secrets` and shared-config mount points are mapped to `volume/kafka-secrets/` and `volume/kafka-config/`. These folders are ignored by Git. Flink checkpoints live under `volume/flink/`. Kafka UI has no persistent data of its own. See the development reset instructions below to clear transactions or completely rebuild local storage.

### Start Kafka and Kafka UI

Make sure Docker Desktop is running, then start Kafka and its browser UI from the repository root:

```bash
docker compose up -d kafka-ui
```

Compose starts Kafka first and waits for it to become healthy before starting Kafka UI. Starting Producer or Raw Writer also runs `kafka-topic-init`, which creates `aml-transactions` if needed before the consumer starts. Open [http://localhost:8080](http://localhost:8080). Select **Topics → `aml-transactions` → Messages** to inspect records.

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

IBM messages also carry `source_dataset`, `source_file`, `source_row`, and
`event_time_kind=local_unknown_timezone`. Raw Writer saves and validates these
fields; Flink reads the dataset directly rather than extracting it from the ID.
The producer checks the IBM header order to distinguish the two `Account` columns.
Before upgrading from old messages without these fields, clear development data
with the reset command below and replay with the updated producer. Existing SQL
tables receive nullable metadata columns; old rows are preserved until reset.

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

Flink runs as two permanent services: `flink-jobmanager` (coordination and Web UI) and `flink-taskmanager` (computation). The temporary `flink-submit` service submits the DataStream job and exits; `latest-offset` submits the historical SQL batch job. This version pins Flink 1.20.3, Kafka/JDBC connectors 3.3.0-1.20, and PostgreSQL JDBC driver 42.7.5.

### Start the cluster and submit a live job

```bash
docker compose up -d --build --wait flink-jobmanager flink-taskmanager raw-writer kafka-ui

docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db < flink/schema.sql

docker compose run --rm flink-submit
```

Compose runs a one-shot `flink-state-init` service before the JobManager. It prepares `volume/flink/` for the Flink user so fresh Linux bind mounts are writable for checkpoints.

Open [Flink Web UI](http://localhost:8081). Under **Running Jobs**, select `aml-account-features-v2` to inspect its operators, records, watermarks and checkpoints. **Task Managers** shows the worker. Kafka UI remains at [http://localhost:8080](http://localhost:8080). A successful cluster startup alone does not submit a job; check that exactly one feature job is running. Cancel an existing job in the UI before submitting another.

The job reads `aml-transactions` independently of Raw Writer, keeps the first event per transaction ID, records later conflicting content, and upserts these features into `account_window_features`:

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

Windows use the dataset's timezone-free event timestamp, not today's ingestion time. A one-minute watermark triggers the first hourly result. The job retains the window for five additional minutes of event time: eligible late events trigger a revised full result, upserted to PostgreSQL. The first 5,000 IBM rows span only part of an hour, so a short live replay may correctly produce no closed-window output yet.

Live mode assumes approximately ordered input. After the five-minute allowed-lateness period, affected account movements are written to `late_transaction_movements` for review rather than silently discarded. Raw Writer still archives the transaction. The side output does not automatically rebuild old windows. A watermark advances with event time, not wall-clock time; replaying an older dataset into an advanced live job sends its unseen transactions to the late path.

#### Verify late and conflicting events

Run the following isolated integration check with ports 5432, 8081, and 9092 free.
It uses its own Compose project
and temporary data directory, never the project's `volume/` data:

```bash
docker build -t aml-python-datastream:dev .
docker build -f flink/Dockerfile.datastream -t aml-flink-datastream:dev .
uv run --extra streaming python scripts/check_streaming_edge_cases.py
```

It checks a transaction arriving before and after its window closes, identical
duplicates, and conflicting duplicate IDs with equal, different same-window,
and cross-window event times. After a successful run it clears the isolated
test topic, consumer groups, SQL rows, and Flink state, and stops its containers.
On failure it keeps the test data for diagnosis. The project name and directory
are printed at startup. This check does not reset existing project containers or data.

Raw Writer retains the first successfully inserted row per transaction ID.
The DataStream job keeps the first event per ID across windows and writes later
conflicting content to `flink_transaction_conflicts`; Raw Writer also records
conflicts in `transaction_conflicts`. The current topic has one partition, so
both consumers see its events in the same order. Conflicts indicate a data-quality
problem; neither the first value nor a later one is automatically established as correct.
An ingestion timestamp can vary on a normal replay and is not by itself a
business-content conflict. The Flink path excludes later conflicting messages
from feature calculation; the conflict records require investigation.

For suspected late data, inspect the Flink job's watermark, status, and available
operator metrics, and compare raw transactions with window features. These checks
do not prove a window is complete. Before rebuilding, stop the affected job and
producers, identify the affected windows and a complete, trusted source, and
resolve conflicting IDs. Validate a bounded rebuild in an isolated database first;
do not overwrite existing windows until results are checked. A bounded Kafka scan
is suitable only if the retained input is complete and conflicts are resolved.
Replacing or clearing old results requires an explicit scoped plan; an upsert
alone will not remove stale rows that disappear from a rebuilt result. The JDBC
sinks are idempotent per key but do not establish end-to-end exactly-once delivery.

The global first-seen deduplication state is retained for the lifetime of the job
and grows with unique transaction IDs; the local demonstration uses heap state.
Window aggregates also retain distinct counterparties until allowed lateness
expires, so high-cardinality windows increase state. Measure state size before
larger runs. A fresh job without a restored checkpoint
rebuilds deduplication state from retained Kafka history, which must be complete.

On 2026-10-29 an isolated live run merged the first 1,000 rows from each IBM
Small dataset in event-time order, followed by a synthetic event to close the
final windows. All 872 HI and 862 LI account/currency/hour feature rows matched
the CSV reference and independent PostgreSQL aggregation. This is a sample
correctness check, not a full-dataset or throughput evaluation.

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

Validation on 2026-10-28 repeated those sample sizes with the explicit IBM source
metadata. Both original fields and source metadata matched the CSV records; all
4,190 HI-Small and 4,316 LI-Small windows matched CSV and SQL recomputation.
Raw Writer processed 11,000 messages and stored 10,000 unique transactions,
including the 500 duplicate deliveries per dataset. Five local unit tests cover
IBM column order, replay ordering, mapping, identity and time validation.

Repeating the bounded job preserved all results. The live boundary fixture produced eight finalized account/currency/hour rows, retained correct results after TaskManager checkpoint recovery, and produced ten rows including the final window in bounded mode.

The small `tests/fixtures/streaming-boundaries.csv` additionally covers hour boundaries, repeated counterparties, self-transfers, different receiving/payment currencies, and eight decimal places. It can be replayed through `/fixtures/streaming-boundaries.csv`; `scripts/check_flink.py tests/fixtures/streaming-boundaries.csv --closed-only` checks only finalized windows of a live job. Producer ordering checks run with `uv run --extra streaming python -m unittest discover -s tests`.

### Stop the services

```bash
docker compose stop
```

This stops and keeps the containers. Their data remains in the matching folders under `volume/`. Use `docker compose start` to restart existing containers; use `up -d --build` after code or configuration changes.

## Reset data during development

Run these commands from the repository root. Both reset options delete stored data. The original files under `dataset/` and the project source code are preserved. Choose one option; clearing test transactions normally does not require deleting `volume/`.

### Clear transactions, features and computation progress (usual reset)

Stop any running Producer first (Ctrl+C in its terminal), then run:

```bash
python scripts/reset_streaming.py --yes
```

This explicitly deletes this project's test data. It stops Raw Writer and Flink first, deletes the `aml-raw-writer-v1` and `aml-flink-features-v1` consumer groups if present, deletes the known AML topics, truncates `transactions` and `account_window_features`, and removes files under `volume/flink/`. It recreates an empty transaction topic and restarts services. PostgreSQL table structures, CSV files, code and configuration remain.

The script verifies both SQL tables are empty and Kafka's earliest/latest offsets are zero. It restarts the existing Flink containers with cleared state and no submitted job; run `docker compose run --rm flink-submit` when ready to resume monitoring.

To clear the same test data and finish with all service containers stopped and kept:

```bash
python scripts/reset_streaming.py --yes --stop
```

With `--stop`, the AML topics and consumer groups are removed and remain absent;
the script does not recreate the transaction topic or restart consumers. Start
again with `docker compose up -d --wait raw-writer` so the topic initializer runs.

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
