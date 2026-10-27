# Graph Investigation Agent for AML

## Replay IBM AML transactions to Kafka

The local IBM AML Small archives and extracted CSV files belong in `dataset/`. They are ignored by Git and are not included in the Docker image.

Kafka messages are stored under `volume/kafka/`, and PostgreSQL files under `volume/postgres/`. Kafka's empty auxiliary `secrets` and shared-config mount points are mapped to `volume/kafka-secrets/` and `volume/kafka-config/`. These folders are ignored by Git. Kafka UI has no persistent data of its own. See the development reset instructions below to clear transactions or completely rebuild local storage.

### Start Kafka and Kafka UI

Make sure Docker Desktop is running, then start Kafka and its browser UI from the repository root:

```bash
docker compose up -d kafka-ui
```

Compose starts Kafka first and waits for it to become healthy before starting Kafka UI. Open [http://localhost:8080](http://localhost:8080). Select **Topics → `aml-transactions` → Messages** to inspect records.

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

The producer preserves the CSV row order and source event timestamp. The dataset rows are not sorted chronologically, so event times can move backwards during replay.

### Stop the services

```bash
docker compose down
```

This stops Kafka, Kafka UI, PostgreSQL, and Raw Writer. Their data remains in the matching folders under `volume/`.

## Reset data during development

Run these commands from the repository root. Both reset options delete stored data. The original files under `dataset/` and the project source code are preserved. Choose one option; clearing test transactions normally does not require deleting `volume/`.

### Clear transactions and consumer progress (usual reset)

This removes the messages in `aml-transactions`, resets Raw Writer's Kafka progress, and clears the SQL transaction rows. The PostgreSQL database, table structure, and local storage folders remain.

First stop any running Producer (press **Ctrl+C** in its terminal), then stop Raw Writer so it cannot write old messages back into PostgreSQL:

```bash
docker compose stop raw-writer
docker compose up -d --wait kafka postgres
```

List consumer groups. If `aml-raw-writer-v1` appears, delete it using the second command. If it is absent, skip the delete command: a new group that has not committed any messages may not exist yet.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --list

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 --delete --group aml-raw-writer-v1
```

Delete the transaction topic and clear the SQL table:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --delete --if-exists --topic aml-transactions

docker compose exec postgres psql -v ON_ERROR_STOP=1 -U aml_user -d aml_db \
  -c "TRUNCATE TABLE transactions;"
```

Kafka topic deletion is asynchronous. Run the following command and wait until `aml-transactions` is no longer listed before recreating it:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

Create a fresh topic, then restart Raw Writer and Kafka UI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --create --topic aml-transactions \
  --partitions 1 --replication-factor 1

docker compose up -d raw-writer kafka-ui
```

Before running Producer again, verify that Kafka's earliest and latest offsets are both `0`, and that the SQL count is `0`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic aml-transactions --time -2

docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 --topic aml-transactions --time -1

docker compose exec postgres psql -U aml_user -d aml_db \
  -c "SELECT count(*) FROM transactions;"
```

Clearing only PostgreSQL does not reset Kafka messages or consumer progress. To rebuild SQL rows from existing Kafka messages instead, stop Raw Writer, truncate the table, delete its consumer group, and restart Raw Writer without deleting the topic.

### Completely rebuild local storage

Use this option to start Kafka and PostgreSQL from their initial state. It deletes all local Kafka topics and consumer progress, and all PostgreSQL databases, tables, and custom database users stored by this project. PostgreSQL recreates the database and credentials configured in Compose at the next startup.

Stop any running Producer, then stop and remove the project's containers before deleting their data folders:

```bash
docker compose down

rm -rf -- ./volume/kafka ./volume/postgres ./volume/kafka-secrets ./volume/kafka-config

mkdir -p ./volume/kafka ./volume/postgres ./volume/kafka-secrets ./volume/kafka-config

docker compose up -d --wait kafka postgres
```

Create the empty transaction topic, then start Raw Writer and Kafka UI. Raw Writer recreates the SQL table automatically:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --create --topic aml-transactions \
  --partitions 1 --replication-factor 1

docker compose up -d --build raw-writer kafka-ui
docker compose logs --tail 20 raw-writer
```

Once the writer logs `Reading aml-transactions into PostgreSQL transactions`, use the offset and SQL count checks above to verify the empty state. `docker compose down -v` does not remove these host bind-mount folders; deleting their contents explicitly is what resets local storage.
