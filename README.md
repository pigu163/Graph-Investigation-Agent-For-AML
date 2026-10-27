# Graph Investigation Agent for AML

## Replay IBM AML transactions to Kafka

The local IBM AML Small archives and extracted CSV files belong in `dataset/`. They are ignored by Git and are not included in the Docker image.

Kafka messages are stored under `volume/kafka/`, and PostgreSQL files under `volume/postgres/`. Kafka's empty auxiliary `secrets` and shared-config mount points are mapped to `volume/kafka-secrets/` and `volume/kafka-config/`. These folders are ignored by Git. Kafka UI has no persistent data of its own. To reset local service data, stop Compose and remove the contents of the relevant folder.

### Start Kafka and Kafka UI

Make sure Docker Desktop is running, then start Kafka and its browser UI from the repository root:

```bash
docker compose up -d kafka-ui
```

Compose starts Kafka first and waits for it to become healthy before starting Kafka UI. Open [http://localhost:8080](http://localhost:8080). Select **Topics → `aml-transactions` → Messages** to inspect records.

PostgreSQL is available on `localhost:5432` with database `aml_db`, user `aml_user`, and local development password `aml_local_dev`. For example, connect from the host with `psql -h localhost -p 5432 -U aml_user -d aml_db`.

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

This stops Kafka, Kafka UI, and PostgreSQL. Their data remains in the matching folders under `volume/`.
