# Graph Investigation Agent for AML

## Replay IBM AML Small Transactions to Kafka

The downloaded and extracted datasets stay in the local `dataset/` directory and are not committed to Git.

```bash
docker compose up -d kafka
docker compose run --rm producer \
  --file /dataset/HI-Small/HI-Small_Trans.csv \
  --bootstrap-servers kafka:29092 \
  --rate 100
```

Use `LI-Small/LI-Small_Trans.csv` to replay the LI dataset. Add `--limit 1000` for a short smoke run. The producer retains CSV row order and original event timestamps; the sample files are not chronologically sorted.
