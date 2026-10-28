SET 'execution.runtime-mode' = '__RUNTIME_MODE__';
SET 'execution.attached' = 'false';
SET 'pipeline.name' = 'aml-account-features-v1';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.dir' = 'file:///opt/flink/state/checkpoints';

CREATE TABLE kafka_transactions (
    transaction_id STRING,
    source_dataset STRING,
    event_time STRING,
    source_bank STRING,
    source_account STRING,
    destination_bank STRING,
    destination_account STRING,
    amount_received DECIMAL(28, 8),
    receiving_currency STRING,
    amount_paid DECIMAL(28, 8),
    payment_currency STRING,
    event_ts AS TO_TIMESTAMP(event_time, 'yyyy/MM/dd HH:mm'),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'aml-transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'aml-flink-features-v1',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = '__BOUNDED_MODE__',
    'format' = 'json',
    'json.fail-on-missing-field' = 'true',
    'json.ignore-parse-errors' = 'false'
);

-- Deduplicate within the transaction's event-time window, then release state.
-- Old replays arriving after a closed window require a controlled rebuild.
CREATE VIEW unique_transactions AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window_start, window_end, transaction_id
        ORDER BY event_ts ASC
    ) AS row_num
    FROM TABLE(TUMBLE(TABLE kafka_transactions, DESCRIPTOR(event_ts), INTERVAL '1' HOUR))
) WHERE row_num = 1;

-- One transaction contributes an outgoing and an incoming account observation.
-- window_time preserves event time across the first windowed operation.
CREATE VIEW account_movements AS
SELECT source_dataset,
       source_bank AS bank_id, source_account AS account_id,
       payment_currency AS currency, window_time AS movement_time,
       CAST(0 AS BIGINT) AS in_count, CAST(1 AS BIGINT) AS out_count,
       CAST(0 AS DECIMAL(28, 8)) AS in_amount, amount_paid AS out_amount,
       CAST(NULL AS STRING) AS sender,
       CASE WHEN source_bank <> destination_bank OR source_account <> destination_account
            THEN CONCAT(destination_bank, ':', destination_account) END AS receiver
FROM unique_transactions
UNION ALL
SELECT source_dataset, destination_bank, destination_account,
       receiving_currency, window_time,
       CAST(1 AS BIGINT), CAST(0 AS BIGINT),
       amount_received, CAST(0 AS DECIMAL(28, 8)),
       CASE WHEN source_bank <> destination_bank OR source_account <> destination_account
            THEN CONCAT(source_bank, ':', source_account) END,
       CAST(NULL AS STRING)
FROM unique_transactions;

CREATE TABLE feature_sink (
    source_dataset STRING,
    bank_id STRING,
    account_id STRING,
    currency STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    feature_version STRING,
    in_tx_count BIGINT,
    out_tx_count BIGINT,
    in_amount_sum DECIMAL(38, 8),
    out_amount_sum DECIMAL(38, 8),
    net_inflow DECIMAL(38, 8),
    fan_in BIGINT,
    fan_out BIGINT,
    PRIMARY KEY (source_dataset, bank_id, account_id, currency,
                 window_start, window_end, feature_version) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/aml_db',
    'table-name' = 'account_window_features',
    'username' = 'aml_user',
    'password' = 'aml_local_dev',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1 s',
    'sink.max-retries' = '3'
);

INSERT INTO feature_sink
SELECT source_dataset, bank_id, account_id, currency, window_start, window_end,
       'hourly-v1', SUM(in_count), SUM(out_count), SUM(in_amount), SUM(out_amount),
       SUM(in_amount - out_amount), COUNT(DISTINCT sender), COUNT(DISTINCT receiver)
FROM TABLE(TUMBLE(TABLE account_movements, DESCRIPTOR(movement_time), INTERVAL '1' HOUR))
GROUP BY source_dataset, bank_id, account_id, currency, window_start, window_end;
