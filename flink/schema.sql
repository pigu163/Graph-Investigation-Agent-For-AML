-- Source timestamps are intentionally stored without an invented timezone.
CREATE TABLE IF NOT EXISTS account_window_features (
    source_dataset TEXT NOT NULL,
    bank_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    feature_version TEXT NOT NULL,
    in_tx_count BIGINT NOT NULL,
    out_tx_count BIGINT NOT NULL,
    in_amount_sum NUMERIC(38, 8) NOT NULL,
    out_amount_sum NUMERIC(38, 8) NOT NULL,
    net_inflow NUMERIC(38, 8) NOT NULL,
    fan_in BIGINT NOT NULL,
    fan_out BIGINT NOT NULL,
    PRIMARY KEY (source_dataset, bank_id, account_id, currency,
                 window_start, window_end, feature_version)
);
