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

-- One record per affected account movement; the original event remains queryable.
CREATE TABLE IF NOT EXISTS late_transaction_movements (
    transaction_id TEXT NOT NULL,
    source_dataset TEXT NOT NULL,
    bank_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    event_time TIMESTAMP NOT NULL,
    raw_event JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_dataset, bank_id, account_id, currency, direction)
);

CREATE TABLE IF NOT EXISTS flink_transaction_conflicts (
    transaction_id TEXT NOT NULL,
    original_fingerprint TEXT NOT NULL,
    conflicting_fingerprint TEXT NOT NULL,
    conflicting_event JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, original_fingerprint, conflicting_fingerprint)
);

-- One idempotent rule crossing; payload contains only public transaction fields.
CREATE TABLE IF NOT EXISTS risk_alerts (
    alert_id TEXT PRIMARY KEY,
    source_dataset TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_risk_alerts_detected ON risk_alerts (detected_at DESC, alert_id);

CREATE TABLE IF NOT EXISTS investigation_cases (
    case_id UUID PRIMARY KEY,
    case_data JSONB NOT NULL,
    status TEXT NOT NULL,
    report JSONB,
    error TEXT,
    disposition JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_scores (
    source_dataset TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    feature_cutoff TIMESTAMP NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    features JSONB,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_dataset, transaction_id, model_version)
);
ALTER TABLE transaction_scores ADD COLUMN IF NOT EXISTS features JSONB;

CREATE TABLE IF NOT EXISTS investigation_queue (
    alert_id TEXT NOT NULL REFERENCES risk_alerts(alert_id),
    party TEXT NOT NULL CHECK (party IN ('sender', 'recipient')),
    priority INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    case_id UUID,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (alert_id, party)
);
ALTER TABLE investigation_queue ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now();
