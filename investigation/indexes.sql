-- Optional account/time indexes for measured investigation workloads.
-- Creating indexes on a large or active table has write and storage costs.
CREATE INDEX IF NOT EXISTS idx_transactions_source_time
    ON transactions (source_dataset, source_bank, source_account, event_time);

CREATE INDEX IF NOT EXISTS idx_transactions_destination_time
    ON transactions (source_dataset, destination_bank, destination_account, event_time);
