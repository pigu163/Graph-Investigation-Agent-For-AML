"""Score archived transactions that triggered a streaming rule alert."""
import hashlib
import os
import time
from pathlib import Path

import numpy as np
import psycopg
from psycopg.types.json import Jsonb
from xgboost import XGBClassifier


def main():
    model_path = Path(os.getenv("AML_MODEL_PATH", "/models/model.json"))
    version = hashlib.sha256(model_path.read_bytes()).hexdigest()
    model = XGBClassifier()
    model.load_model(model_path)

    cursor = ("", "")
    while True:
        with psycopg.connect(connect_timeout=5) as conn:
            rows = conn.execute("""
                SELECT DISTINCT t.source_dataset, t.transaction_id, t.event_time, t.amount_paid,
                       t.source_bank, t.source_account, t.payment_currency, s.features
                FROM risk_alerts a
                JOIN transactions t ON t.source_dataset = a.source_dataset AND t.transaction_id = a.transaction_id
                LEFT JOIN transaction_scores s ON s.source_dataset = t.source_dataset
                  AND s.transaction_id = t.transaction_id AND s.model_version = %s
                WHERE (t.source_dataset, t.transaction_id) > (%s, %s)
                ORDER BY t.source_dataset, t.transaction_id LIMIT 100
            """, (version, *cursor)).fetchall()
            for dataset, transaction_id, event_time, amount, bank, account, currency, previous in rows:
                count_1h, count_24h, amount_1h, amount_24h = conn.execute("""
                    SELECT count(*) FILTER (WHERE event_time >= %s - interval '1 hour'),
                           count(*),
                           coalesce(sum(amount_paid) FILTER (WHERE event_time >= %s - interval '1 hour'), 0),
                           coalesce(sum(amount_paid), 0)
                    FROM transactions
                    WHERE source_dataset = %s AND source_bank = %s AND source_account = %s
                      AND payment_currency = %s AND event_time >= %s - interval '24 hours'
                      AND event_time < %s
                """, (event_time, event_time, dataset, bank, account, currency, event_time, event_time)).fetchone()
                feature_values = [float(amount), count_1h, count_24h, float(amount_1h), float(amount_24h)]
                if feature_values == previous:
                    continue
                features = np.array([feature_values], dtype=np.float32)
                score = float(model.predict_proba(features)[0, 1])
                conn.execute("""
                    INSERT INTO transaction_scores (source_dataset, transaction_id, model_version, feature_cutoff, score, features)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_dataset, transaction_id, model_version) DO UPDATE
                    SET score = EXCLUDED.score, features = EXCLUDED.features, scored_at = clock_timestamp()
                """, (dataset, transaction_id, version, event_time, score, Jsonb(feature_values)))
        if rows:
            cursor = (rows[-1][0], rows[-1][1])
        else:
            cursor = ("", "")
            time.sleep(2)


if __name__ == "__main__":
    main()
