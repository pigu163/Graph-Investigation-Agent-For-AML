"""Train a transaction-ranking model on the IBM HI-Small CSV."""
import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score
from xgboost import XGBClassifier

FEATURES = ["amount_paid", "count_1h", "count_24h", "amount_1h", "amount_24h"]


def prepare(connection, source):
    connection.execute("""
        CREATE TEMP TABLE transactions AS
        SELECT strptime(Timestamp, '%Y/%m/%d %H:%M') AS event_time,
               "From Bank" AS bank, Account AS account,
               "Payment Currency" AS currency, "Amount Paid"::DOUBLE AS amount_paid,
               "Is Laundering"::INTEGER AS label
        FROM read_csv(?, all_varchar=true)
    """, [str(source)])
    connection.execute("""
        CREATE TEMP TABLE minutes AS
        SELECT event_time, bank, account, currency, count(*) AS n, sum(amount_paid) AS amount
        FROM transactions GROUP BY ALL
    """)
    connection.execute("""
        CREATE TEMP TABLE histories AS
        SELECT event_time, bank, account, currency,
            coalesce(sum(n) OVER w1, 0) AS count_1h,
            coalesce(sum(n) OVER w24, 0) AS count_24h,
            coalesce(sum(amount) OVER w1, 0) AS amount_1h,
            coalesce(sum(amount) OVER w24, 0) AS amount_24h
        FROM minutes
        WINDOW w1 AS (PARTITION BY bank, account, currency ORDER BY event_time
                      RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE PRECEDING),
               w24 AS (PARTITION BY bank, account, currency ORDER BY event_time
                       RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND INTERVAL 1 MINUTE PRECEDING)
    """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("SET threads=4")
    prepare(connection, args.input)
    train_end, valid_end = connection.execute(
        "SELECT quantile_disc(event_time, 0.7), quantile_disc(event_time, 0.85) FROM transactions"
    ).fetchone()
    splits = []
    for where, params in (("t.event_time < ?", [train_end]),
                          ("t.event_time >= ? AND t.event_time < ?", [train_end, valid_end]),
                          ("t.event_time >= ?", [valid_end])):
        result = connection.execute(f"""
            SELECT t.amount_paid, h.count_1h, h.count_24h, h.amount_1h, h.amount_24h, t.label
            FROM transactions t JOIN histories h USING (event_time, bank, account, currency)
            WHERE {where}
            ORDER BY t.event_time, t.bank, t.account, t.currency, t.amount_paid, t.label
        """, params).fetchnumpy()
        splits.append((np.column_stack([result[name] for name in FEATURES]).astype(np.float32), result["label"]))
    connection.close()
    train_x, train_y = splits[0]
    valid_x, valid_y = splits[1]
    test_x, test_y = splits[2]
    model = XGBClassifier(n_estimators=300, max_depth=4, learning_rate=0.05,
                          tree_method="hist", n_jobs=4, random_state=42,
                          scale_pos_weight=float((train_y == 0).sum() / max(1, (train_y == 1).sum())),
                          early_stopping_rounds=20, eval_metric="aucpr")
    model.fit(train_x, train_y, eval_set=[(valid_x, valid_y)], verbose=False)
    predictions = model.predict_proba(test_x)[:, 1]
    model.save_model(args.output / "model.json")
    metadata = {"features": FEATURES, "history": "sender and payment currency, strictly before current minute, inclusive 1h/24h lower bound",
                "train_end": train_end.isoformat(), "validation_end": valid_end.isoformat(),
                "rows": [len(y) for _, y in splits], "positives": [int(sum(y)) for _, y in splits],
                "test_average_precision": float(average_precision_score(test_y, predictions)),
                "test_positive_rate": float(np.mean(test_y)),
                "test_precision_at_20": float(np.mean(test_y[np.argsort(predictions)[-20:]])),
                "test_roc_auc": float(roc_auc_score(test_y, predictions)), "best_iteration": model.best_iteration}
    (args.output / "metrics.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
