"""Compare every Flink window feature with the original CSV and PostgreSQL.

Run after a bounded job has FINISHED. PGHOST/PGDATABASE/PGUSER/PGPASSWORD
select the local PostgreSQL instance, just as in check_raw_writer.py.
"""

import argparse
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import islice
from pathlib import Path

import psycopg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--closed-only", action="store_true",
                        help="Check only windows closed by the sample's one-minute watermark")
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")

    # This reference calculation does not import the Flink job or producer logic.
    expected = {}
    with args.file.open(newline="", encoding="utf-8") as source:
        rows = csv.reader(source)
        next(rows)
        sample = list(islice(rows, args.limit))
    for row in sample:
        start = datetime.strptime(row[0], "%Y/%m/%d %H:%M").replace(minute=0)
        external = (row[1], row[2]) != (row[3], row[4])
        for direction, bank, account, currency, amount, other_bank, other_account in (
            ("in", row[3], row[4], row[6], row[5], row[1], row[2]),
            ("out", row[1], row[2], row[8], row[7], row[3], row[4]),
        ):
            key = (args.file.stem, bank, account, currency, start, start + timedelta(hours=1))
            value = expected.setdefault(key, {
                "in_count": 0, "out_count": 0,
                "in_sum": Decimal(0), "out_sum": Decimal(0),
                "in_parties": set(), "out_parties": set(),
            })
            value[f"{direction}_count"] += 1
            value[f"{direction}_sum"] += Decimal(amount)
            if external:
                value[f"{direction}_parties"].add((other_bank, other_account))

    reference = {
        key: (v["in_count"], v["out_count"], v["in_sum"], v["out_sum"],
              v["in_sum"] - v["out_sum"], len(v["in_parties"]), len(v["out_parties"]))
        for key, v in expected.items()
    }
    if args.closed_only:
        watermark = max(datetime.strptime(r[0], "%Y/%m/%d %H:%M") for r in sample) - timedelta(minutes=1)
        reference = {k: v for k, v in reference.items() if k[5] <= watermark}
        assert reference, "Sample does not advance event time enough to close a window"
    with psycopg.connect(connect_timeout=10) as connection:
        ids = {r[0] for r in connection.execute(
            "SELECT transaction_id FROM transactions WHERE split_part(transaction_id, ':', 1) = %s",
            (args.file.stem,),
        )}
        assert ids == {f"{args.file.stem}:{i}" for i in range(1, len(sample) + 1)}, "Raw transaction IDs differ"
        result = connection.execute("""
            SELECT source_dataset, bank_id, account_id, currency, window_start, window_end,
                   in_tx_count, out_tx_count, in_amount_sum, out_amount_sum,
                   net_inflow, fan_in, fan_out
            FROM account_window_features WHERE source_dataset = %s AND feature_version = 'hourly-v1'
        """, (args.file.stem,)).fetchall()
        actual = {tuple(row[:6]): tuple(row[6:]) for row in result}
        assert actual.keys() == reference.keys(), (
            f"Window keys differ: missing={len(reference.keys() - actual.keys())}, "
            f"extra={len(actual.keys() - reference.keys())}"
        )
        for key, value in reference.items():
            assert actual[key] == value, f"Feature mismatch at {key}: {actual[key]} != {value}"

        # A second, independent SQL aggregation checks the persisted business data.
        sql_rows = connection.execute("""
            WITH movements AS (
                SELECT split_part(transaction_id, ':', 1) AS dataset,
                       source_bank AS bank, source_account AS account, payment_currency AS currency,
                       date_trunc('hour', event_time) AS start,
                       0::bigint AS inc, 1::bigint AS outc, 0::numeric AS ina, amount_paid AS outa,
                       NULL::text AS sender,
                       CASE WHEN (source_bank, source_account) <> (destination_bank, destination_account)
                            THEN destination_bank || ':' || destination_account END AS receiver
                FROM transactions WHERE split_part(transaction_id, ':', 1) = %s
                UNION ALL
                SELECT split_part(transaction_id, ':', 1), destination_bank, destination_account,
                       receiving_currency, date_trunc('hour', event_time),
                       1::bigint, 0::bigint, amount_received, 0::numeric,
                       CASE WHEN (source_bank, source_account) <> (destination_bank, destination_account)
                            THEN source_bank || ':' || source_account END, NULL::text
                FROM transactions WHERE split_part(transaction_id, ':', 1) = %s
            )
            SELECT dataset, bank, account, currency, start, start + interval '1 hour',
                   sum(inc), sum(outc), sum(ina), sum(outa), sum(ina) - sum(outa),
                   count(DISTINCT sender), count(DISTINCT receiver)
            FROM movements GROUP BY dataset, bank, account, currency, start
        """, (args.file.stem, args.file.stem)).fetchall()
        sql_result = {tuple(r[:6]): tuple(r[6:]) for r in sql_rows}
        if args.closed_only:
            sql_result = {k: v for k, v in sql_result.items() if k[5] <= watermark}
        assert sql_result == actual, "SQL recalculation differs"
    print(f"Verified {len(sample)} source transactions and all {len(actual)} account/currency/hour windows for {args.file.stem}")


if __name__ == "__main__":
    main()
