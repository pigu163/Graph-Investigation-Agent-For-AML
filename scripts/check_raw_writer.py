"""Compare a CSV sample with the actual rows saved in PostgreSQL.

Use the PGHOST, PGDATABASE, PGUSER and PGPASSWORD environment variables.
Run after Raw Writer has consumed the sample:
    python scripts/check_raw_writer.py dataset/LI-Small/LI-Small_Trans.csv
"""

import argparse
import csv
from datetime import datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path

import psycopg


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be greater than zero")

    checked = 0
    with args.file.open(newline="", encoding="utf-8") as source:
        rows = csv.reader(source)
        next(rows)
        with psycopg.connect(autocommit=True) as connection:
            for number, row in enumerate(islice(rows, args.limit), start=1):
                transaction_id = f"{args.file.stem}:{number}"
                stored = connection.execute(
                    """SELECT event_time, source_bank, source_account,
                        destination_bank, destination_account, amount_received,
                        receiving_currency, amount_paid, payment_currency,
                        payment_format, is_laundering, raw_event
                    FROM transactions WHERE transaction_id = %s""",
                    (transaction_id,),
                ).fetchone()
                assert stored is not None, f"Missing transaction: {transaction_id}"

                # Compare database values to the source CSV, without using writer code.
                expected = list(row)
                expected[0] = datetime.strptime(row[0], "%Y/%m/%d %H:%M")
                expected[5] = Decimal(row[5])
                expected[7] = Decimal(row[7])
                expected[10] = int(row[10])
                assert stored[:11] == tuple(expected), f"Mismatch: {transaction_id}"
                assert stored[11]["transaction_id"] == transaction_id
                checked += 1
    print(f"Verified {checked} stored transactions against {args.file}")


if __name__ == "__main__":
    main()
