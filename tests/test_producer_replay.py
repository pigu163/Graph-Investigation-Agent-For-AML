"""Small replay checks: run with python -m unittest discover -s tests."""

import csv
import tempfile
import unittest
from pathlib import Path

from graph_investigation_agent_for_aml.producer import (
    IBM_HEADER,
    build_event,
    read_rows,
)
from graph_investigation_agent_for_aml.raw_writer import (
    business_values,
    transaction_values,
)

SAMPLE = ["2022/09/01 00:08", "011", "A001", "012", "A002",
          "12.34000001", "US Dollar", "10.00000001", "Euro", "Wire", "0"]


class ReplayOrderTest(unittest.TestCase):
    def test_sort_preserves_original_ids_and_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            with path.open("w", newline="") as target:
                writer = csv.writer(target)
                writer.writerow(IBM_HEADER)
                for timestamp in ("2022/09/01 01:00", "2022/09/01 00:00", "2022/08/01 00:00"):
                    writer.writerow([timestamp] + ["value"] * 10)
            ordered = list(read_rows(path, 2, True))
            self.assertEqual([number for number, _ in ordered], [2, 1])
            self.assertEqual([row[0] for _, row in ordered], ["2022/09/01 00:00", "2022/09/01 01:00"])
            self.assertEqual([number for number, _ in read_rows(path, 2, False)], [1, 2])

    def test_bad_column_count_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.csv"
            path.write_text(",".join(IBM_HEADER) + "\nonly,two\n")
            for sorting in (False, True):
                with self.assertRaises(ValueError):
                    list(read_rows(path, None, sorting))

    def test_wrong_header_order_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.csv"
            header = IBM_HEADER.copy()
            header[1], header[2] = header[2], header[1]
            path.write_text(",".join(header) + "\n" + ",".join(SAMPLE) + "\n")
            with self.assertRaises(ValueError):
                list(read_rows(path, None, False))

    def test_ibm_mapping_preserves_source_values_and_time(self):
        event = build_event(Path("LI-Small_Trans.csv"), 7, SAMPLE)
        values = transaction_values(event)
        self.assertEqual(event["source_bank"], "011")
        self.assertEqual(event["destination_account"], "A002")
        self.assertEqual(str(values["amount_received"]), "12.34000001")
        self.assertEqual(event["receiving_currency"], "US Dollar")
        self.assertEqual(event["payment_currency"], "Euro")
        self.assertIsNone(values["event_time"].tzinfo)
        self.assertIsNotNone(values["ingested_at"].utcoffset())
        self.assertEqual(event["transaction_id"], "LI-Small_Trans:7")
        self.assertEqual(event["source_row"], 7)
        other = build_event(Path("HI-Small_Trans.csv"), 7, SAMPLE)
        self.assertNotEqual(event["transaction_id"], other["transaction_id"])
        replay = build_event(Path("LI-Small_Trans.csv"), 7, SAMPLE)
        self.assertEqual(event["transaction_id"], replay["transaction_id"])

    def test_writer_rejects_inconsistent_source_or_invented_timezone(self):
        event = build_event(Path("LI-Small_Trans.csv"), 1, SAMPLE)
        for change in ({"source_row": 2}, {"source_dataset": "HI-Small_Trans"},
                       {"event_time_kind": "offset"}, {"ingested_at": "2026-10-28T10:00:00"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                transaction_values(event | change)

    def test_duplicate_comparison_ignores_ingestion_time_but_detects_conflicts(self):
        event = build_event(Path("LI-Small_Trans.csv"), 1, SAMPLE)
        original = business_values(transaction_values(event))
        replay = event | {"ingested_at": "2026-10-28T10:00:00+00:00",
                          "amount_received": "12.340000010"}
        self.assertEqual(original, business_values(transaction_values(replay)))
        for change in ({"amount_received": "900"}, {"event_time": "2022/09/01 01:08"},
                       {"destination_account": "A003"}):
            with self.subTest(change=change):
                self.assertNotEqual(original, business_values(transaction_values(event | change)))


if __name__ == "__main__":
    unittest.main()
