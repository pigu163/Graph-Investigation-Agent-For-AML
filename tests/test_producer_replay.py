"""Small replay checks: run with python -m unittest discover -s tests."""

import csv
import tempfile
import unittest
from pathlib import Path

from graph_investigation_agent_for_aml.producer import read_rows


class ReplayOrderTest(unittest.TestCase):
    def test_sort_preserves_original_ids_and_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            with path.open("w", newline="") as target:
                writer = csv.writer(target)
                writer.writerow(["header"] * 11)
                for timestamp in ("2022/09/01 01:00", "2022/09/01 00:00", "2022/08/01 00:00"):
                    writer.writerow([timestamp] + ["value"] * 10)
            ordered = list(read_rows(path, 2, True))
            self.assertEqual([number for number, _ in ordered], [2, 1])
            self.assertEqual([row[0] for _, row in ordered], ["2022/09/01 00:00", "2022/09/01 01:00"])
            self.assertEqual([number for number, _ in read_rows(path, 2, False)], [1, 2])

    def test_bad_column_count_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.csv"
            path.write_text(",".join(["header"] * 11) + "\nonly,two\n")
            for sorting in (False, True):
                with self.assertRaises(ValueError):
                    list(read_rows(path, None, sorting))


if __name__ == "__main__":
    unittest.main()
