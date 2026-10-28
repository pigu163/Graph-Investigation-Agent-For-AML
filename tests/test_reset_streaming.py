import unittest
from unittest.mock import patch

from scripts import reset_streaming


class ResetStreamingTest(unittest.TestCase):
    def test_both_modes_clean_flink_state_as_root_without_removing_services(self):
        for stop in (False, True):
            with self.subTest(stop=stop), \
                    patch.object(reset_streaming, "compose", return_value="") as compose, \
                    patch.object(reset_streaming, "kafka", side_effect=lambda tool, *args:
                                 "aml-transactions:0:0" if tool == "kafka-get-offsets" else ""), \
                    patch.object(reset_streaming, "sql", side_effect=lambda statement:
                                 "0|0" if "SELECT (SELECT count(*)" in statement else ""), \
                    patch.object(reset_streaming, "wait_for_transactions_table"):
                reset_streaming.reset(stop=stop)

            cleanup = [call.args for call in compose.call_args_list
                       if call.args and call.args[0] == "run"]
            self.assertEqual(len(cleanup), 1)
            self.assertEqual(cleanup[0][:9], ("run", "--rm", "--no-deps", "--user", "0",
                                              "--entrypoint", "sh", "flink-submit", "-c"))
            self.assertIn("find /opt/flink/state -mindepth 1 -delete", cleanup[0][9])
            self.assertIn("test -z", cleanup[0][9])
            self.assertFalse(any(call.args and call.args[0] in ("rm", "down")
                                 for call in compose.call_args_list))


if __name__ == "__main__":
    unittest.main()
