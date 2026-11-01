"""The selected alert must identify an archived transaction and the requested party."""
from datetime import datetime
from decimal import Decimal

import pytest
from fastapi import HTTPException

from graph_investigation_agent_for_aml.api import investigation_case


class Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class Connection:
    def __init__(self, stored):
        self.stored = stored

    def execute(self, sql, params):
        if "FROM risk_alerts" in sql:
            return Result({"payload": {"source_dataset": "demo", "transaction_id": "demo:1", "transaction": {
                "event_time": "2022/09/01 09:04", "source_bank": "01", "source_account": "A",
                "destination_bank": "02", "destination_account": "B", "amount_paid": "10.00",
                "payment_currency": "USD", "amount_received": "10.00", "receiving_currency": "USD"}}})
        return Result(self.stored)


def test_alert_investigation_selects_the_archived_sender_or_recipient():
    transaction = {"source_bank": "01", "source_account": "A", "destination_bank": "02",
                   "destination_account": "B", "event_time": datetime(2022, 9, 1, 9, 4),
                   "amount_paid": Decimal("10.00000000"), "payment_currency": "USD",
                   "amount_received": Decimal("10.00000000"), "receiving_currency": "USD"}
    sender = investigation_case(Connection(transaction), "alert-1", "sender")
    recipient = investigation_case(Connection(transaction), "alert-1", "recipient")
    assert (sender["bank"], sender["account"]) == ("01", "A")
    assert (recipient["bank"], recipient["account"]) == ("02", "B")
    assert sender["alert_id"] == recipient["alert_id"] == "alert-1"
    assert sender["end"] == "2022-09-01T09:04:00.000001"
    with pytest.raises(HTTPException) as missing:
        investigation_case(Connection(None), "alert-1", "sender")
    assert missing.value.status_code == 409
    with pytest.raises(HTTPException) as mismatch:
        investigation_case(Connection({**transaction, "source_account": "other"}), "alert-1", "sender")
    assert mismatch.value.status_code == 409
