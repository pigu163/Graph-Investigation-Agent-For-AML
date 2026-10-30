"""Bounded, evidence-oriented queries against the stored transaction table.

Pass a psycopg connection explicitly. Event-time inputs use the source's naive
timestamps; results are JSON-compatible and decimal amounts remain exact strings.
"""

from datetime import UTC, datetime
from decimal import Decimal

from psycopg.rows import dict_row

VERSION = "1.0"
EVIDENCE_LIMIT = 20
MAX_PAGE_SIZE = 1000

# Explicit columns exclude the evaluation label and the label-bearing raw JSON.
TRANSACTION_COLUMNS = """
    t.transaction_id, t.source_dataset, t.source_file, t.source_row,
    t.event_time, t.event_time_kind, t.source_bank, t.source_account,
    t.destination_bank, t.destination_account, t.amount_paid,
    t.payment_currency, t.amount_received, t.receiving_currency,
    t.payment_format, t.ingested_at, t.stored_at,
    t.kafka_topic, t.kafka_partition, t.kafka_offset
"""

# A self-transfer is one transaction but represents one movement in each direction.
MOVEMENTS = """
    WITH movements AS (
        SELECT transaction_id, event_time, 'in' AS direction,
               source_bank AS counterparty_bank, source_account AS counterparty_account,
               receiving_currency AS currency, amount_received AS amount,
               (source_bank = destination_bank AND source_account = destination_account) AS is_self
        FROM transactions
        WHERE source_dataset = %(dataset)s AND event_time >= %(start)s AND event_time < %(end)s
          AND destination_bank = %(bank)s AND destination_account = %(account)s
          AND (%(direction)s = 'all' OR %(direction)s = 'in')
          AND (%(currency)s::text IS NULL OR receiving_currency = %(currency)s)
        UNION ALL
        SELECT transaction_id, event_time, 'out' AS direction,
               destination_bank AS counterparty_bank, destination_account AS counterparty_account,
               payment_currency AS currency, amount_paid AS amount,
               (source_bank = destination_bank AND source_account = destination_account) AS is_self
        FROM transactions
        WHERE source_dataset = %(dataset)s AND event_time >= %(start)s AND event_time < %(end)s
          AND source_bank = %(bank)s AND source_account = %(account)s
          AND (%(direction)s = 'all' OR %(direction)s = 'out')
          AND (%(currency)s::text IS NULL OR payment_currency = %(currency)s)
    )
"""


def _json_value(value):
    if isinstance(value, (datetime, Decimal)):
        return str(value) if isinstance(value, Decimal) else value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _params(dataset, bank, account, start, end, direction="all", currency=None):
    for name, value in (("dataset", dataset), ("bank", bank), ("account", account)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a nonempty string")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise TypeError("start and end must be datetime values")
    if start.utcoffset() is not None or end.utcoffset() is not None:
        raise ValueError("source event timestamps must be timezone-naive")
    if start >= end:
        raise ValueError("start must be earlier than end")
    if direction not in ("in", "out", "all"):
        raise ValueError("direction must be in, out, or all")
    if currency is not None and (not isinstance(currency, str) or not currency.strip()):
        raise ValueError("currency must be a nonempty string or None")
    return {"dataset": dataset, "bank": bank, "account": account, "start": start,
            "end": end, "direction": direction, "currency": currency}


def _page(params, limit, offset):
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    if type(offset) is not int or offset < 0:
        raise ValueError("offset must be a nonnegative integer")
    return {**params, "limit": limit + 1, "offset": offset}


def _response(tool, query, result, **extra):
    return _json_value({
        "tool": tool, "tool_version": VERSION,
        "queried_at": datetime.now(UTC), "source": "postgresql.transactions",
        "query": query, "result": result, **extra,
    })


def get_transactions(connection, dataset, bank, account, start, end, *,
                     direction="all", currency=None, limit=100, offset=0):
    """Return one row per transaction involving the account in [start, end)."""
    params = _params(dataset, bank, account, start, end, direction, currency)
    page = _page(params, limit, offset)
    statement = f"""
        SELECT {TRANSACTION_COLUMNS},
            CASE WHEN t.source_bank = %(bank)s AND t.source_account = %(account)s
                       AND t.destination_bank = %(bank)s AND t.destination_account = %(account)s
                 THEN 'self'
                 WHEN t.destination_bank = %(bank)s AND t.destination_account = %(account)s
                 THEN 'in' ELSE 'out' END AS direction
        FROM transactions t
        WHERE t.source_dataset = %(dataset)s AND t.event_time >= %(start)s AND t.event_time < %(end)s
          AND ((t.destination_bank = %(bank)s AND t.destination_account = %(account)s
                AND (%(direction)s = 'all' OR %(direction)s = 'in')
                AND (%(currency)s::text IS NULL OR t.receiving_currency = %(currency)s))
            OR (t.source_bank = %(bank)s AND t.source_account = %(account)s
                AND (%(direction)s = 'all' OR %(direction)s = 'out')
                AND (%(currency)s::text IS NULL OR t.payment_currency = %(currency)s)))
        ORDER BY t.event_time, t.transaction_id LIMIT %(limit)s OFFSET %(offset)s
    """
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(statement, page)
        rows = cursor.fetchall()
    return _response("get_transactions", params, rows[:limit],
                     pagination={"limit": limit, "offset": offset, "has_more": len(rows) > limit},
                     limitations=["Event timestamps use the source's timezone semantics.",
                                  "Offset pages can change if transactions are added between queries."])


def get_transaction_details(connection, dataset, transaction_id):
    """Return a source-scoped transaction and its provenance, or None if absent."""
    for name, value in (("dataset", dataset), ("transaction_id", transaction_id)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a nonempty string")
    query = {"dataset": dataset, "transaction_id": transaction_id}
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(f"SELECT {TRANSACTION_COLUMNS} FROM transactions t "
                       "WHERE t.source_dataset = %(dataset)s AND t.transaction_id = %(transaction_id)s",
                       query)
        row = cursor.fetchone()
    return _response("get_transaction_details", query, row,
                     limitations=["Event timestamps use the source's timezone semantics."])


def get_account_profile(connection, dataset, bank, account, start, end, *, currency=None):
    """Summarize complete-window incoming/outgoing movements by currency."""
    params = _params(dataset, bank, account, start, end, currency=currency)
    with connection.cursor(row_factory=dict_row) as cursor:
        # One statement ensures direction and currency totals use the same snapshot.
        cursor.execute(MOVEMENTS + """
            , ranked AS (
                SELECT *, row_number() OVER (PARTITION BY direction, currency
                    ORDER BY event_time, transaction_id) AS evidence_rank FROM movements
            )
            SELECT direction, currency, GROUPING(currency) AS direction_total,
                   count(*) AS transaction_count,
                   CASE WHEN GROUPING(currency) = 0 THEN sum(amount) END AS amount_sum,
                   count(DISTINCT (counterparty_bank, counterparty_account))
                       FILTER (WHERE NOT is_self) AS distinct_counterparty_count,
                   array_agg(transaction_id ORDER BY event_time, transaction_id)
                       FILTER (WHERE evidence_rank <= %(evidence_limit)s) AS transaction_ids
            FROM ranked
            GROUP BY GROUPING SETS ((direction), (direction, currency))
            ORDER BY direction, direction_total DESC, currency
        """, {**params, "evidence_limit": EVIDENCE_LIMIT})
        rows = cursor.fetchall()
    directions = [{"direction": row["direction"],
                   "transaction_count": row["transaction_count"],
                   "distinct_counterparty_count": row["distinct_counterparty_count"]}
                  for row in rows if row["direction_total"]]
    currencies = [row for row in rows if not row.pop("direction_total")]
    for row in currencies:
        row["evidence_truncated"] = row["transaction_count"] > EVIDENCE_LIMIT
    return _response("get_account_profile", params,
                     {"directions": directions, "currencies": currencies},
                     limitations=["Self-transfers count as both in and out but not external counterparties.",
                                  "Amounts are grouped by movement currency; no currency conversion is performed.",
                                  "Sampled transaction IDs are bounded; use get_transactions for full evidence."])


def get_counterparties(connection, dataset, bank, account, start, end, *,
                       direction="all", currency=None, limit=100, offset=0):
    """Summarize external counterparties by direction and movement currency."""
    params = _params(dataset, bank, account, start, end, direction, currency)
    page = _page(params, limit, offset)
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(MOVEMENTS + """
            , ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY direction, counterparty_bank, counterparty_account, currency
                    ORDER BY event_time, transaction_id) AS evidence_rank
                FROM movements WHERE NOT is_self
            )
            SELECT direction, counterparty_bank, counterparty_account, currency,
                   count(*) AS transaction_count, sum(amount) AS amount_sum,
                   min(event_time) AS first_event_time, max(event_time) AS last_event_time,
                   array_agg(transaction_id ORDER BY event_time, transaction_id)
                       FILTER (WHERE evidence_rank <= %(evidence_limit)s) AS transaction_ids
            FROM ranked
            GROUP BY direction, counterparty_bank, counterparty_account, currency
            ORDER BY direction, counterparty_bank, counterparty_account, currency
            LIMIT %(limit)s OFFSET %(offset)s
        """, {**page, "evidence_limit": EVIDENCE_LIMIT})
        rows = cursor.fetchall()
    for row in rows:
        row["evidence_truncated"] = row["transaction_count"] > EVIDENCE_LIMIT
    return _response("get_counterparties", params, rows[:limit],
                     pagination={"limit": limit, "offset": offset, "has_more": len(rows) > limit},
                     limitations=["Self-transfers are excluded from external counterparties.",
                                  "Sampled transaction IDs are bounded; use get_transactions for full evidence.",
                                  "Offset pages can change if transactions are added between queries."])
