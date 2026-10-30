# Transaction investigation queries

The four functions in `src/graph_investigation_agent_for_aml/investigation.py`
read PostgreSQL's `transactions` table and return JSON-compatible evidence for
an investigator or Agent. Install the project dependencies and provide the
standard PostgreSQL `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD`
environment variables. For example:

```python
from datetime import datetime

import psycopg

from graph_investigation_agent_for_aml.investigation import (
    get_account_profile,
    get_counterparties,
    get_transaction_details,
    get_transactions,
)

with psycopg.connect() as connection:
    account = (connection, "LI-Small_Trans", "20", "1234",
               datetime(2022, 9, 1), datetime(2022, 9, 2))
    transactions = get_transactions(*account, direction="all", limit=100)
    profile = get_account_profile(*account)
    counterparties = get_counterparties(*account)
    if transactions["result"]:
        details = get_transaction_details(
            connection, "LI-Small_Trans", transactions["result"][0]["transaction_id"]
        )
```

The account and dates above are illustrative; use values present in the source.
The caller owns the connection and transaction lifetime. The module executes
only predefined parameterized SELECT queries. It never exposes the dataset's
`is_laundering` label or its label-bearing `raw_event` JSON.

## Business meaning

| Function | Question | Filter and grouping | Output |
| --- | --- | --- | --- |
| `get_transactions` | Which transactions involve this account? | Dataset, bank, account, `[start, end)`, optional direction/currency; sorted by event time and ID. | One row per transaction, with both amount/currency pairs and source provenance. |
| `get_account_profile` | How much activity and how many counterparties did the account have? | Full matching window, split by direction and currency; distinct counterparties use bank plus account. | Movement counts, exact amount sums, distinct external counterparties and up to 20 transaction IDs per direction/currency. |
| `get_counterparties` | Which other accounts transacted with it? | Full matching window, grouped by direction, counterparty bank/account and movement currency. | Counts, amounts, first/last event time and up to 20 transaction IDs per group. |
| `get_transaction_details` | What supports a particular transaction ID? | Exact dataset and transaction ID. | Whitelisted transaction fields, source file/row and Kafka/storage provenance, or `null` when absent. |

Incoming uses destination bank/account, received amount and receiving currency;
outgoing uses source bank/account, paid amount and payment currency. No currency
conversion is performed. A self-transfer appears once in transaction details,
but counts once in each movement direction; it is excluded from *external*
counterparty counts and lists. The account profile is derived only from
transactions; it does not assert customer identity or KYC information.

All account functions identify an account by dataset, bank and account. The
source timestamps are timezone-naive, so the API requires naive `datetime`
values and does not assume a timezone. The producer records the original
`event_time_kind` in each transaction. Amounts are decimal strings to preserve
precision. Different accounts or datasets with the same account number are not
merged.

`get_transactions` and `get_counterparties` support `direction` (`in`, `out`,
`all`), optional currency, `limit` (1–1000) and `offset`. A self-transfer in a
transaction result is labeled `self` even when one direction was requested.
Pagination reports `has_more`; pages can shift if data changes between calls.
Profile counts and sums cover the full filter, not a transaction page. Evidence
ID samples are capped at 20 per group with `evidence_truncated` signaling more.
To inspect all supporting records, page through the matching account transactions
and check the relevant counterparty/direction/currency.

## Verification

Use the actual pipeline with `tests/fixtures/streaming-boundaries.csv` and a
5,000-row LI-Small replay, then set `AML_TEST_DATABASE_URL` to the corresponding
disposable database and run `pytest tests/test_investigation.py`. The tests
check self-transfers, differing paid/received currencies, decimal precision,
half-open time boundaries, pagination, scoped account and transaction IDs,
bounded evidence, label exclusion and actual IBM transaction retrieval. The
test-created evidence-limit rows are rolled back.

The live streaming job can revise window features after late events; these
functions query archived transactions rather than assuming window features are
complete. The `transactions` table does not currently provide an account/time
index by default; measure query performance and add indexes if the dataset size warrants it.

## Bounded graph and temporal investigation

`graph_investigation.py` expands a local directed NetworkX `MultiDiGraph` in
batches from PostgreSQL and returns a JSON-compatible graph with the original
transaction IDs, timestamps, direction, amounts, currencies, and provenance.
Its functions consume the returned graph without requiring another database call:

```python
from graph_investigation_agent_for_aml.graph_investigation import (
    compute_graph_features,
    detect_cycles,
    detect_fan_in,
    detect_fan_out,
    detect_rapid_pass_through,
    detect_reciprocal_flows,
    expand_transaction_graph,
    trace_fund_flow,
)

with psycopg.connect() as connection:
    account = (connection, "LI-Small_Trans", "20", "1234",
               datetime(2022, 9, 1), datetime(2022, 9, 2))
    graph = expand_transaction_graph(*account, hops=2, direction="all",
                                     max_nodes=500, max_edges=2000)
    features = compute_graph_features(graph)
    fan_in = detect_fan_in(graph, min_counterparties=3)
    fan_out = detect_fan_out(graph, min_counterparties=3)
    reciprocal = detect_reciprocal_flows(graph, max_delay_seconds=86400)
    cycles = detect_cycles(graph, max_hops=3, max_gap_seconds=86400)
    paths = trace_fund_flow(graph, max_hops=3, max_gap_seconds=86400)
    rapid = detect_rapid_pass_through(graph, max_delay_seconds=3600)
```

Expansion fetches transactions touching each successive frontier; it does not
necessarily include edges between two outermost nodes. It returns requested
limits, completed hops, graph size and explicit truncation reason. All analysis
results carry this scope; a negative fan result is `null` if expansion was
truncated. Results are about this local graph and time window, not a whole
network. The limit is on fetched output, not a guarantee of database scan time.
If ingestion runs concurrently, use a repeatable-read database transaction for
a stable multi-query snapshot.

Direction is followed at each expansion hop: `out` walks outgoing transactions,
`in` walks incoming transactions, and `all` uses both. Neighbor counts exclude
self-transfers; transaction counts include them. Use `all` for features,
reciprocal analysis and rapid pass-through, which need both directions. Fan-in
and fan-out return distinct external counterparties and supporting transactions.

Paths are directed simple paths starting at the target; cycles are simple
directed cycles containing it. Cycles are rotated to a feasible chronological
start, which may be another account; structural cycles that fail chronological
or gap checks remain visible with `temporal_status` explaining the outcome.
Equal timestamps are retained but flagged as order-ambiguous. Adjacent transactions expose currency comparability and
amount comparison; cross-currency amounts are not compared. Reciprocal and
rapid pass-through results are transaction pairs, not allocation of money.
These indicators do not prove that the same funds moved or establish crime.
`max_results` and `max_states` bound path/pair search, and `search_truncated`
signals an incomplete search independently of graph expansion truncation.

Optional [account/time index definitions](indexes.sql) are provided for
measured workloads. They are not installed automatically: index creation can
affect write throughput and storage. Compare `EXPLAIN (ANALYZE, BUFFERS)` on
representative account degrees and time windows before adopting them.

For integration testing, set `AML_GRAPH_TEST_DATABASE_URL` to a disposable
PostgreSQL database named `aml_graph_test` and run
`pytest tests/test_graph_investigation.py`. The fixture writes synthetic graph
records and, when available, 1,000 real LI-Small CSV rows into that database;
never point this variable at a database containing user data. Without it, the
pure graph tests still run and database tests are skipped.
