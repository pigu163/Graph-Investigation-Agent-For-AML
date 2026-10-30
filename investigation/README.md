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
index; measure query performance and add indexes if the dataset size warrants it.
