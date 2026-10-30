# Agent evaluation: synthetic pilot

These cases use the first six rows of `tests/fixtures/graph-investigation.csv`, replayed through Kafka into an isolated PostgreSQL database. A Flink job was submitted; Kafka and Flink were stopped before investigation. The labels in `tests/fixtures/agent-cases.json` describe toy indicators, not criminal activity. All test containers and temporary data were removed afterwards.

## Fixed baseline checks

The fixed workflow was checked against four manually labeled accounts (A–D). On this fixture, both indicator statuses agreed with their labels, and the expected transaction references matched for positive indicators. This is a small deterministic test of the tools and evaluation logic, not a population-level performance measurement.

## DeepSeek pilot

An earlier A-account run had a limit of 10 tool actions and used eight model requests. It executed both target analyses, reported both indicators and had no transaction validation errors, then stopped at the tool limit. The fixed baseline was not run in the same experiment, so this is not a controlled comparison.

A subsequent B/D pilot gave each account one model request and both workflows the same limit of eight tool actions. The Agent performed profile, transaction and counterparty queries, then exhausted its model budget without testing either target hypothesis. The fixed baseline performed the target analyses: B had no fan-in indicator and did have a rapid-pair indicator; D had neither. Both matched the fixture labels. An unexamined hypothesis is not a negative prediction. This pilot demonstrates the budget and missing-analysis reporting, not comparative model accuracy.

The two B/D requests exhausted the user's authorization of ten DeepSeek requests for this testing round (eight earlier plus two here). No further paid tests are authorized by that allowance.

## Interpretation

The automated field checks compare pattern references against the graph returned by the same database, not against an independent source. `stored_at` is the ingestion timestamp into this database, so historical CSV replay does not reconstruct the date information originally became available. More labeled cases, comparable model/tool budgets and appropriately recorded provider usage would be needed for a useful comparative study.

## Four-case DeepSeek comparison

After a separate authorization of up to 20 DeepSeek requests, A–D were each run once against the same half-fixture database with a seven-tool limit, five-model-request limit and 120-second nominal time budget. The baseline had the same tool and time limits and no model calls. Labels were excluded from model input. Each Agent run used all five requests and stopped at its seven-tool limit: 20 requests in total. Provider-reported usage totaled 62,402 input and 1,078 output tokens; monetary cost was not calculated.

| Account | Agent fan-in | Agent rapid pair | Baseline fan-in | Baseline rapid pair | Agent seconds | Baseline seconds |
| --- | --- | --- | --- | --- | ---: | ---: |
| A | observed | observed | observed | observed | 5.462 | 0.041 |
| B | not examined | observed | not observed | observed | 5.324 | 0.071 |
| C | not observed | not observed | not observed | not observed | 5.096 | 0.025 |
| D | not observed | not observed | not observed | not observed | 5.000 | 0.036 |

The Agent's seven examined indicator outcomes agreed with the manually specified fixture labels; B fan-in was not examined and is not counted as correct or incorrect. The baseline examined all eight and agreed with the same labels. On the three positive Agent results, the reported transaction-ID sets exactly matched the specified references. Both workflows reported zero transaction validation errors. All Agent runs hit the tool limit, so `tool_budget_exhausted` remains part of each result. The synthetic labels are tied to this tiny fixture and current indicator definitions; these counts are not estimates of real-world detection accuracy, crime identification, or model superiority. This was one run per account; timing reflects this local test and current provider responses, not a benchmark.

## Latest isolated rerun

After the implementation and evidence-state updates, the same four cases were
replayed into a fresh isolated PostgreSQL database from the first six of twelve
fixture rows. Kafka and Flink were stopped before the Agent run. The CLI used
DeepSeek `deepseek-flash`, at most five model requests and eight tool actions per
case; the four cases consumed exactly 20 model requests. Provider-reported usage
was 61,201 input tokens and 1,049 output tokens; monetary cost was not calculated.

All four reports stopped at the model-request budget after seven completed tool
actions, with zero evidence-validation errors. A and B's positive findings had
exactly the labeled transaction-ID sets; all eight labeled indicator states across
A–D matched the fixture labels. The fixed baseline completed normally for every
case. This is a reproducible integration regression, not evidence of production
accuracy or a claim that the indicators establish criminal conduct.
