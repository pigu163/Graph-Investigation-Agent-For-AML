"""Build a report from tool outputs and check embedded transaction evidence."""

from datetime import datetime
from decimal import Decimal

REQUIRED_TRANSACTION_FIELDS = {"transaction_id", "event_time", "source_bank", "source_account",
                               "destination_bank", "destination_account", "amount_paid",
                               "payment_currency", "amount_received", "receiving_currency"}

PATTERNS = ("detect_fan_in", "detect_fan_out", "detect_rapid_pass_through",
            "detect_reciprocal_flows", "detect_cycles", "trace_fund_flow")


def _transactions(value):
    """Walk nested pattern results, finding transaction-shaped objects."""
    if isinstance(value, dict):
        if "transaction_id" in value:
            yield value
        for child in value.values():
            yield from _transactions(child)
    elif isinstance(value, list):
        for child in value:
            yield from _transactions(child)


def build_report(state):
    evidence = state.get("evidence", [])
    indexed = {item["tool"]: (index, item) for index, item in enumerate(evidence)}
    graph = indexed.get("expand_transaction_graph")
    source_rows = {row["transaction_id"]: row for row in graph[1]["result"]["transactions"]} if graph else {}
    findings, validation_errors = [], []
    for name in PATTERNS:
        if name not in indexed:
            continue
        index, item = indexed[name]
        # Every embedded transaction must match the exact database-backed graph
        # row. This verifies the evidence references, not the algorithm itself.
        checked_ids = []
        for row in _transactions(item["result"]):
            txid = row["transaction_id"]
            original = source_rows.get(txid)
            if (original is None or not REQUIRED_TRANSACTION_FIELDS <= row.keys()
                    or any(original.get(key) != value for key, value in row.items())):
                validation_errors.append({"tool": name, "transaction_id": txid})
            else:
                checked_ids.append(txid)
        # Recalculate pair comparisons from source rows. This checks consistency
        # of reported times and amounts, not whether an AML pattern proves crime.
        if name in ("detect_rapid_pass_through", "detect_reciprocal_flows"):
            for pair in item["result"]:
                incoming, outgoing = pair["incoming"], pair["outgoing"]
                if any(error["tool"] == name for error in validation_errors):
                    break
                comparable = incoming["receiving_currency"] == outgoing["payment_currency"]
                expected = {"currency_comparable": comparable,
                            "incoming_received": incoming["amount_received"], "outgoing_paid": outgoing["amount_paid"],
                            "incoming_currency": incoming["receiving_currency"], "outgoing_currency": outgoing["payment_currency"],
                            "outgoing_not_greater_than_incoming": (Decimal(outgoing["amount_paid"]) <= Decimal(incoming["amount_received"]) if comparable else None)}
                seconds = (datetime.fromisoformat(outgoing["event_time"]) - datetime.fromisoformat(incoming["event_time"])).total_seconds()
                if pair["amount_comparison"] != expected or pair["time_difference_seconds"] != seconds or pair["equal_timestamp_order_unknown"] != (seconds == 0):
                    validation_errors.append({"tool": name, "transaction_id": incoming["transaction_id"]})
        if any(error["tool"] == name for error in validation_errors):
            continue
        findings.append({"tool": name, "evidence_index": index, "result": item["result"],
                         "verified_transaction_ids": sorted(set(checked_ids)),
                         "scope": item.get("scope"), "search_truncated": item.get("search_truncated")})

    # Summaries are deterministic and point back to the verified tool result.
    hypotheses = []
    for tool, label in (("detect_fan_in", "Concentrated incoming transfers"),
                        ("detect_rapid_pass_through", "Rapid onward transfers")):
        item = next((finding for finding in findings if finding["tool"] == tool), None)
        if item is None:
            hypotheses.append({"hypothesis": label, "status": "not_examined", "evidence_index": None})
            continue
        truncated = bool((item.get("scope") or {}).get("truncated") or item.get("search_truncated"))
        observed = item["result"].get("detected") if tool == "detect_fan_in" else bool(item["result"])
        status = "indicator_observed" if observed else ("inconclusive" if truncated or observed is None else "indicator_not_observed")
        hypotheses.append({"hypothesis": label, "status": status, "evidence_index": item["evidence_index"]})

    # Never turn a missing, truncated, or failed analysis into a negative finding.
    tested = {item["tool"] for item in findings}
    unexamined = [name for name in ("detect_fan_in", "detect_rapid_pass_through") if name not in tested]
    narrative = [f"{item['hypothesis']}: {item['status'].replace('_', ' ')}."
                 for item in hypotheses]
    narrative.append("These indicators require review and do not establish criminal activity.")
    return {"case": state["case"], "summary": " ".join(narrative), "hypotheses": hypotheses, "findings": findings, "unexamined_tools": unexamined,
            "validation_errors": validation_errors,
            "next_steps": ["Review referenced transactions and investigate unexamined hypotheses.",
                           "Seek customer and business context before drawing conclusions."],
            "stop_reason": state["stop_reason"], "events": state.get("events", []), "evidence": evidence,
            "limitations": ["Evidence checks compare transaction fields to the expanded graph, not to an independent data source.",
                            "Patterns are indicators, not proof of crime or transfer of the same funds.",
                            "Evidence is limited to stored transactions, the selected window and tool bounds.",
                            "Legitimate explanations require customer and business context."]}


def render_report(report):
    """Render the already validated report without asking a model to create facts."""
    case = report["case"]
    lines = ["# Investigation report", "",
             f"Target: {case.get('dataset', 'unknown')} / {case.get('bank', 'unknown')} / {case.get('account', 'unknown')}",
             f"Investigation window: [{case.get('start', 'unknown')}, {case.get('end', 'unknown')})",
             f"Database availability cutoff: {case.get('as_of') or 'not set'}",
             f"Stop reason: {report['stop_reason']}", "", "## Hypotheses"]
    for hypothesis in report["hypotheses"]:
        index = hypothesis["evidence_index"]
        reference = f" (evidence[{index}])" if index is not None else ""
        lines.append(f"- {hypothesis['hypothesis']}: {hypothesis['status'].replace('_', ' ')}{reference}.")
    lines.extend(["", "## Investigation steps"])
    for event in report["events"]:
        lines.append(f"- {event['time']}: {event['action']} — {event['status']}.")
    if not report["events"]:
        lines.append("- No tools were executed.")
    lines.extend(["", "## Account activity"])
    profile = next(((i, item) for i, item in enumerate(report["evidence"]) if item["tool"] == "get_account_profile"), None)
    if profile:
        index, item = profile
        lines.append(f"- Source: evidence[{index}]; counts cover the full selected window.")
        for row in item["result"]["directions"]:
            lines.append(f"- {row['direction']}: {row['transaction_count']} movements; {row['distinct_counterparty_count']} distinct external counterparties.")
        for row in item["result"]["currencies"]:
            lines.append(f"- {row['direction']} amount: {row['amount_sum']} {row['currency']}.")
    else:
        lines.append("- Account profile unavailable.")
    lines.extend(["", "## Findings"])
    for finding in report["findings"]:
        lines.append(f"- {finding['tool']} (evidence[{finding['evidence_index']}]): "
                     f"{len(finding['verified_transaction_ids'])} verified transaction references.")
        if finding["tool"] in ("detect_cycles", "trace_fund_flow"):
            for path in finding["result"][:5]:
                ids = " → ".join(row["transaction_id"] for row in path["transactions"])
                lines.append(f"  - {path['temporal_status']}: {ids}.")
            if len(finding["result"]) > 5:
                lines.append("  - Additional paths are available in JSON output.")
    if not report["findings"]:
        lines.append("- No verified pattern findings are available.")
    lines.extend(["", "## Verified transaction evidence"])
    graph = next((item for item in report["evidence"] if item["tool"] == "expand_transaction_graph"), None)
    rows = {row["transaction_id"]: row for row in graph["result"]["transactions"]} if graph else {}
    ids = sorted({txid for finding in report["findings"] for txid in finding["verified_transaction_ids"]})
    # ponytail: keep reports readable by showing at most 20 details; JSON retains all evidence.
    for txid in ids[:20]:
        row = rows[txid]
        lines.append(f"- {txid}: {row.get('event_time', 'unknown')}; "
                     f"{row.get('source_bank', '?')}/{row.get('source_account', '?')} → "
                     f"{row.get('destination_bank', '?')}/{row.get('destination_account', '?')}; "
                     f"paid {row.get('amount_paid', '?')} {row.get('payment_currency', '?')}; "
                     f"received {row.get('amount_received', '?')} {row.get('receiving_currency', '?')}.")
    if not ids:
        lines.append("- No verified transaction references are available.")
    if len(ids) > 20:
        lines.append(f"- Showing 20 of {len(ids)} referenced transactions; use JSON output for the full structured evidence.")
    if comparison := report.get("comparison"):
        lines.extend(["", "## Fixed baseline comparison"])
        for label in ("agent", "baseline"):
            result = comparison[label]
            lines.append(f"- {label}: {result['tool_calls']} tool calls, {result['elapsed_seconds']} seconds, "
                         f"{result['validation_errors']} validation errors; stop: {result['stop_reason']}.")
            for hypothesis, status in result["hypotheses"].items():
                lines.append(f"  - {hypothesis}: {status.replace('_', ' ')}.")
        lines.extend(f"- {item}" for item in comparison["limitations"])
    if model := report.get("model"):
        lines.extend(["", "## Model usage",
                      (f"- Provider: {model['provider']}; model: {model.get('model_id', 'unknown')}; requests: {model['requests']}; "
                      f"reported input tokens: {model['usage']['prompt_tokens']}; "
                      f"reported output tokens: {model['usage']['completion_tokens']}." ),
                      f"- {model['usage_limitation']}"])
    lines.extend(["", "## Limitations and follow-up"])
    lines.extend(f"- {item}" for item in report["limitations"] + report["next_steps"])
    lines.append("- Possible legitimate explanations include routine business collections or treasury transfers; customer context is needed to assess them.")
    for finding in report["findings"]:
        if (finding.get("scope") or {}).get("truncated") or finding.get("search_truncated"):
            lines.append(f"- {finding['tool']} was truncated; absence of further results is inconclusive.")
    if report["validation_errors"]:
        lines.append("- Some findings failed transaction evidence validation and were excluded.")
    return "\n".join(lines) + "\n"
