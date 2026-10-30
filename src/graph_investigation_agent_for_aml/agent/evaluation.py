"""Compare an investigation with a fixed, bounded analytical baseline."""
import time

from .graph import build_graph

# The baseline answers the same two hypotheses using the same tool functions.
FIXED_ACTIONS = ("expand_transaction_graph", "detect_fan_in", "detect_rapid_pass_through")


def run_baseline(connection, case, *, max_tools, timeout_seconds):
    def fixed_decision(_prompt, context):
        available = context["available_actions"]
        action = next((name for name in FIXED_ACTIONS if name in available), "finish")
        return {"action": action, "purpose": "Run the fixed comparison procedure."}

    started = time.monotonic()
    graph = build_graph(connection, fixed_decision, max_tools=max_tools, timeout_seconds=timeout_seconds)
    result = graph.invoke({"case": case, "events": [], "evidence": [], "graph_data": None},
                          {"recursion_limit": 2 * max_tools + 5})
    return result["report"], time.monotonic() - started


def compare_reports(agent_report, baseline_report, *, agent_seconds, baseline_seconds):
    """Compare observed evidence; ground-truth accuracy requires external labels."""
    def metrics(report, duration):
        hypotheses = {item["hypothesis"]: item["status"] for item in report["hypotheses"]}
        ids = {finding["tool"]: sorted(finding["verified_transaction_ids"]) for finding in report["findings"]}
        return {"hypotheses": hypotheses, "transaction_ids_by_tool": ids,
                "tool_calls": len(report["events"]), "elapsed_seconds": round(duration, 3),
                "validation_errors": len(report["validation_errors"]),
                "stop_reason": report["stop_reason"]}

    return {"agent": metrics(agent_report, agent_seconds), "baseline": metrics(baseline_report, baseline_seconds),
            "limitations": ["This compares reported results, not accuracy against independently labeled cases.",
                            "Model cost is not inferred from call count; use provider usage and pricing."]}


def score_case(report, expected):
    """Score independently supplied indicator labels; unknown results stay unknown.

    Labels describe the synthetic indicator, not confirmed criminal activity.
    """
    hypotheses = {item["hypothesis"]: item["status"] for item in report["hypotheses"]}
    findings = {item["tool"]: item for item in report["findings"]}
    output = {}
    for tool, label in (("detect_fan_in", "Concentrated incoming transfers"),
                        ("detect_rapid_pass_through", "Rapid onward transfers")):
        actual = hypotheses.get(label, "not_examined")
        target = expected[tool]
        classified = actual in ("indicator_observed", "indicator_not_observed")
        ids = set(findings.get(tool, {}).get("verified_transaction_ids", [])) if actual == "indicator_observed" else set()
        reference = set(target["transaction_ids"])
        output[tool] = {"expected": target["observed"], "status": actual,
                        "correct": (actual == "indicator_observed") == target["observed"] if classified else None,
                        "reference_precision": len(ids & reference) / len(ids) if ids else None,
                        "reference_recall": len(ids & reference) / len(reference) if reference and classified else None,
                        "unexpected_transaction_ids": sorted(ids - reference),
                        "missing_transaction_ids": sorted(reference - ids)}
    return output
