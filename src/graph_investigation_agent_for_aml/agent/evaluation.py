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
