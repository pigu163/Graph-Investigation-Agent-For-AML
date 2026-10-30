"""Scope the existing analytical functions to the current case."""
from datetime import datetime

from graph_investigation_agent_for_aml import graph_investigation as graph_tools
from graph_investigation_agent_for_aml import investigation as queries

ACTIONS = ("get_account_profile", "get_transactions", "get_counterparties",
           "expand_transaction_graph", "detect_fan_in", "detect_fan_out",
           "detect_reciprocal_flows", "detect_rapid_pass_through", "detect_cycles",
           "trace_fund_flow", "compute_graph_features")
GRAPH_ACTIONS = set(ACTIONS[4:])


def execute(connection, case, action, graph_data):
    """Execute only predefined functions with fixed bounds and a fixed target."""
    if action not in ACTIONS:
        raise ValueError("Unknown investigation tool")
    args = (connection, case["dataset"], case["bank"], case["account"],
            datetime.fromisoformat(case["start"]), datetime.fromisoformat(case["end"]))
    as_of = datetime.fromisoformat(case["as_of"]) if case.get("as_of") else None
    if action in GRAPH_ACTIONS:
        if graph_data is None:
            raise ValueError("Expand the transaction graph first")
        return getattr(graph_tools, action)(graph_data)
    if action == "expand_transaction_graph":
        return graph_tools.expand_transaction_graph(*args, hops=2, max_nodes=200, max_edges=500, as_of=as_of)
    if action == "get_transactions":
        return queries.get_transactions(*args, limit=100, as_of=as_of)
    if action == "get_counterparties":
        return queries.get_counterparties(*args, limit=100, as_of=as_of)
    return queries.get_account_profile(*args, as_of=as_of)
