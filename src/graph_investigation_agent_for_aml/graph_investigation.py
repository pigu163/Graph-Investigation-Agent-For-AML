"""Bounded ego-graph and temporal investigations over stored transactions.

The graph is expanded in database batches, serialized with evidence, and can be
passed to the pure analysis functions below without a database connection.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise

import networkx as nx
from psycopg.rows import dict_row

from graph_investigation_agent_for_aml.investigation import (
    TRANSACTION_COLUMNS,
    _params,
    _response,
)

MAX_HOPS = 5
MAX_NODES = 2000
MAX_EDGES = 10000
MAX_RESULTS = 1000
MAX_STATES = 100000


def _bounded_int(name, value, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"{name} must be between {lower} and {upper}")


def _node(bank, account):
    return {"bank_id": bank, "account_id": account}


def _graph_result(tool, graph_data, result, *, search_truncated=False, **query):
    scope = graph_data["scope"]
    return _response(tool, {**graph_data["query"], **query}, result,
                     scope=scope, search_truncated=search_truncated,
                     limitations=[
                         "Results are limited to the returned local graph and investigation window.",
                         "Graph connections and temporal proximity do not prove the same funds moved.",
                         "Equal source timestamps do not establish transaction order.",
                     ])


def expand_transaction_graph(connection, dataset, bank, account, start, end, *,
                             hops=2, direction="all", max_nodes=500, max_edges=2000):
    """Expand frontier edges up to hops; preserve parallel transactions and bounds.

    Outer-hop nodes are included, but edges between two outer-hop nodes are not
    fetched unless they also touch an earlier frontier. Use a repeatable-read
    transaction if a stable snapshot is needed while ingestion is running.
    """
    params = _params(dataset, bank, account, start, end, direction)
    _bounded_int("hops", hops, 1, MAX_HOPS)
    _bounded_int("max_nodes", max_nodes, 1, MAX_NODES)
    _bounded_int("max_edges", max_edges, 1, MAX_EDGES)
    graph = nx.MultiDiGraph()
    target = (bank, account)
    graph.add_node(target)
    frontier = {target}
    seen = set()
    reason = None
    completed_hops = 0

    # Two directional branches make account/time indexes available to the planner.
    statement = f"""
        WITH frontier AS (
            SELECT * FROM unnest(%(banks)s::text[], %(accounts)s::text[]) AS f(bank, account)
        ), candidates AS (
            SELECT {TRANSACTION_COLUMNS} FROM transactions t
            JOIN frontier f ON t.source_bank = f.bank AND t.source_account = f.account
            WHERE t.source_dataset = %(dataset)s AND t.event_time >= %(start)s AND t.event_time < %(end)s
              AND %(direction)s IN ('all', 'out') AND NOT (t.transaction_id = ANY(%(seen)s::text[]))
            UNION
            SELECT {TRANSACTION_COLUMNS} FROM transactions t
            JOIN frontier f ON t.destination_bank = f.bank AND t.destination_account = f.account
            WHERE t.source_dataset = %(dataset)s AND t.event_time >= %(start)s AND t.event_time < %(end)s
              AND %(direction)s IN ('all', 'in') AND NOT (t.transaction_id = ANY(%(seen)s::text[]))
        )
        SELECT * FROM candidates ORDER BY event_time, transaction_id LIMIT %(limit)s
    """
    for depth in range(hops):
        if not frontier:
            break
        ordered = sorted(frontier)
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(statement, {**params, "banks": [n[0] for n in ordered],
                                       "accounts": [n[1] for n in ordered],
                                       "seen": sorted(seen),
                                       "limit": max_edges - graph.number_of_edges() + 1})
            rows = cursor.fetchall()
        next_frontier = set()
        for row in rows:
            source = (row["source_bank"], row["source_account"])
            destination = (row["destination_bank"], row["destination_account"])
            if graph.number_of_edges() >= max_edges:
                reason = "max_edges"
                break
            new_nodes = {source, destination} - set(graph.nodes)
            if graph.number_of_nodes() + len(new_nodes) > max_nodes:
                reason = "max_nodes"
                break
            seen.add(row["transaction_id"])
            graph.add_edge(source, destination, key=row["transaction_id"], **row)
            next_frontier.update(new_nodes)
        if reason:
            break
        completed_hops = depth + 1
        frontier = next_frontier

    nodes = [_node(*n) for n in sorted(graph.nodes)]
    edges = [dict(data) for _, _, _, data in sorted(
        graph.edges(keys=True, data=True), key=lambda item: (item[3]["event_time"], item[2]))]
    query = {**params, "hops": hops, "max_nodes": max_nodes, "max_edges": max_edges}
    return _response("expand_transaction_graph", query, {"nodes": nodes, "transactions": edges},
                     scope={"dataset": dataset, "target": _node(*target),
                            "completed_hops": completed_hops, "truncated": reason is not None,
                            "truncation_reason": reason, "node_count": len(nodes),
                            "transaction_count": len(edges), "expansion": "frontier_edges"},
                     limitations=["Outer-hop to outer-hop edges are not necessarily included.",
                                  "Use repeatable-read when concurrent ingestion requires a stable snapshot.",
                                  "Event timestamps retain the source's timezone semantics."])


def _to_graph(graph_data):
    """Recreate the directed multigraph from the bounded serialized result."""
    graph = nx.MultiDiGraph()
    for node in graph_data["result"]["nodes"]:
        graph.add_node((node["bank_id"], node["account_id"]))
    for row in graph_data["result"]["transactions"]:
        source = (row["source_bank"], row["source_account"])
        destination = (row["destination_bank"], row["destination_account"])
        graph.add_edge(source, destination, key=row["transaction_id"], **row)
    target = graph_data["scope"]["target"]
    return graph, (target["bank_id"], target["account_id"])


def compute_graph_features(graph_data):
    """Return target degrees, transaction counts and per-currency totals."""
    _require_both_directions(graph_data)
    graph, target = _to_graph(graph_data)
    currencies = {}
    for direction, edges in (("in", graph.in_edges(target, keys=True, data=True)),
                             ("out", graph.out_edges(target, keys=True, data=True))):
        for _, _, _, row in edges:
            currency = row["receiving_currency"] if direction == "in" else row["payment_currency"]
            amount = row["amount_received"] if direction == "in" else row["amount_paid"]
            key = (direction, currency)
            currencies[key] = currencies.get(key, Decimal(0)) + Decimal(amount)
    result = {
        "in_transaction_count": graph.in_degree(target),
        "out_transaction_count": graph.out_degree(target),
        "in_neighbor_count": len(set(graph.predecessors(target)) - {target}),
        "out_neighbor_count": len(set(graph.successors(target)) - {target}),
        "mutual_neighbor_count": len((set(graph.predecessors(target)) &
                                      set(graph.successors(target))) - {target}),
        "in_transaction_ids": sorted(key for _, _, key in graph.in_edges(target, keys=True)),
        "out_transaction_ids": sorted(key for _, _, key in graph.out_edges(target, keys=True)),
        "amounts": [{"direction": direction, "currency": currency, "amount": amount}
                    for (direction, currency), amount in sorted(currencies.items())],
    }
    return _graph_result("compute_graph_features", graph_data, result)


def _fan(graph_data, direction, min_counterparties):
    _bounded_int("min_counterparties", min_counterparties, 1, MAX_NODES)
    if graph_data["query"]["direction"] not in ("all", direction):
        raise ValueError(f"graph must include {direction} direction")
    graph, target = _to_graph(graph_data)
    neighbors = set(graph.predecessors(target) if direction == "in" else graph.successors(target)) - {target}
    edges = graph.in_edges(target, keys=True, data=True) if direction == "in" else graph.out_edges(target, keys=True, data=True)
    transactions = [dict(row) for source, destination, _, row in edges if source != destination]
    transactions.sort(key=lambda row: (row["event_time"], row["transaction_id"]))
    tool = "detect_fan_in" if direction == "in" else "detect_fan_out"
    detected = len(neighbors) >= min_counterparties
    return _graph_result(tool, graph_data,
                         {"detected": detected if detected or not graph_data["scope"]["truncated"] else None,
                          "counterparty_count": len(neighbors),
                          "counterparties": [_node(*n) for n in sorted(neighbors)],
                          "transactions": transactions}, min_counterparties=min_counterparties)


def detect_fan_in(graph_data, *, min_counterparties=2):
    """Identify target's distinct external senders in the graph window."""
    return _fan(graph_data, "in", min_counterparties)


def detect_fan_out(graph_data, *, min_counterparties=2):
    """Identify target's distinct external receivers in the graph window."""
    return _fan(graph_data, "out", min_counterparties)


def _time(row):
    return datetime.fromisoformat(row["event_time"])


def _compatibility(incoming, outgoing):
    compatible = incoming["receiving_currency"] == outgoing["payment_currency"]
    return {"currency_comparable": compatible,
            "incoming_received": incoming["amount_received"],
            "outgoing_paid": outgoing["amount_paid"],
            "incoming_currency": incoming["receiving_currency"],
            "outgoing_currency": outgoing["payment_currency"],
            "outgoing_not_greater_than_incoming": (
                Decimal(outgoing["amount_paid"]) <= Decimal(incoming["amount_received"])
                if compatible else None)}


def _require_both_directions(graph_data):
    if graph_data["query"]["direction"] != "all":
        raise ValueError("graph must include both incoming and outgoing directions")


def _pair_results(graph_data, kind, max_delay_seconds, max_results, max_states):
    _bounded_int("max_delay_seconds", max_delay_seconds, 0, 31 * 24 * 3600)
    _bounded_int("max_results", max_results, 1, MAX_RESULTS)
    _bounded_int("max_states", max_states, 1, MAX_STATES)
    _require_both_directions(graph_data)
    graph, target = _to_graph(graph_data)
    incoming = [dict(row) for source, _, _, row in graph.in_edges(target, keys=True, data=True)
                if source != target]
    outgoing = [dict(row) for _, destination, _, row in graph.out_edges(target, keys=True, data=True)
                if destination != target]
    incoming.sort(key=lambda row: (_time(row), row["transaction_id"]))
    outgoing.sort(key=lambda row: (_time(row), row["transaction_id"]))
    results = []
    states = 0
    truncated = False
    for first in incoming:
        for second in outgoing:
            states += 1
            if states > max_states:
                truncated = True
                break
            seconds = (_time(second) - _time(first)).total_seconds()
            if kind == "detect_reciprocal_flows":
                if (first["source_bank"], first["source_account"]) != (second["destination_bank"], second["destination_account"]):
                    continue
                if abs(seconds) > max_delay_seconds:
                    continue
            elif seconds < 0 or seconds > max_delay_seconds:
                continue
            if len(results) == max_results:
                truncated = True
                break
            results.append({"incoming": first, "outgoing": second,
                            "time_difference_seconds": seconds,
                            "equal_timestamp_order_unknown": seconds == 0,
                            "amount_comparison": _compatibility(first, second)})
        if truncated:
            break
    return _graph_result(kind, graph_data, results, search_truncated=truncated,
                         max_delay_seconds=max_delay_seconds, max_results=max_results,
                         max_states=max_states, examined_pairs=min(states, max_states))


def detect_reciprocal_flows(graph_data, *, max_delay_seconds=86400,
                            max_results=100, max_states=10000):
    """Find opposite-direction transaction pairs with the same counterparty."""
    return _pair_results(graph_data, "detect_reciprocal_flows", max_delay_seconds,
                         max_results, max_states)


def detect_rapid_pass_through(graph_data, *, max_delay_seconds=3600,
                              max_results=100, max_states=10000):
    """Find incoming/outgoing pairs; matching is an indicator, not attribution."""
    return _pair_results(graph_data, "detect_rapid_pass_through", max_delay_seconds,
                         max_results, max_states)


def _walk(graph_data, *, cycle, max_hops, max_gap_seconds, max_results, max_states):
    _bounded_int("max_hops", max_hops, 2, MAX_HOPS)
    _bounded_int("max_gap_seconds", max_gap_seconds, 0, 31 * 24 * 3600)
    _bounded_int("max_results", max_results, 1, MAX_RESULTS)
    _bounded_int("max_states", max_states, 1, MAX_STATES)
    if graph_data["query"]["direction"] == "in":
        raise ValueError("graph must include outgoing direction")
    graph, target = _to_graph(graph_data)
    results = []
    states = 0
    truncated = False

    def visit(current, visited, edges):
        nonlocal states, truncated
        candidates = sorted(graph.out_edges(current, keys=True, data=True),
                            key=lambda item: (_time(item[3]), item[2]))
        for _, next_node, _, row in candidates:
            states += 1
            if states > max_states:
                truncated = True
                return
            if next_node == current or (next_node in visited and not (cycle and next_node == target)):
                continue
            if edges and not cycle:
                gap = _time(row) - _time(edges[-1])
                if gap < timedelta(0) or gap > timedelta(seconds=max_gap_seconds):
                    continue
            path = [*edges, dict(row)]
            closes_cycle = cycle and next_node == target and len(path) >= 2
            temporal_status = None
            if closes_cycle:
                # A chronological cycle can start at any account, not necessarily the ego.
                rotations = [path[i:] + path[:i] for i in range(len(path))]
                chronological = [candidate for candidate in rotations if all(
                    _time(a) <= _time(b) for a, b in pairwise(candidate))]
                feasible = next((candidate for candidate in chronological if all(
                    timedelta(0) <= _time(b) - _time(a) <= timedelta(seconds=max_gap_seconds)
                    for a, b in pairwise(candidate))), None)
                if feasible:
                    path = feasible
                    temporal_status = "chronological"
                elif chronological:
                    path = chronological[0]
                    temporal_status = "gap_exceeded"
                else:
                    temporal_status = "not_chronological"
            report = closes_cycle if cycle else len(path) >= 2
            if report:
                if len(results) == max_results:
                    truncated = True
                    return
                results.append({"transactions": path,
                                "temporal_status": temporal_status if cycle else "chronological",
                                "equal_timestamp_order_unknown": len({_time(item) for item in path}) < len(path),
                                "amount_comparisons": [_compatibility(a, b)
                                                       for a, b in pairwise(path)]})
            if len(path) < max_hops and next_node not in visited and not closes_cycle:
                visit(next_node, visited | {next_node}, path)
                if truncated:
                    return

    visit(target, {target}, [])
    tool = "detect_cycles" if cycle else "trace_fund_flow"
    return _graph_result(tool, graph_data, results, search_truncated=truncated,
                         max_hops=max_hops, max_gap_seconds=max_gap_seconds,
                         max_results=max_results, max_states=max_states,
                         examined_edges=min(states, max_states))


def detect_cycles(graph_data, *, max_hops=3, max_gap_seconds=86400,
                  max_results=100, max_states=10000):
    """Find directed cycles containing the target and assess their timing."""
    return _walk(graph_data, cycle=True, max_hops=max_hops, max_gap_seconds=max_gap_seconds,
                 max_results=max_results, max_states=max_states)


def trace_fund_flow(graph_data, *, max_hops=3, max_gap_seconds=86400,
                    max_results=100, max_states=10000):
    """Find chronological simple outgoing paths of at least two transactions."""
    return _walk(graph_data, cycle=False, max_hops=max_hops, max_gap_seconds=max_gap_seconds,
                 max_results=max_results, max_states=max_states)
