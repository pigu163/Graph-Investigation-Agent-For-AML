"""An explicit LangGraph loop: decide, execute a bounded tool, then decide again."""
import time
from collections.abc import Callable
from datetime import UTC, datetime

from langgraph.graph import END, START, StateGraph

from .models import ModelDecision
from .report import build_report
from .state import InvestigationState
from .tools import ACTIONS, GRAPH_ACTIONS, execute


class ModelBudgetExceeded(Exception):
    """The model request allowance for this run has been used."""


SYSTEM_PROMPT = """You are a financial crime investigator. Choose the next analytical action based only on the case and tool results supplied. Return a JSON object with exactly two strings: action and purpose. action must be one of the available actions or finish. Start with the account profile and transactions. Expand the graph before using graph analysis. Investigate the user's question adaptively, including possible benign explanations. Avoid repeating actions. If evidence is adequate or unavailable, choose finish. A pattern is not proof of crime. Never invent facts or tool outputs."""


def build_graph(connection, decide: Callable, *, max_tools=8, timeout_seconds=120):
    """Build one graph for one case. `decide` is the model adapter, not a LangChain agent."""
    if not 1 <= max_tools <= 20 or timeout_seconds <= 0:
        raise ValueError("Invalid investigation budget")
    deadline = time.monotonic() + timeout_seconds

    def plan(state: InvestigationState):
        # State carries real tool outputs between nodes. The program, not the
        # model, enforces the allowed actions and the tool-call/time budget.
        if len(state.get("events", [])) >= max_tools:
            return {"next_action": "finish", "stop_reason": "tool_budget_exhausted"}
        if time.monotonic() >= deadline:
            return {"next_action": "finish", "stop_reason": "time_budget_exhausted"}
        completed = {e["action"] for e in state.get("events", [])}
        available = [a for a in ACTIONS if a not in completed and
                     (a not in GRAPH_ACTIONS or state.get("graph_data") is not None)]
        # Always establish the account baseline before asking the model to route.
        for baseline in ("get_account_profile", "get_transactions"):
            if baseline not in completed:
                return {"next_action": baseline, "purpose": "Establish the account baseline."}
        try:
            decision = decide(SYSTEM_PROMPT, {"case": state["case"], "evidence": state.get("evidence", []),
                                              "events": state.get("events", []), "hypotheses": state.get("hypotheses", []),
                                              "available_actions": available + ["finish"],
                                              "remaining_seconds": max(0.001, deadline - time.monotonic())})
            decision = ModelDecision.model_validate(decision)
            action, purpose = decision.action, decision.purpose
            if action not in available + ["finish"]:
                raise ValueError("Model returned an invalid action or purpose")
        except ModelBudgetExceeded:
            return {"next_action": "finish", "stop_reason": "model_budget_exhausted"}
        except Exception as exc:  # noqa: BLE001 - preserve partial evidence on tool/model failure
            if time.monotonic() >= deadline:
                return {"next_action": "finish", "stop_reason": "time_budget_exhausted"}
            status = getattr(exc, "code", None)
            detail = f"HTTP_{status}" if isinstance(status, int) else type(exc).__name__
            return {"next_action": "finish", "stop_reason": f"model_error: {detail}"}
        if time.monotonic() >= deadline:
            return {"next_action": "finish", "stop_reason": "time_budget_exhausted"}
        if action == "finish":
            return {"next_action": "finish", "stop_reason": "agent_finished"}
        return {"next_action": action, "purpose": purpose}

    def run_tool(state: InvestigationState):
        action = state["next_action"]
        event = {"action": action, "purpose": state["purpose"], "time": datetime.now(UTC).isoformat()}
        evidence = list(state.get("evidence", []))
        try:
            # Bound database execution as well as the graph expansion itself.
            with connection.transaction():
                remaining_ms = max(1, min(15000, int((deadline - time.monotonic()) * 1000)))
                connection.execute(f"SET LOCAL statement_timeout = '{remaining_ms}ms'")
                result = execute(connection, state["case"], action, state.get("graph_data"))
            event["status"] = "completed"
            evidence.append(result)
            graph_data = result if action == "expand_transaction_graph" else state.get("graph_data")
        except Exception as exc:  # noqa: BLE001 - preserve partial evidence on tool/model failure
            event.update(status="failed", error=type(exc).__name__)
            graph_data = state.get("graph_data")
        # Reassess the two current hypotheses from validated tool evidence after
        # every step. The next model call sees these statuses, including unknown.
        updated = {**state, "events": state.get("events", []) + [event], "evidence": evidence,
                   "graph_data": graph_data, "stop_reason": "in_progress"}
        return {"events": updated["events"], "evidence": evidence, "graph_data": graph_data,
                "hypotheses": build_report(updated)["hypotheses"]}

    def report(state: InvestigationState):
        # The report node validates citations without asking the model to invent facts.
        return {"report": build_report(state)}

    # Nodes are functions; edges control their order. The conditional edge is
    # the loop: after each tool, ask the model whether to continue or finish.
    builder = StateGraph(InvestigationState)
    builder.add_node("plan", plan)
    builder.add_node("tool", run_tool)
    builder.add_node("report", report)
    builder.add_edge(START, "plan")
    builder.add_conditional_edges("plan", lambda s: "report" if s["next_action"] == "finish" else "tool",
                                  {"tool": "tool", "report": "report"})
    builder.add_edge("tool", "plan")
    builder.add_edge("report", END)
    return builder.compile()
