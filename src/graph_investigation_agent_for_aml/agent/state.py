"""The shared state passed between LangGraph nodes."""
from typing import TypedDict


class InvestigationState(TypedDict, total=False):
    case: dict
    evidence: list[dict]
    hypotheses: list[dict]
    events: list[dict]
    graph_data: dict | None
    next_action: str
    purpose: str
    stop_reason: str
    report: dict
