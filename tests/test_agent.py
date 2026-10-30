"""Exercise real LangGraph routing without network or a database server."""
from contextlib import nullcontext

from graph_investigation_agent_for_aml.agent import graph as agent_graph


class Connection:
    def transaction(self):
        return nullcontext()

    def execute(self, statement):
        assert "statement_timeout" in statement


def test_adaptive_loop_preserves_evidence_and_enforces_budget(monkeypatch):
    calls = []

    def execute(connection, case, action, graph):
        calls.append(action)
        transaction = {"transaction_id": "t1", "event_time": "2022-09-01T09:00:00", "amount_paid": "1",
                       "payment_currency": "USD", "amount_received": "1", "receiving_currency": "USD",
                       "source_bank": "1", "source_account": "A", "destination_bank": "1", "destination_account": "B"}
        result = {"transactions": [transaction]} if action == "expand_transaction_graph" else transaction
        return {"tool": action, "result": result, "scope": {"truncated": False}}

    monkeypatch.setattr(agent_graph, "execute", execute)
    choices = iter([{"action": "expand_transaction_graph", "purpose": "Inspect neighbors"},
                    {"action": "detect_fan_in", "purpose": "Check concentration"}])
    graph = agent_graph.build_graph(Connection(), lambda *_: next(choices), max_tools=4)
    result = graph.invoke({"case": {"account": "A"}, "events": [], "evidence": [], "graph_data": None})
    assert calls == ["get_account_profile", "get_transactions", "expand_transaction_graph", "detect_fan_in"]
    assert result["report"]["stop_reason"] == "tool_budget_exhausted"
    assert result["report"]["findings"][0]["result"]["transaction_id"] == "t1"
    assert len(result["report"]["events"]) == 4


def test_model_and_tool_failures_are_visible(monkeypatch):
    def execute(*args):
        raise RuntimeError("private database detail")

    monkeypatch.setattr(agent_graph, "execute", execute)
    def decide(*args):
        raise ValueError("private model detail")
    graph = agent_graph.build_graph(Connection(), decide)
    result = graph.invoke({"case": {}, "events": [], "evidence": [], "graph_data": None})
    assert result["report"]["stop_reason"] == "model_error: ValueError"
    assert [event["status"] for event in result["report"]["events"]] == ["failed", "failed"]
    assert "private" not in str(result["report"])


def test_deepseek_request_cap(monkeypatch):
    import io

    import pytest

    from graph_investigation_agent_for_aml.agent.cli import model_decision
    from graph_investigation_agent_for_aml.agent.graph import ModelBudgetExceeded

    requests = []

    def open_request(request, timeout):
        requests.append(request.full_url)
        return io.BytesIO(b'{"choices":[{"message":{"content":"{\\"action\\":\\"finish\\",\\"purpose\\":\\"done\\"}"}}]}')

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    decide = model_decision("test-key")
    for _ in range(3):
        assert decide("Return JSON", {"evidence": []})["action"] == "finish"
    with pytest.raises(ModelBudgetExceeded):
        decide("Return JSON", {"evidence": []})
    assert len(requests) == decide.calls == 3
    assert all(url == "https://api.deepseek.com/chat/completions" for url in requests)


def test_report_rejects_modified_or_unknown_transaction_evidence():
    from graph_investigation_agent_for_aml.agent.report import build_report

    graph = {"tool": "expand_transaction_graph", "result": {"transactions": [
        {"transaction_id": "t1", "amount_paid": "5.00", "event_time": "2022-09-01T09:00:00"}]}}
    fan = {"tool": "detect_fan_in", "result": {"transactions": [
        {"transaction_id": "t1", "amount_paid": "7.00"}]}}
    report = build_report({"case": {}, "stop_reason": "agent_finished", "evidence": [graph, fan]})
    assert report["findings"] == []
    assert report["validation_errors"] == [{"tool": "detect_fan_in", "transaction_id": "t1"}]
    assert "detect_fan_in" in report["unexamined_tools"]


def test_cutoff_with_replayed_half_fixture():
    """Opt in only with an isolated database loaded from half the graph fixture."""
    import os
    from datetime import datetime, timedelta

    import psycopg
    import pytest

    from graph_investigation_agent_for_aml.agent.report import (
        build_report,
        render_report,
    )
    from graph_investigation_agent_for_aml.agent.tools import execute

    dsn = os.environ.get("AML_AGENT_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set AML_AGENT_TEST_DATABASE_URL to the isolated replay database")
    case = {"dataset": "graph-investigation", "bank": "01", "account": "A",
            "start": "2022-09-01T09:00:00", "end": "2022-09-01T10:00:00"}
    with psycopg.connect(dsn) as connection:
        times = connection.execute("SELECT stored_at FROM transactions WHERE source_dataset = %s ORDER BY stored_at", (case["dataset"],)).fetchall()
        assert len(times) == 6
        early, late = times[0][0] - timedelta(microseconds=1), times[-1][0] + timedelta(microseconds=1)
        middle = times[2][0]
        case["as_of"] = middle.isoformat()
        partial = execute(connection, case, "expand_transaction_graph", None)["result"]["transactions"]
        assert 0 < len(partial) < 6
        assert all(datetime.fromisoformat(row["stored_at"]) <= middle for row in partial)
        for cutoff, empty in ((early, True), (late, False)):
            case["as_of"] = cutoff.isoformat()
            results = [execute(connection, case, action, None) for action in ("get_account_profile", "get_transactions", "get_counterparties", "expand_transaction_graph")]
            assert bool(results[1]["result"]) is not empty
            assert bool(results[2]["result"]) is not empty
            assert bool(results[3]["result"]["transactions"]) is not empty
            assert bool(results[0]["result"]["directions"]) is not empty
            for row in results[3]["result"]["transactions"]:
                assert datetime.fromisoformat(row["stored_at"]) <= cutoff
            if not empty:
                fan = execute(connection, case, "detect_fan_in", results[3])
                rapid = execute(connection, case, "detect_rapid_pass_through", results[3])
                report = build_report({"case": case, "stop_reason": "agent_finished", "evidence": [*results, fan, rapid]})
                readable = render_report(report)
                assert "graph-investigation:1" in readable and "100.00 USD" in readable
                assert len(report["findings"]) == 2
                assert not report["validation_errors"]
                assert all(finding["verified_transaction_ids"] for finding in report["findings"])
                from graph_investigation_agent_for_aml.agent.evaluation import (
                    compare_reports,
                    run_baseline,
                )
                baseline, elapsed = run_baseline(connection, case, max_tools=6, timeout_seconds=30)
                comparison = compare_reports(report, baseline, agent_seconds=0, baseline_seconds=elapsed)
                assert comparison["baseline"]["tool_calls"] == 5
                assert comparison["baseline"]["validation_errors"] == 0
                assert all(value == "indicator_observed" for value in comparison["baseline"]["hypotheses"].values())
                assert comparison["baseline"]["transaction_ids_by_tool"]["detect_fan_in"] == ["graph-investigation:1", "graph-investigation:4", "graph-investigation:6"]


def test_report_distinguishes_missing_and_truncated_hypotheses():
    from graph_investigation_agent_for_aml.agent.report import build_report

    graph = {"tool": "expand_transaction_graph", "result": {"transactions": []}}
    fan = {"tool": "detect_fan_in", "result": {"detected": None, "transactions": []},
           "scope": {"truncated": True}}
    report = build_report({"case": {}, "stop_reason": "tool_budget_exhausted", "evidence": [graph, fan]})
    assert [item["status"] for item in report["hypotheses"]] == ["inconclusive", "not_examined"]


def test_readable_report_uses_only_verified_evidence():
    from graph_investigation_agent_for_aml.agent.report import (
        build_report,
        render_report,
    )

    transaction = {"transaction_id": "t1", "event_time": "2022-09-01T09:00:00", "amount_paid": "5.00",
                   "payment_currency": "USD", "amount_received": "5.00", "receiving_currency": "USD",
                   "source_bank": "1", "source_account": "A", "destination_bank": "1", "destination_account": "B"}
    graph = {"tool": "expand_transaction_graph", "result": {"transactions": [transaction]}}
    fan = {"tool": "detect_fan_in", "result": {"detected": True, "transactions": [transaction]}}
    report = build_report({"case": {"account": "B"}, "stop_reason": "agent_finished", "evidence": [graph, fan]})
    text = render_report(report)
    assert "t1: 2022-09-01T09:00:00" in text
    assert "paid 5.00 USD" in text
    assert "evidence[1]" in text
    assert "not proof of crime" in text
    assert "Rapid onward transfers: not examined" in text


def test_labeled_cases_against_fixed_baseline():
    """Labels are held outside the model input and describe the first six rows."""
    import json
    import os
    from pathlib import Path

    import psycopg
    import pytest

    from graph_investigation_agent_for_aml.agent.evaluation import (
        run_baseline,
        score_case,
    )

    dsn = os.environ.get("AML_AGENT_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set AML_AGENT_TEST_DATABASE_URL to the isolated replay database")
    labels = json.loads((Path(__file__).parent / "fixtures/agent-cases.json").read_text())
    with psycopg.connect(dsn) as connection:
        for label in labels:
            case = {"dataset": "graph-investigation", "bank": "01", "account": label["account"],
                    "start": "2022-09-01T09:00:00", "end": "2022-09-01T10:00:00"}
            report, _ = run_baseline(connection, case, max_tools=6, timeout_seconds=30)
            score = score_case(report, label["expected"])
            assert all(result["correct"] is True for result in score.values()), (label["account"], score)
            for result in score.values():
                if result["expected"]:
                    assert result["reference_precision"] == result["reference_recall"] == 1.0


def test_evaluation_keeps_missing_analysis_unscored():
    from graph_investigation_agent_for_aml.agent.evaluation import score_case
    from graph_investigation_agent_for_aml.agent.report import build_report

    expected = {"detect_fan_in": {"observed": True, "transaction_ids": ["t1"]},
                "detect_rapid_pass_through": {"observed": False, "transaction_ids": []}}
    report = build_report({"case": {}, "stop_reason": "model_budget_exhausted", "evidence": []})
    score = score_case(report, expected)
    assert score["detect_fan_in"]["correct"] is None
    assert score["detect_fan_in"]["reference_recall"] is None
    assert score["detect_rapid_pass_through"]["correct"] is None


def test_timeout_after_model_response_does_not_run_another_tool(monkeypatch):
    ticks = iter([0, 0, 0, 0, 0, 0, 121])
    monkeypatch.setattr(agent_graph.time, "monotonic", lambda: next(ticks, 121))
    calls = []

    def execute(connection, case, action, graph):
        calls.append(action)
        return {"tool": action, "result": {}, "scope": {}}

    monkeypatch.setattr(agent_graph, "execute", execute)
    graph = agent_graph.build_graph(Connection(), lambda *_: {"action": "get_counterparties", "purpose": "Check"})
    result = graph.invoke({"case": {}, "events": [], "evidence": [], "graph_data": None})
    assert calls == ["get_account_profile", "get_transactions"]
    assert result["report"]["stop_reason"] == "time_budget_exhausted"


def test_report_rejects_wrong_pair_time_and_incomplete_transaction():
    from graph_investigation_agent_for_aml.agent.report import build_report

    first = {"transaction_id": "t1", "event_time": "2022-09-01T09:00:00", "amount_paid": "5",
             "payment_currency": "USD", "amount_received": "5", "receiving_currency": "USD",
             "source_bank": "1", "source_account": "A", "destination_bank": "1", "destination_account": "B"}
    second = {**first, "transaction_id": "t2", "event_time": "2022-09-01T09:10:00"}
    graph = {"tool": "expand_transaction_graph", "result": {"transactions": [first, second]}}
    pair = {"incoming": first, "outgoing": second, "time_difference_seconds": 0,
            "equal_timestamp_order_unknown": False,
            "amount_comparison": {"currency_comparable": True, "incoming_received": "5", "outgoing_paid": "5",
                                  "incoming_currency": "USD", "outgoing_currency": "USD", "outgoing_not_greater_than_incoming": True}}
    rapid = {"tool": "detect_rapid_pass_through", "result": [pair]}
    fan = {"tool": "detect_fan_in", "result": {"transactions": [{"transaction_id": "t1"}]}}
    report = build_report({"case": {}, "stop_reason": "agent_finished", "evidence": [graph, rapid, fan]})
    assert report["findings"] == []
    assert {error["tool"] for error in report["validation_errors"]} == {"detect_rapid_pass_through", "detect_fan_in"}


def test_pydantic_request_and_decision_boundaries():
    import pytest
    from pydantic import ValidationError

    from graph_investigation_agent_for_aml.agent.models import (
        InvestigationRequest,
        ModelDecision,
    )

    case = {"dataset": "toy", "bank": "01", "account": "0001", "question": "check",
            "start": "2022-09-01T00:00:00", "end": "2022-09-02T00:00:00"}
    parsed = InvestigationRequest.model_validate(case)
    assert parsed.bank == "01" and parsed.account == "0001"
    for change in ({"account": " "}, {"start": "2022-09-03T00:00:00"},
                   {"start": "2022-09-01T00:00:00Z"}, {"as_of": "2022-09-01T00:00:00"}):
        with pytest.raises(ValidationError):
            InvestigationRequest.model_validate({**case, **change})
    assert InvestigationRequest.model_validate({**case, "as_of": "2022-09-01T00:00:00Z"}).as_of.utcoffset() is not None
    for decision in ({"action": "finish", "purpose": ""}, {"action": 1, "purpose": "check"},
                     {"action": "finish", "purpose": "check", "extra": "unexpected"}):
        with pytest.raises(ValidationError):
            ModelDecision.model_validate(decision)


def test_markdown_includes_comparison_and_model_usage():
    from graph_investigation_agent_for_aml.agent.report import (
        build_report,
        render_report,
    )

    report = build_report({"case": {}, "stop_reason": "agent_finished", "evidence": []})
    result = {"tool_calls": 2, "elapsed_seconds": 0.5, "validation_errors": 0,
              "stop_reason": "agent_finished", "hypotheses": {"Fan-in": "not_examined"}}
    report["comparison"] = {"agent": result, "baseline": result, "limitations": ["Not an accuracy estimate."]}
    report["model"] = {"provider": "deepseek", "requests": 1,
                       "usage": {"prompt_tokens": 12, "completion_tokens": 3},
                       "usage_limitation": "Monetary cost not calculated."}
    text = render_report(report)
    assert "## Fixed baseline comparison" in text and "agent: 2 tool calls" in text
    assert "## Model usage" in text and "reported input tokens: 12" in text
    assert "Not an accuracy estimate." in text


def test_configured_deepseek_model_is_sent_without_automatic_fallback(monkeypatch):
    import io
    import json

    from graph_investigation_agent_for_aml.agent.cli import model_decision

    payloads = []

    def open_request(request, timeout):
        payloads.append(json.loads(request.data))
        return io.BytesIO(b'{"choices":[{"message":{"content":"{\\"action\\":\\"finish\\",\\"purpose\\":\\"done\\"}"}}]}')

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    decide = model_decision("test-key", model="example/deepseek-model", max_calls=1)
    decide("Return JSON", {"evidence": []})
    assert payloads[0]["model"] == decide.model == "example/deepseek-model"
    assert payloads[0]["thinking"] == {"type": "disabled"}
    assert decide.calls == 1


def test_cli_exit_codes_signal_failed_or_incomplete_reports():
    from graph_investigation_agent_for_aml.agent.cli import report_exit_code

    report = {"stop_reason": "agent_finished", "validation_errors": [], "events": []}
    assert report_exit_code(report) == 0
    assert report_exit_code({**report, "stop_reason": "model_error: HTTP_402"}) == 1
    assert report_exit_code({**report, "events": [{"status": "failed"}]}) == 1
    assert report_exit_code({**report, "validation_errors": [{}]}) == 1
    assert report_exit_code({**report, "stop_reason": "tool_budget_exhausted"}) == 2


def test_cli_writes_partial_report_and_returns_failure_for_402(monkeypatch, tmp_path):
    import json
    import os
    import sys
    from urllib.error import HTTPError

    import psycopg
    import pytest

    from graph_investigation_agent_for_aml.agent import cli

    dsn = os.environ.get("AML_AGENT_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set AML_AGENT_TEST_DATABASE_URL to the isolated replay database")
    real_connect = psycopg.connect
    monkeypatch.setattr(cli.psycopg, "connect", lambda **kwargs: real_connect(dsn, **kwargs))
    monkeypatch.setattr(cli, "load_key", lambda *_: "test-key")

    def reject(*args, **kwargs):
        raise HTTPError("https://example.invalid", 402, "Payment Required", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", reject)
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", ["agent", "--dataset", "graph-investigation", "--bank", "01", "--account", "A",
                                      "--start", "2022-09-01T09:00:00", "--end", "2022-09-01T10:00:00",
                                      "--question", "Investigate", "--output", str(output)])
    assert cli.main() == 1
    report = json.loads(output.read_text())
    assert report["stop_reason"] == "model_error: HTTP_402"
    assert [event["status"] for event in report["events"]] == ["completed", "completed"]
    assert report["model"]["requests"] == 1


def test_hypotheses_update_in_state_before_next_decision(monkeypatch):
    contexts = []
    choices = iter(["expand_transaction_graph", "detect_fan_in", "finish"])

    def execute(connection, case, action, graph):
        if action == "expand_transaction_graph":
            result = {"transactions": []}
        elif action == "detect_fan_in":
            result = {"detected": False, "transactions": []}
        else:
            result = {}
        return {"tool": action, "result": result, "scope": {"truncated": False}}

    def decide(prompt, context):
        contexts.append(context)
        return {"action": next(choices), "purpose": "Check the case"}

    monkeypatch.setattr(agent_graph, "execute", execute)
    graph = agent_graph.build_graph(Connection(), decide, max_tools=6)
    result = graph.invoke({"case": {}, "events": [], "evidence": [], "graph_data": None})
    assert contexts[0]["hypotheses"][0]["status"] == "not_examined"
    assert contexts[2]["hypotheses"][0]["status"] == "indicator_not_observed"
    assert result["report"]["hypotheses"] == contexts[2]["hypotheses"]
