"""Run an on-demand case: python -m graph_investigation_agent_for_aml.agent.cli --help."""
import argparse
import json
import os
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from pydantic import ValidationError

from .evaluation import compare_reports, run_baseline
from .graph import ModelBudgetExceeded, build_graph
from .models import InvestigationRequest
from .report import render_report


def load_key(name="DS_key"):
    """Read only the requested key; never load or print unrelated credentials."""
    key = os.environ.get(name)
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            env_name, separator, value = line.removeprefix("export ").partition("=")
            if separator and env_name.strip() == name:
                key = value.strip().strip("\"'")
                break
    if not key:
        raise ValueError(f"Set {name} in .env or the environment")
    return key


def model_decision(key, max_calls=3, model=None):
    """Call the selected OpenAI-compatible endpoint using the standard library."""
    if not 1 <= max_calls <= 10:
        raise ValueError("Model call limit must be between 1 and 10")
    if model is None:
        model = "deepseek-flash"
    if not model.strip():
        raise ValueError("Model ID must not be empty")
    url = "https://api.deepseek.com/chat/completions"
    calls = 0
    def decide(system_prompt, context):
        nonlocal calls
        # Limit requests, including unsuccessful attempts, before contacting the provider.
        if calls >= max_calls:
            raise ModelBudgetExceeded("Model request limit reached")
        # Keep the full evidence in state and the report; send bounded summaries
        # to the model so a large subgraph cannot exhaust its context window.
        summarized = []
        for item in context["evidence"]:
            compact = dict(item)
            if item["tool"] == "expand_transaction_graph":
                compact["result"] = "Graph retained in state; scope contains its size and limits."
            else:
                encoded = json.dumps(compact.get("result"), ensure_ascii=False)
                if len(encoded) > 12000:
                    compact["result"] = {"preview": encoded[:12000], "preview_truncated": True}
            summarized.append(compact)
        context = {**context, "evidence": summarized}
        body = json.dumps({"model": model, "messages": [{"role": "system", "content": system_prompt},
                           {"role": "user", "content": json.dumps(context, ensure_ascii=False)}],
                           "temperature": 0, "max_tokens": 512, "response_format": {"type": "json_object"},
                           "thinking": {"type": "disabled"}}).encode()
        request = urllib.request.Request(url, data=body,
                                        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        calls += 1
        decide.calls = calls
        # A single request may not consume more than the remaining case budget.
        request_timeout = min(30, max(0.001, context.get("remaining_seconds", 30)))
        with urllib.request.urlopen(request, timeout=request_timeout) as response:
            data = json.load(response)
        usage = data.get("usage", {})
        for key_name in ("prompt_tokens", "completion_tokens"):
            if isinstance(usage.get(key_name), int):
                decide.usage[key_name] += usage[key_name]
        return json.loads(data["choices"][0]["message"]["content"])
    decide.model = model
    decide.calls = 0
    decide.usage = {"prompt_tokens": 0, "completion_tokens": 0}
    return decide


def report_exit_code(report):
    """Keep partial evidence while signalling failures and incomplete budgets."""
    if (report["stop_reason"].startswith("model_error:") or report["validation_errors"]
            or any(event["status"] == "failed" for event in report["events"])):
        return 1
    if report["stop_reason"] != "agent_finished":
        return 2
    return 0


def main():
    parser = argparse.ArgumentParser(description="Investigate stored transactions with a bounded LangGraph agent.")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--bank", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--start", required=True, help="Inclusive source-local timestamp, e.g. 2022-09-01T00:00:00")
    parser.add_argument("--end", required=True, help="Exclusive source-local timestamp")
    parser.add_argument("--question", required=True)
    parser.add_argument("--as-of", help="Only include records stored by this timezone-aware timestamp")
    parser.add_argument("--max-tools", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--max-model-calls", type=int, default=3, help="Maximum model requests, up to 10")
    parser.add_argument("--model", help="Explicit model ID; check provider pricing before use")
    parser.add_argument("--compare-baseline", action="store_true", help="Also run a fixed tool sequence for comparison")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--output", help="Write the selected report format to this path; otherwise print it")
    args = parser.parse_args()
    # Validate user input before contacting the database or model provider.
    try:
        request = InvestigationRequest(dataset=args.dataset, bank=args.bank, account=args.account,
                                       start=args.start, end=args.end, as_of=args.as_of, question=args.question)
    except ValidationError as exc:
        parser.error(str(exc))
    case = {**request.model_dump(mode="json"), "case_id": str(uuid4()),
            "trigger": "on_demand", "created_at": datetime.now(UTC).isoformat()}
    key = load_key()
    with psycopg.connect(connect_timeout=10) as connection:
        # A consistent snapshot keeps account and graph evidence comparable.
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        decide = model_decision(key, max_calls=args.max_model_calls, model=args.model)
        started = time.monotonic()
        result = build_graph(connection, decide, max_tools=args.max_tools,
                             timeout_seconds=args.timeout).invoke({"case": case, "evidence": [], "events": [], "graph_data": None},
                                                                   {"recursion_limit": 2 * args.max_tools + 5})
        elapsed = time.monotonic() - started
        if args.compare_baseline:
            baseline, baseline_elapsed = run_baseline(connection, case, max_tools=args.max_tools, timeout_seconds=args.timeout)
            result["report"]["comparison"] = compare_reports(result["report"], baseline, agent_seconds=elapsed, baseline_seconds=baseline_elapsed)
    result["report"]["model"] = {"provider": "deepseek", "model_id": decide.model, "requests": decide.calls, "usage": decide.usage,
                                 "usage_limitation": "Counts include only responses that supplied usage; monetary cost is not calculated."}
    output = (json.dumps(result["report"], ensure_ascii=False, indent=2)
              if args.format == "json" else render_report(result["report"]).rstrip())
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    # Reports are written even when the process signals a partial or failed run.
    return report_exit_code(result["report"])


if __name__ == "__main__":
    raise SystemExit(main())
