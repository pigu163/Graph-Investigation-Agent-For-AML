"""Local API for Flink alerts and analyst-initiated investigations."""
import asyncio
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal
from uuid import uuid4

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from .investigation import get_transaction_details
from .agent.cli import load_key, model_decision, report_exit_code
from .agent.graph import build_graph
from .agent.models import InvestigationRequest

app = FastAPI(title="AML investigation API")
SCORE_LIMITATION = "This score is a snapshot at scored_at. Earlier events may be missing due to ingestion delays, and later arrivals can change the current score."
monitoring_lock = threading.Lock()
monitoring_cache = {"expires": 0.0, "data": None}
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
                   allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"])


def authorize(authorization: Annotated[str | None, Header()] = None):
    token = os.getenv("AML_API_TOKEN", "")
    if token and not secrets.compare_digest(authorization or "", f"Bearer {token}"):
        raise HTTPException(status_code=401, detail="Authentication required")



@app.get("/api/models", dependencies=[Depends(authorize)])
def registered_models():
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise HTTPException(status_code=503, detail="MLflow tracking server is not configured")
    try:
        request = urllib.request.Request(
            tracking_uri.rstrip("/") + "/api/2.0/mlflow/registered-models/search",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.load(response)
        for model in payload.get("registered_models", []):
            for version in model.get("latest_versions", []):
                if version.get("run_id"):
                    query = urllib.parse.urlencode({"run_id": version["run_id"]})
                    with urllib.request.urlopen(
                        tracking_uri.rstrip("/") + "/api/2.0/mlflow/runs/get?" + query,
                        timeout=5,
                    ) as response:
                        run = json.load(response)
                    version["metrics"] = run.get("run", {}).get("data", {}).get("metrics", [])
        return {"models": payload.get("registered_models", []),
                "next_page_token": payload.get("next_page_token")}
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="MLflow tracking server unavailable") from exc


class StartInvestigation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    alert_id: str = Field(min_length=1, max_length=128)
    party: Literal["sender", "recipient"]


class Disposition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["accepted", "further_review", "closed"]
    note: str = Field(default="", max_length=2000)


class QueueInvestigation(StartInvestigation):
    priority: int = Field(default=0, ge=0, le=10)


def db_connect():
    return psycopg.connect(connect_timeout=3, row_factory=dict_row)


def read_alerts():
    with db_connect() as conn:
        rows = conn.execute("""SELECT a.payload, a.detected_at,
              EXISTS (SELECT 1 FROM transactions t WHERE t.transaction_id = a.transaction_id
                      AND t.source_dataset = a.source_dataset) AS transaction_available,
              (SELECT jsonb_build_object('score', s.score, 'model_version', s.model_version,
                                         'feature_cutoff', s.feature_cutoff, 'scored_at', s.scored_at,
                                         'limitation', %s::text)
               FROM transaction_scores s
               WHERE s.source_dataset = a.source_dataset AND s.transaction_id = a.transaction_id
               ORDER BY s.scored_at DESC LIMIT 1) AS score
              FROM risk_alerts a ORDER BY a.detected_at DESC, a.alert_id DESC LIMIT 100""",
                            (SCORE_LIMITATION,)).fetchall()
    return [{**row["payload"], "detected_at": row["detected_at"].isoformat(),
             "transaction_available": row["transaction_available"], "score": row["score"]} for row in rows]


def flink_status():
    url = os.getenv("AML_FLINK_REST_URL", "http://127.0.0.1:8081").rstrip("/") + "/jobs/overview"
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            jobs = json.load(response)["jobs"]
        return "running" if any(j["name"] == "aml-account-features-v2" and j["state"] == "RUNNING" for j in jobs) else "not_running"
    except (OSError, ValueError, KeyError):
        return "unavailable"


@app.get("/api/status", dependencies=[Depends(authorize)])
def status():
    try:
        with db_connect() as conn:
            ready = conn.execute("SELECT to_regclass('public.risk_alerts') IS NOT NULL AS ready").fetchone()["ready"]
    except psycopg.Error:
        return {"database": "unavailable", "flink": flink_status()}
    return {"database": "ready" if ready else "schema_missing", "flink": flink_status()}


@app.get("/api/alerts", dependencies=[Depends(authorize)])
def alerts():
    try:
        return {"alerts": read_alerts(), "limit": 100}
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Alert database unavailable or schema not initialized") from exc


@app.get("/api/monitoring", dependencies=[Depends(authorize)])
def monitoring():
    try:
        with monitoring_lock:
            if time.monotonic() < monitoring_cache["expires"]:
                return monitoring_cache["data"]
            with db_connect() as conn:
                totals = conn.execute("""SELECT count(*) AS transactions, max(stored_at) AS latest_archive
                                         FROM transactions""").fetchone()
                totals["alerts"] = conn.execute("SELECT count(*) AS count FROM risk_alerts").fetchone()["count"]
                trend = conn.execute("""SELECT date_trunc('hour', event_time) AS hour, count(*) AS transactions
                                        FROM transactions GROUP BY 1 ORDER BY 1 DESC LIMIT 24""").fetchall()
            result = jsonable_encoder({"totals": totals, "hourly_transactions": list(reversed(trend)),
                                       "updated_at": datetime.now(UTC)})
            monitoring_cache.update(data=result, expires=time.monotonic() + 30)
            return result
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Monitoring database unavailable or schema not initialized") from exc


@app.get("/api/alerts/stream", dependencies=[Depends(authorize)])
async def alert_stream():
    async def events():
        previous = None
        while True:
            try:
                payload = json.dumps({"alerts": await asyncio.to_thread(read_alerts), "limit": 100}, ensure_ascii=False)
                if payload != previous:
                    yield f"event: snapshot\ndata: {payload}\n\n"
                    previous = payload
                else:
                    yield ": heartbeat\n\n"
            except psycopg.Error:
                previous = None
                yield 'event: unavailable\ndata: {"message":"Alert database unavailable or schema not initialized"}\n\n'
            await asyncio.sleep(1)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def investigation_case(conn, alert_id, party):
    row = conn.execute("SELECT payload FROM risk_alerts WHERE alert_id = %s", (alert_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert = row["payload"]
    event = alert["transaction"]
    stored = conn.execute("""SELECT source_bank, source_account, destination_bank, destination_account,
        event_time, amount_paid, payment_currency, amount_received, receiving_currency
        FROM transactions WHERE source_dataset = %s AND transaction_id = %s""",
        (alert["source_dataset"], alert["transaction_id"])).fetchone()
    if stored is None:
        raise HTTPException(status_code=409, detail="Trigger transaction is not archived yet; retry shortly")
    fields = ("source_bank", "source_account", "destination_bank", "destination_account", "payment_currency", "receiving_currency")
    if (any(stored[name] != event[name] for name in fields)
            or stored["event_time"] != datetime.strptime(event["event_time"], "%Y/%m/%d %H:%M")
            or any(stored[name] != Decimal(event[name]) for name in ("amount_paid", "amount_received"))):
        raise HTTPException(status_code=409, detail="Alert transaction differs from archived transaction")
    prefix = "source" if party == "sender" else "destination"
    event_time = stored["event_time"]
    request = InvestigationRequest(dataset=alert["source_dataset"], bank=stored[f"{prefix}_bank"],
                                   account=stored[f"{prefix}_account"], start=event_time - timedelta(hours=24),
                                   end=event_time + timedelta(microseconds=1),
                                   question="Investigate the selected account's transaction patterns and consider legitimate explanations.")
    score = conn.execute("""SELECT score, model_version, feature_cutoff, scored_at FROM transaction_scores
                            WHERE source_dataset = %s AND transaction_id = %s
                            ORDER BY scored_at DESC LIMIT 1""",
                         (alert["source_dataset"], alert["transaction_id"])).fetchone()
    if score is not None:
        score["limitation"] = SCORE_LIMITATION
    return {**request.model_dump(mode="json"), "case_id": str(uuid4()), "trigger": "manual_alert",
            "alert_id": alert_id, "transaction_id": alert["transaction_id"],
            "score": jsonable_encoder(score), "selected_party": party, "created_at": datetime.now(UTC).isoformat()}


def run_investigation(conn, case):
    with db_connect() as owner:
        available = owner.execute(
            "SELECT pg_try_advisory_lock(hashtextextended('aml-investigation-capacity', 0)) AS available"
        ).fetchone()["available"]
        if not available:
            raise HTTPException(status_code=429, detail="An investigation is already running; retry when it completes")
        owner.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (case["case_id"],))
        return execute_investigation(conn, case)


def execute_investigation(conn, case):
    try:
        decide = model_decision(load_key(), max_calls=3)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="Investigation model is not configured") from exc
    with db_connect() as writer:
        writer.execute("INSERT INTO investigation_cases (case_id, case_data, status) VALUES (%s, %s, 'running')",
                       (case["case_id"], Jsonb(case)))
    try:
        result = build_graph(conn, decide, max_tools=8, timeout_seconds=120).invoke(
            {"case": case, "evidence": [], "events": [], "graph_data": None}, {"recursion_limit": 21})
        report = result["report"]
        if case.get("score") is not None:
            report["limitations"].append(SCORE_LIMITATION)
        report["model"] = {"provider": "deepseek", "model_id": decide.model,
                           "requests": decide.calls, "usage": decide.usage}
        report = jsonable_encoder(report)
        status = "completed" if report_exit_code(report) == 0 else "partial"
        with db_connect() as writer:
            writer.execute("UPDATE investigation_cases SET status = %s, report = %s, updated_at = now() WHERE case_id = %s",
                           (status, Jsonb(report), case["case_id"]))
        return report
    except Exception:
        with db_connect() as writer:
            writer.execute("UPDATE investigation_cases SET status = 'failed', error = %s, updated_at = now() WHERE case_id = %s",
                           ("Investigation failed", case["case_id"]))
        raise



@app.post("/api/investigations", dependencies=[Depends(authorize)])
def investigate(request: StartInvestigation):
    try:
        with db_connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            return run_investigation(conn, investigation_case(conn, request.alert_id, request.party))
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc


@app.post("/api/investigation-queue", dependencies=[Depends(authorize)])
def enqueue_investigation(request: QueueInvestigation):
    try:
        with db_connect() as conn:
            if conn.execute("SELECT 1 FROM risk_alerts WHERE alert_id = %s", (request.alert_id,)).fetchone() is None:
                raise HTTPException(status_code=404, detail="Alert not found")
            row = conn.execute("""
                INSERT INTO investigation_queue (alert_id, party, priority)
                VALUES (%s, %s, %s)
                ON CONFLICT (alert_id, party) DO UPDATE
                  SET priority = GREATEST(investigation_queue.priority, EXCLUDED.priority),
                      updated_at = now()
                RETURNING alert_id, party, priority, status, case_id, error
            """, (request.alert_id, request.party, request.priority)).fetchone()
        return jsonable_encoder(row)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation queue unavailable") from exc


@app.get("/api/investigation-queue", dependencies=[Depends(authorize)])
def list_investigation_queue():
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT alert_id, party, priority, status, case_id, error, created_at, updated_at
                FROM investigation_queue ORDER BY priority DESC, created_at LIMIT 100
            """).fetchall()
        return jsonable_encoder({"items": rows})
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation queue unavailable") from exc


@app.post("/api/investigation-queue/retry", dependencies=[Depends(authorize)])
def retry_investigation(request: StartInvestigation):
    try:
        with db_connect() as conn:
            row = conn.execute("""
                UPDATE investigation_queue
                SET status = 'queued', error = NULL, case_id = NULL, available_at = now(), updated_at = now()
                WHERE alert_id = %s AND party = %s AND status IN ('failed', 'partial')
                RETURNING alert_id, party, status
            """, (request.alert_id, request.party)).fetchone()
        if row is None:
            raise HTTPException(status_code=409, detail="Only failed or partial queue entries can be retried")
        return jsonable_encoder(row)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation queue unavailable") from exc


@app.post("/api/investigations/on-demand", dependencies=[Depends(authorize)])
def investigate_on_demand(request: InvestigationRequest):
    case = {**request.model_dump(mode="json"), "case_id": str(uuid4()), "trigger": "on_demand",
            "created_at": datetime.now(UTC).isoformat()}
    try:
        with db_connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            return run_investigation(conn, case)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc


@app.get("/api/transactions", dependencies=[Depends(authorize)])
def transaction_details(dataset: str, transaction_id: str):
    try:
        with db_connect() as conn:
            result = get_transaction_details(conn, dataset, transaction_id)
        if result["result"] is None:
            raise HTTPException(status_code=404, detail="Transaction not found")
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc


@app.get("/api/investigations", dependencies=[Depends(authorize)])
def investigations():
    try:
        with db_connect() as conn:
            conn.execute("""UPDATE investigation_cases SET status = 'failed',
                            error = 'Investigation interrupted', updated_at = now()
                            WHERE status = 'running'
                            AND pg_try_advisory_xact_lock(hashtextextended(case_id::text, 0))""")
            rows = conn.execute("""SELECT case_id, case_data, status, disposition, created_at, updated_at
                                   FROM investigation_cases ORDER BY created_at DESC LIMIT 100""").fetchall()
        return jsonable_encoder({"cases": rows})
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc


@app.get("/api/investigations/{case_id}", dependencies=[Depends(authorize)])
def get_investigation(case_id: str):
    try:
        with db_connect() as conn:
            conn.execute("""UPDATE investigation_cases SET status = 'failed',
                            error = 'Investigation interrupted', updated_at = now()
                            WHERE case_id::text = %s AND status = 'running'
                            AND pg_try_advisory_xact_lock(hashtextextended(case_id::text, 0))""", (case_id,))
            row = conn.execute("""SELECT case_id, case_data AS "case", status, report, error, disposition,
                                   created_at, updated_at FROM investigation_cases WHERE case_id::text = %s""",
                               (case_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Investigation not found")
        return jsonable_encoder(row)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc


@app.post("/api/investigations/{case_id}/disposition", dependencies=[Depends(authorize)])
def set_disposition(case_id: str, disposition: Disposition):
    try:
        with db_connect() as conn:
            row = conn.execute("""UPDATE investigation_cases SET disposition = %s, updated_at = now()
                                  WHERE case_id::text = %s RETURNING case_id""",
                               (Jsonb(disposition.model_dump()), case_id)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Investigation not found")
        return {"case_id": str(row["case_id"]), "disposition": disposition.model_dump()}
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc



@app.get("/api/investigations/{case_id}/graph", dependencies=[Depends(authorize)])
def investigation_graph(case_id: str):
    case = get_investigation(case_id)
    report = case["report"]
    if report is None:
        raise HTTPException(status_code=409, detail="Investigation report is not available")
    graph = next((item for item in report["evidence"] if item["tool"] == "expand_transaction_graph"), None)
    if graph is None:
        raise HTTPException(status_code=404, detail="The investigation did not expand a transaction graph")
    return graph
