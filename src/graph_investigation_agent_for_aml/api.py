"""Local API for Flink alerts and analyst-initiated investigations."""
import asyncio
import json
import os
import secrets
import urllib.error
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
from pydantic import BaseModel, ConfigDict, Field

from .agent.cli import load_key, model_decision
from .agent.graph import build_graph
from .agent.models import InvestigationRequest

app = FastAPI(title="AML investigation API")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
                   allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"])


def authorize(authorization: Annotated[str | None, Header()] = None):
    token = os.getenv("AML_API_TOKEN", "")
    if token and not secrets.compare_digest(authorization or "", f"Bearer {token}"):
        raise HTTPException(status_code=401, detail="Authentication required")


class StartInvestigation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    alert_id: str = Field(min_length=1, max_length=128)
    party: Literal["sender", "recipient"]


def db_connect():
    return psycopg.connect(connect_timeout=3, row_factory=dict_row)


def read_alerts():
    with db_connect() as conn:
        rows = conn.execute("""SELECT a.payload, a.detected_at,
              EXISTS (SELECT 1 FROM transactions t WHERE t.transaction_id = a.transaction_id
                      AND t.source_dataset = a.source_dataset) AS transaction_available
              FROM risk_alerts a ORDER BY a.detected_at DESC, a.alert_id DESC LIMIT 100""").fetchall()
    return [{**row["payload"], "detected_at": row["detected_at"].isoformat(),
             "transaction_available": row["transaction_available"]} for row in rows]


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
    return {**request.model_dump(mode="json"), "case_id": str(uuid4()), "trigger": "manual_alert",
            "alert_id": alert_id, "selected_party": party, "created_at": datetime.now(UTC).isoformat()}


@app.post("/api/investigations", dependencies=[Depends(authorize)])
def investigate(request: StartInvestigation):
    # One analyst click is one bounded synchronous investigation; no LLM is called by the alert pipeline.
    try:
        with db_connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            case = investigation_case(conn, request.alert_id, request.party)
            try:
                decide = model_decision(load_key(), max_calls=3)
            except ValueError as exc:
                raise HTTPException(status_code=503, detail="Investigation model is not configured") from exc
            result = build_graph(conn, decide, max_tools=8, timeout_seconds=120).invoke(
                {"case": case, "evidence": [], "events": [], "graph_data": None}, {"recursion_limit": 21})
            report = result["report"]
            report["model"] = {"provider": "deepseek", "model_id": decide.model,
                               "requests": decide.calls, "usage": decide.usage}
            return jsonable_encoder(report)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Investigation database unavailable") from exc
