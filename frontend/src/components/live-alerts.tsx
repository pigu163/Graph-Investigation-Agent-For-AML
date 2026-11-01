"use client";

import { useEffect, useState } from "react";
import { Activity, AlertTriangle, ArrowRight, ShieldAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import { GlassPanel } from "@/components/glass-panel";

type Transaction = {
  transaction_id: string; source_bank: string; source_account: string;
  destination_bank: string; destination_account: string; event_time: string;
  amount_paid: string; payment_currency: string; amount_received: string; receiving_currency: string;
};
type Alert = {
  alert_id: string; alert_type: string; rule_version: string; source_dataset: string;
  bank_id: string; account_id: string; currency: string; observed: string; threshold: string;
  event_time: string; detected_at: string; window_start?: string; window_end?: string;
  transaction_available: boolean; transaction: Transaction;
};
type Report = {
  case: { bank: string; account: string; start: string; end: string; selected_party: string };
  stop_reason: string; summary: string; limitations: string[]; next_steps: string[];
  hypotheses: { hypothesis: string; status: string }[];
  events: { action: string; status: string; time: string; error?: string }[];
  findings: { tool: string; verified_transaction_ids: string[] }[];
  evidence: unknown[]; validation_errors: unknown[];
  model: { requests: number; model_id: string };
};

const label = (type: string) => ({ fan_in: "Fan-in threshold", fan_out: "Fan-out threshold", large_transaction: "Large transaction" }[type] || type);

export function LiveAlerts() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedId, setSelectedId] = useState<string>();
  const [connection, setConnection] = useState("Connecting to alert stream…");
  const [flink, setFlink] = useState("unknown");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [report, setReport] = useState<Report>();
  const selected = alerts.find((a) => a.alert_id === selectedId) || alerts[0];

  useEffect(() => {
    const stream = new EventSource("/api/backend/alerts/stream");
    stream.addEventListener("snapshot", (event) => {
      try { setAlerts((JSON.parse((event as MessageEvent).data) as { alerts: Alert[] }).alerts); setConnection("Connected"); }
      catch { setConnection("Invalid response from alert service"); }
    });
    stream.addEventListener("unavailable", () => setConnection("Alert database unavailable"));
    stream.onerror = () => setConnection("Disconnected · retrying");
    const controller = new AbortController();
    async function checkStatus() {
      try {
        const response = await fetch("/api/backend/status", { cache: "no-store", signal: controller.signal });
        if (!response.ok) throw new Error();
        const data = await response.json() as { database: string; flink: string };
        setFlink(data.flink);
        if (data.database !== "ready") setConnection(data.database === "schema_missing" ? "Alert schema not initialized" : "Alert database unavailable");
      } catch { if (!controller.signal.aborted) setFlink("unavailable"); }
    }
    void checkStatus();
    const timer = setInterval(checkStatus, 5000);
    return () => { controller.abort(); clearInterval(timer); stream.close(); };
  }, []);

  async function investigate(party: "sender" | "recipient") {
    if (!selected) return;
    setSelectedId(selected.alert_id); setRunning(true); setError(""); setReport(undefined);
    try {
      const response = await fetch("/api/backend/investigations", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ alert_id: selected.alert_id, party }) });
      const data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Investigation failed");
      setReport(data as Report);
    } catch (err) { setError(err instanceof Error ? err.message : "Investigation failed"); }
    finally { setRunning(false); }
  }

  return <div className="space-y-6">
    <header className="pt-3 md:pt-6"><p className="mb-3 text-xs tracking-[.2em] text-primary uppercase">Monitoring / live rules</p><h1 className="text-3xl font-semibold tracking-tight md:text-4xl">Transaction alerts</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">Rule crossings from the Flink transaction stream. Select an alert to inspect the triggering transaction and investigate either account.</p></header>
    <GlassPanel className="flex flex-wrap items-center gap-x-6 gap-y-2 px-5 py-4 text-sm" aria-live="polite"><span className="flex items-center gap-2"><Activity className="size-4 text-primary" aria-hidden="true" />{connection}</span><span className="text-muted-foreground">Flink job: {flink === "running" ? "running" : flink === "not_running" ? "not running" : flink === "unavailable" ? "unavailable" : "checking"}</span><span className="text-xs text-muted-foreground">Latest 100 alerts · thresholds are configurable demo rules</span></GlassPanel>
    {flink !== "running" && <p className="text-sm text-muted-foreground">Live monitoring is not confirmed. Previously saved alerts may still be shown.</p>}
    <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(360px,0.9fr)]">
      <section className="content-panel min-w-0 p-5" aria-labelledby="alerts-heading"><div className="mb-5 flex items-center justify-between"><div><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Rule crossings</p><h2 id="alerts-heading" className="mt-1 text-lg font-medium">Recent alerts</h2></div><ShieldAlert className="size-5 text-primary" aria-hidden="true" /></div>
        {alerts.length === 0 ? <p className="rounded-xl border border-white/10 bg-black/10 p-5 text-sm text-muted-foreground">{connection === "Connected" ? "No alerts in the current result." : "No alert data available while the connection is unavailable."}</p> :
        <ul className="space-y-2">{alerts.map((a) => <li key={a.alert_id}><button type="button" disabled={running} onClick={() => { setSelectedId(a.alert_id); setReport(undefined); setError(""); }} aria-pressed={selected?.alert_id === a.alert_id} className={`w-full rounded-2xl border p-4 text-left transition-colors disabled:cursor-wait ${selected?.alert_id === a.alert_id ? "border-primary/50 bg-primary/10" : "border-white/10 bg-black/10 hover:bg-white/5"}`}><span className="flex flex-wrap items-center justify-between gap-2"><span className="text-sm font-medium">{label(a.alert_type)}</span><span className="text-xs text-muted-foreground">{a.event_time} · source time</span></span><span className="mt-2 block text-sm">{a.bank_id} / {a.account_id} <span className="text-muted-foreground">· {a.source_dataset}</span></span><span className="mt-2 block text-xs text-muted-foreground">Observed {a.observed} {a.alert_type === "large_transaction" ? a.currency : "counterparties"} · threshold {a.threshold} · {a.rule_version}</span></button></li>)}</ul>}
      </section>
      <section className="content-panel min-w-0 p-5" aria-labelledby="details-heading"><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Evidence</p><h2 id="details-heading" className="mt-1 text-lg font-medium">Triggering transaction</h2>
        {!selected ? <p className="mt-5 text-sm text-muted-foreground">Select an alert to see the transaction and investigation options.</p> : <div className="mt-5 space-y-5"><div className="rounded-2xl border border-white/10 bg-black/15 p-4 text-sm"><p className="break-all font-medium">{selected.transaction.transaction_id}</p><p className="mt-2 text-muted-foreground">{selected.transaction.event_time} · source local time, timezone unknown</p><div className="mt-4 grid gap-3 sm:grid-cols-2"><div><p className="text-xs text-muted-foreground">Sender</p><p className="mt-1 break-all">{selected.transaction.source_bank} / {selected.transaction.source_account}</p></div><div><p className="text-xs text-muted-foreground">Recipient</p><p className="mt-1 break-all">{selected.transaction.destination_bank} / {selected.transaction.destination_account}</p></div></div><p className="mt-4 tabular-nums">Paid {selected.transaction.amount_paid} {selected.transaction.payment_currency} <ArrowRight className="inline size-4" aria-hidden="true" /> received {selected.transaction.amount_received} {selected.transaction.receiving_currency}</p>{selected.window_start && <p className="mt-3 text-xs text-muted-foreground">Event-time window: {selected.window_start} to {selected.window_end}</p>}</div>
          <div><p className="text-sm font-medium">Start a manual investigation</p><p className="mt-1 text-xs leading-5 text-muted-foreground">Uses the selected account and the 24 hours ending at this transaction. Up to 3 model requests; the report may be partial. No model call happens until you choose an account.</p><div className="mt-3 flex flex-wrap gap-2"><Button size="sm" disabled={running || !selected.transaction_available} onClick={() => void investigate("sender")}>Investigate sender</Button><Button size="sm" variant="outline" disabled={running || !selected.transaction_available} onClick={() => void investigate("recipient")}>Investigate recipient</Button></div>{!selected.transaction_available && <p className="mt-2 text-xs text-muted-foreground">Waiting for the trigger transaction to be archived.</p>}{running && <p className="mt-3 text-sm text-primary" role="status">Investigation running; waiting for the Agent report…</p>}{error && <p className="mt-3 text-sm text-destructive" role="alert">{error}</p>}</div>
        </div>}
      </section>
    </div>
    {report && <section className="content-panel p-5 md:p-7" aria-labelledby="report-heading"><div className="flex items-start gap-3"><AlertTriangle className="mt-1 size-5 shrink-0 text-chart-4" aria-hidden="true" /><div><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Investigation report · {report.case.selected_party}</p><h2 id="report-heading" className="mt-1 text-xl font-medium">{report.case.bank} / {report.case.account}</h2><p className="mt-2 text-sm text-muted-foreground">{report.case.start} to {report.case.end} · stop: {report.stop_reason} · {report.model.requests} model requests</p></div></div><p className="mt-5 text-sm leading-6">{report.summary}</p><div className="mt-5 grid gap-4 md:grid-cols-2"><div><h3 className="text-sm font-medium">Hypotheses</h3><ul className="mt-2 space-y-2 text-sm text-muted-foreground">{report.hypotheses.map((h) => <li key={h.hypothesis}>{h.hypothesis}: {h.status.replaceAll("_", " ")}</li>)}</ul></div><div><h3 className="text-sm font-medium">Actual tool events</h3><ul className="mt-2 space-y-2 text-sm text-muted-foreground">{report.events.map((e, i) => <li key={`${e.action}-${i}`}>{e.action}: {e.status}{e.error ? ` (${e.error})` : ""}</li>)}</ul></div></div><div className="mt-5"><h3 className="text-sm font-medium">Verified transaction references</h3>{report.findings.length ? <ul className="mt-2 space-y-2 break-all text-sm text-muted-foreground">{report.findings.map((f) => <li key={f.tool}>{f.tool}: {f.verified_transaction_ids.join(", ") || "No transaction references"}</li>)}</ul> : <p className="mt-2 text-sm text-muted-foreground">No verified findings returned.</p>}</div><details className="mt-5 text-sm"><summary className="cursor-pointer font-medium">Inspect full tool evidence ({report.evidence.length})</summary><pre className="mt-3 max-h-96 overflow-auto rounded-xl bg-black/25 p-4 text-xs">{JSON.stringify(report.evidence, null, 2)}</pre></details><p className="mt-5 text-xs leading-5 text-muted-foreground">{report.limitations.join(" ")} {report.validation_errors.length > 0 ? `${report.validation_errors.length} evidence validation errors.` : ""}</p></section>}
  </div>;
}
