"use client";

import { useCallback, useEffect, useState } from "react";
import dynamic from "next/dynamic";
const CaseGraph = dynamic(() => import("@/components/case-graph"), { ssr: false });
import type { ComponentProps } from "react";
import { Activity, AlertTriangle, ArrowRight, ShieldAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import { GlassPanel } from "@/components/glass-panel";
import { MonitoringSummary } from "@/components/monitoring-summary";

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
  score: { score: number; model_version: string; feature_cutoff: string; scored_at: string; limitation: string } | null;
};
type Report = {
  case: { case_id: string; bank: string; account: string; start: string; end: string; selected_party?: string };
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
  const [queueStatus, setQueueStatus] = useState("");
  const [queued, setQueued] = useState<{alert_id: string; party: string; status: string; error: string | null; case_id: string | null}[]>([]);
  const [report, setReport] = useState<Report>();
  const [cases, setCases] = useState<{case_id: string; case_data: {bank: string; account: string}; status: string}[]>([]);
  const [caseId, setCaseId] = useState<string>();
  const [caseStatus, setCaseStatus] = useState("");
  const [decision, setDecision] = useState("accepted");
  const [note, setNote] = useState("");
  const [graph, setGraph] = useState<ComponentProps<typeof CaseGraph>["snapshot"]>();
  const [graphError, setGraphError] = useState("");
  const [transactionId, setTransactionId] = useState<string | null>(null);
  const selectTransaction = useCallback((id: string) => setTransactionId(id), []);
  const [request, setRequest] = useState({dataset: "", bank: "", account: "", start: "", end: "", question: ""});
  const rankedAlerts = [...alerts].sort((a, b) => (b.score?.score ?? -1) - (a.score?.score ?? -1));
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
      try {
        const response = await fetch("/api/backend/investigation-queue", { cache: "no-store", signal: controller.signal });
        if (response.ok) setQueued((await response.json()).items);
      } catch { /* The service status above shows connectivity failures. */ }
    }
    void checkStatus();
    void fetch("/api/backend/investigations").then(async (response) => {
      if (!response.ok) throw new Error();
      setCases((await response.json()).cases);
    }).catch(() => setCaseStatus("Saved investigations unavailable"));
    const timer = setInterval(checkStatus, 5000);
    return () => { controller.abort(); clearInterval(timer); stream.close(); };
  }, []);

  function resetCase() {
    setCaseId(undefined);
    setReport(undefined);
    setGraph(undefined);
    setGraphError("");
    setTransactionId(null);
    setDecision("accepted");
    setNote("");
    setCaseStatus("");
  }

  async function investigate(party: "sender" | "recipient") {
    if (!selected) return;
    setSelectedId(selected.alert_id); setRunning(true); setError("");
    resetCase();
    try {
      const response = await fetch("/api/backend/investigations", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ alert_id: selected.alert_id, party }) });
      const data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Investigation failed");
      setReport(data as Report);
      setCaseId(data.case.case_id);
      const casesResponse = await fetch("/api/backend/investigations");
      if (casesResponse.ok) setCases((await casesResponse.json()).cases);
    } catch (err) { setError(err instanceof Error ? err.message : "Investigation failed"); }
    finally { setRunning(false); }
  }

  async function enqueue(party: "sender" | "recipient") {
    if (!selected) return;
    setQueueStatus("");
    try {
      const response = await fetch("/api/backend/investigation-queue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ alert_id: selected.alert_id, party, priority: 0 }),
      });
      const item = await response.json();
      if (!response.ok) throw new Error(item.detail || "Queue unavailable");
      setQueueStatus(`${party}: ${item.status}`);
    } catch (err) {
      setQueueStatus(err instanceof Error ? err.message : "Queue unavailable");
    }
  }

  async function retryQueued(alertId: string, party: string) {
    try {
      const response = await fetch("/api/backend/investigation-queue/retry", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ alert_id: alertId, party }),
      });
      if (!response.ok) throw new Error("Unable to retry investigation");
      setQueued((items) => items.map((item) => item.alert_id === alertId && item.party === party
        ? { ...item, status: "queued", error: null, case_id: null } : item));
    } catch (err) {
      setQueueStatus(err instanceof Error ? err.message : "Queue unavailable");
    }
  }

  async function showTransaction(id: string) {
    if (!caseId) return;
    setTransactionId(id); setGraphError("");
    try {
      const response = await fetch(`/api/backend/investigations/${encodeURIComponent(caseId)}/graph`);
      if (!response.ok) throw new Error("No saved graph is available for this investigation.");
      const snapshot = await response.json();
      setGraph(snapshot);
      if (!snapshot.result.transactions.some((row: { transaction_id: string }) => row.transaction_id === id))
        setGraphError("This transaction is outside the saved graph.");
    } catch (err) { setGraphError(err instanceof Error ? err.message : "Graph unavailable"); }
  }

  async function openCase(id: string) {
    setCaseStatus(""); setReport(undefined); setCaseId(id); setGraph(undefined); setGraphError(""); setTransactionId(null);
    try {
      const response = await fetch(`/api/backend/investigations/${encodeURIComponent(id)}`);
      if (!response.ok) throw new Error("Could not open investigation");
      const data = await response.json();
      setCaseStatus(data.error || data.status);
      setReport(data.report || undefined);
      setDecision(data.disposition?.decision || "accepted");
      setNote(data.disposition?.note || "");
    } catch (err) { setCaseStatus(err instanceof Error ? err.message : "Could not open investigation"); }
  }

  async function investigateOnDemand(event: React.FormEvent) {
    event.preventDefault(); setRunning(true); setError("");
    resetCase();
    try {
      const response = await fetch("/api/backend/investigations/on-demand", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request),
      });
      const data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Investigation failed");
      setReport(data as Report); setCaseId(data.case.case_id);
      const casesResponse = await fetch("/api/backend/investigations");
      if (casesResponse.ok) setCases((await casesResponse.json()).cases);
    } catch (err) { setError(err instanceof Error ? err.message : "Investigation failed"); }
    finally { setRunning(false); }
  }

  async function saveDisposition() {
    if (!caseId) return;
    try {
      const response = await fetch(`/api/backend/investigations/${encodeURIComponent(caseId)}/disposition`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ decision, note }),
      });
      if (!response.ok) throw new Error("Could not save disposition");
      setCaseStatus("Disposition saved");
    } catch (err) { setCaseStatus(err instanceof Error ? err.message : "Could not save disposition"); }
  }

  return <div className="space-y-6">
    <header className="pt-3 md:pt-6"><p className="mb-3 text-xs tracking-[.2em] text-primary uppercase">Monitoring / live rules</p><h1 className="text-3xl font-semibold tracking-tight md:text-4xl">Transaction alerts</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">Rule crossings from the Flink transaction stream. Select an alert to inspect the triggering transaction and investigate either account.</p></header>
    <MonitoringSummary />
    <GlassPanel className="flex flex-wrap items-center gap-x-6 gap-y-2 px-5 py-4 text-sm" aria-live="polite"><span className="flex items-center gap-2"><Activity className="size-4 text-primary" aria-hidden="true" />{connection}</span><span className="text-muted-foreground">Flink job: {flink === "running" ? "running" : flink === "not_running" ? "not running" : flink === "unavailable" ? "unavailable" : "checking"}</span><span className="text-xs text-muted-foreground">Latest 100 alerts · thresholds are configurable demo rules</span></GlassPanel>
    {flink !== "running" && <p className="text-sm text-muted-foreground">Live monitoring is not confirmed. Previously saved alerts may still be shown.</p>}
    <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(360px,0.9fr)]">
      <section className="content-panel min-w-0 p-5" aria-labelledby="alerts-heading"><div className="mb-5 flex items-center justify-between"><div><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Rule crossings</p><h2 id="alerts-heading" className="mt-1 text-lg font-medium">Recent alerts</h2></div><ShieldAlert className="size-5 text-primary" aria-hidden="true" /></div>
        {alerts.length === 0 ? <p className="rounded-xl border border-white/10 bg-black/10 p-5 text-sm text-muted-foreground">{connection === "Connected" ? "No alerts in the current result." : "No alert data available while the connection is unavailable."}</p> :
        <ul className="space-y-2">{rankedAlerts.map((a) => <li key={a.alert_id}><button type="button" disabled={running} onClick={() => { setSelectedId(a.alert_id); setReport(undefined); setError(""); setCaseId(undefined); setGraph(undefined); setGraphError(""); setTransactionId(null); }} aria-pressed={selected?.alert_id === a.alert_id} className={`w-full rounded-2xl border p-4 text-left transition-colors disabled:cursor-wait ${selected?.alert_id === a.alert_id ? "border-primary/50 bg-primary/10" : "border-white/10 bg-black/10 hover:bg-white/5"}`}><span className="flex flex-wrap items-center justify-between gap-2"><span className="text-sm font-medium">{label(a.alert_type)}</span><span className="text-xs text-muted-foreground">{a.event_time} · source time</span></span><span className="mt-2 block text-sm">{a.bank_id} / {a.account_id} <span className="text-muted-foreground">· {a.source_dataset}</span></span><span className="mt-2 block text-xs text-muted-foreground">Observed {a.observed} {a.alert_type === "large_transaction" ? a.currency : "counterparties"} · threshold {a.threshold} · {a.rule_version}</span><span className="mt-2 block text-xs text-muted-foreground">{a.score ? `Ranking score ${a.score.score.toFixed(3)} · ${a.score.model_version} · feature cutoff ${a.score.feature_cutoff} · ${a.score.limitation}` : "Ranking score unavailable"}</span></button></li>)}</ul>}
      </section>
      <section className="content-panel min-w-0 p-5" aria-labelledby="details-heading"><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Evidence</p><h2 id="details-heading" className="mt-1 text-lg font-medium">Triggering transaction</h2>
        {!selected ? <p className="mt-5 text-sm text-muted-foreground">Select an alert to see the transaction and investigation options.</p> : <div className="mt-5 space-y-5"><div className="rounded-2xl border border-white/10 bg-black/15 p-4 text-sm"><p className="break-all font-medium">{selected.transaction.transaction_id}</p><p className="mt-2 text-muted-foreground">{selected.transaction.event_time} · source local time, timezone unknown</p><div className="mt-4 grid gap-3 sm:grid-cols-2"><div><p className="text-xs text-muted-foreground">Sender</p><p className="mt-1 break-all">{selected.transaction.source_bank} / {selected.transaction.source_account}</p></div><div><p className="text-xs text-muted-foreground">Recipient</p><p className="mt-1 break-all">{selected.transaction.destination_bank} / {selected.transaction.destination_account}</p></div></div><p className="mt-4 tabular-nums">Paid {selected.transaction.amount_paid} {selected.transaction.payment_currency} <ArrowRight className="inline size-4" aria-hidden="true" /> received {selected.transaction.amount_received} {selected.transaction.receiving_currency}</p>{selected.window_start && <p className="mt-3 text-xs text-muted-foreground">Event-time window: {selected.window_start} to {selected.window_end}</p>}</div>
          <div><p className="text-sm font-medium">Start a manual investigation</p><p className="mt-1 text-xs leading-5 text-muted-foreground">Uses the selected account and the 24 hours ending at this transaction. Up to 3 model requests; the report may be partial. No model call happens until you choose an account.</p><div className="mt-3 flex flex-wrap gap-2"><Button size="sm" disabled={running || !selected.transaction_available} onClick={() => void investigate("sender")}>Investigate sender</Button><Button size="sm" variant="outline" disabled={running || !selected.transaction_available} onClick={() => void investigate("recipient")}>Investigate recipient</Button><Button size="sm" variant="outline" onClick={() => void enqueue("sender")}>Queue sender</Button><Button size="sm" variant="outline" onClick={() => void enqueue("recipient")}>Queue recipient</Button></div>{queueStatus && <p className="mt-2 text-sm" role="status">{queueStatus}</p>}{!selected.transaction_available && <p className="mt-2 text-xs text-muted-foreground">Waiting for the trigger transaction to be archived.</p>}{running && <p className="mt-3 text-sm text-primary" role="status">Investigation running; waiting for the Agent report…</p>}{error && <p className="mt-3 text-sm text-destructive" role="alert">{error}</p>}</div>
        </div>}
      </section>
    </div>
    <section className="content-panel p-5">
      <h2 className="text-lg font-medium">Investigate an account</h2>
      <form className="mt-4 grid gap-3 sm:grid-cols-2" onSubmit={(event) => void investigateOnDemand(event)}>
        {(["dataset", "bank", "account", "start", "end", "question"] as const).map((field) =>
          <label key={field} className="text-sm capitalize">{field}
            <input required type={field === "start" || field === "end" ? "datetime-local" : "text"} className="mt-1 block w-full rounded bg-slate-800 p-2"
              value={request[field]} onChange={(event) => setRequest({...request, [field]: event.target.value})} />
          </label>)}
        <Button disabled={running} type="submit">Investigate</Button>
        {running && <p role="status">Investigation running…</p>}
        {error && <p role="alert" className="text-destructive">{error}</p>}
      </form>
    </section>
    <section className="content-panel p-5">
      <h2 className="text-lg font-medium">Investigation queue</h2>
      {queued.length === 0 ? <p className="mt-3 text-sm text-muted-foreground">No queued investigations.</p> :
        <ul className="mt-3 space-y-2">{queued.map((item) => <li className="text-sm" key={`${item.alert_id}-${item.party}`}>{item.alert_id} · {item.party} · {item.status}{item.error ? ` · ${item.error}` : ""}{item.case_id && <button type="button" className="ml-2 underline" onClick={() => void openCase(item.case_id!)}>Open case</button>}{["failed", "partial"].includes(item.status) && <button type="button" className="ml-2 underline" onClick={() => void retryQueued(item.alert_id, item.party)}>Retry</button>}</li>)}</ul>}
    </section>
    <section className="content-panel p-5">
      <h2 className="text-lg font-medium">Saved investigations</h2>
      {cases.length === 0 ? <p className="mt-3 text-sm text-muted-foreground">No saved investigations available.</p> :
        <ul className="mt-3 space-y-2">{cases.map((item) => <li key={item.case_id}>
          <button type="button" className="text-left text-sm underline" onClick={() => void openCase(item.case_id)}>
            {item.case_data.bank} / {item.case_data.account} · {item.status} · {item.case_id}
          </button>
        </li>)}</ul>}
      {caseStatus && <p className="mt-3 text-sm" role="status">{caseStatus}</p>}
      {caseId && <div className="mt-4 space-y-3">
        <label className="block text-sm">Decision
          <select className="ml-3 rounded bg-slate-800 p-2" value={decision} onChange={(event) => setDecision(event.target.value)}>
            <option value="accepted">Accept evidence</option><option value="further_review">Further review</option><option value="closed">Close case</option>
          </select>
        </label>
        <label className="block text-sm">Note
          <textarea className="mt-2 block w-full rounded bg-slate-800 p-2" value={note} maxLength={2000} onChange={(event) => setNote(event.target.value)} />
        </label>
        <Button size="sm" onClick={() => void saveDisposition()}>Save disposition</Button>
      </div>}
    </section>
    {report && <section className="content-panel p-5 md:p-7" aria-labelledby="report-heading"><div className="flex items-start gap-3"><AlertTriangle className="mt-1 size-5 shrink-0 text-chart-4" aria-hidden="true" /><div><p className="text-xs tracking-[.16em] text-muted-foreground uppercase">Investigation report · {report.case.selected_party}</p><h2 id="report-heading" className="mt-1 text-xl font-medium">{report.case.bank} / {report.case.account}</h2><p className="mt-2 text-sm text-muted-foreground">{report.case.start} to {report.case.end} · stop: {report.stop_reason} · {report.model.requests} model requests</p></div></div><p className="mt-5 text-sm leading-6">{report.summary}</p><div className="mt-5 grid gap-4 md:grid-cols-2"><div><h3 className="text-sm font-medium">Hypotheses</h3><ul className="mt-2 space-y-2 text-sm text-muted-foreground">{report.hypotheses.map((h) => <li key={h.hypothesis}>{h.hypothesis}: {h.status.replaceAll("_", " ")}</li>)}</ul></div><div><h3 className="text-sm font-medium">Actual tool events</h3><ul className="mt-2 space-y-2 text-sm text-muted-foreground">{report.events.map((e, i) => <li key={`${e.action}-${i}`}>{e.action}: {e.status}{e.error ? ` (${e.error})` : ""}</li>)}</ul></div></div><div className="mt-5"><h3 className="text-sm font-medium">Verified transaction references</h3>{report.findings.length ? <ul className="mt-2 space-y-2 break-all text-sm text-muted-foreground">{report.findings.map((f) => <li key={f.tool}>{f.tool}: {f.verified_transaction_ids.length ? f.verified_transaction_ids.map((id) => <button key={id} className="ml-2 text-primary underline" onClick={() => void showTransaction(id)}>{id}</button>) : "No transaction references"}</li>)}</ul> : <p className="mt-2 text-sm text-muted-foreground">No verified findings returned.</p>}</div><details className="mt-5 text-sm"><summary className="cursor-pointer font-medium">Inspect full tool evidence ({report.evidence.length})</summary><pre className="mt-3 max-h-96 overflow-auto rounded-xl bg-black/25 p-4 text-xs">{JSON.stringify(report.evidence, null, 2)}</pre></details><p className="mt-5 text-xs leading-5 text-muted-foreground">{report.limitations.join(" ")} {report.validation_errors.length > 0 ? `${report.validation_errors.length} evidence validation errors.` : ""}</p></section>}
    {graphError && <p role="status">{graphError}</p>}
    {graph && <CaseGraph snapshot={graph} selected={transactionId} onSelect={selectTransaction} />}
  </div>;
}
