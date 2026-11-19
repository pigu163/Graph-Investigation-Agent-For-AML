"use client";

import { useEffect, useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

type Monitoring = {
  updated_at: string;
  totals: { transactions: number; alerts: number; latest_archive: string | null };
  hourly_transactions: { hour: string; transactions: number }[];
};

export function MonitoringSummary() {
  const [data, setData] = useState<Monitoring>();
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    async function refresh() {
      try {
        const response = await fetch("/api/backend/monitoring", { signal: controller.signal, cache: "no-store" });
        if (!response.ok) throw new Error();
        setData(await response.json());
        setError("");
      } catch {
        if (!controller.signal.aborted) setError("Monitoring data unavailable");
      }
    }
    void refresh();
    const timer = setInterval(refresh, 10000);
    return () => { controller.abort(); clearInterval(timer); };
  }, []);

  return <section className="content-panel p-5" aria-labelledby="monitoring-heading">
    <h2 id="monitoring-heading" className="text-lg font-medium">Monitoring overview</h2>
    {error && <p role="status" className="mt-3 text-sm text-destructive">{error}</p>}
    {!data && !error && <p className="mt-3 text-sm text-muted-foreground">Loading monitoring data…</p>}
    {data && <>
      <p className="mt-2 text-xs text-muted-foreground">Updated {data.updated_at} · refreshed at most every 30 seconds</p>
      <div className="mt-4 grid gap-4 sm:grid-cols-3">
        <p>Archived transactions <strong className="block tabular-nums">{data.totals.transactions.toLocaleString()}</strong></p>
        <p>Rule alerts <strong className="block tabular-nums">{data.totals.alerts.toLocaleString()}</strong></p>
        <p>Latest archive <strong className="block text-sm">{data.totals.latest_archive || "No transactions"}</strong></p>
      </div>
      <h3 className="mt-6 text-sm font-medium">Transactions by event hour</h3>
      {data.hourly_transactions.length ? <div className="mt-3 h-56" role="img" aria-label="Transaction counts for the latest 24 recorded event hours">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data.hourly_transactions}>
            <CartesianGrid stroke="#26334b" vertical={false} />
            <XAxis dataKey="hour" hide />
            <YAxis allowDecimals={false} width={55} />
            <Tooltip />
            <Bar dataKey="transactions" fill="#8da9ff" />
          </BarChart>
        </ResponsiveContainer>
      </div> : <p className="mt-3 text-sm text-muted-foreground">No transaction history available.</p>}
      <p className="mt-2 text-xs text-muted-foreground">Latest 24 event hours with transactions. Source event time has no timezone.</p>
    </>}
  </section>;
}
