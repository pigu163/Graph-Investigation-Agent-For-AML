"use client";

import { useEffect, useRef, useState } from "react";
import { MultiDirectedGraph } from "graphology";
import Sigma from "sigma";

import { GlassPanel } from "@/components/glass-panel";
import { Button } from "@/components/ui/button";

// Explicit opt-in fixtures for renderer configuration. No risk scores or findings.
const accounts = [
  { id: "DEMO-A", x: 0, y: 0.5 },
  { id: "DEMO-B", x: 1, y: 1 },
  { id: "DEMO-C", x: 1, y: 0 },
];
const transactions = [
  { id: "DEMO-T1", source: "DEMO-A", target: "DEMO-B", amount: "125.00", currency: "USD", time: "2026-01-01T10:00:00Z" },
  { id: "DEMO-T2", source: "DEMO-B", target: "DEMO-C", amount: "80.00", currency: "USD", time: "2026-01-01T10:30:00Z" },
];

export default function TransactionGraph() {
  const container = useRef<HTMLDivElement>(null);
  const renderer = useRef<Sigma | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!container.current) return;
    const graph = new MultiDirectedGraph();
    accounts.forEach(({ id, x, y }) => graph.addNode(id, { x, y, label: id, size: 10, color: "#b3c7ff" }));
    transactions.forEach(({ id, source, target }) => graph.addDirectedEdgeWithKey(id, source, target, { size: 3, color: "#849abf", type: "arrow" }));
    let instance: Sigma;
    try {
      instance = new Sigma(graph, container.current, { labelColor: { color: "#eef2fa" }, labelRenderedSizeThreshold: 0, enableEdgeEvents: true });
      renderer.current = instance;
    } catch {
      // Keep the account/transaction list available if WebGL cannot initialize.
      queueMicrotask(() => setError("WebGL is unavailable. Use the account and transaction lists below."));
      return;
    }
    instance.on("clickNode", ({ node }) => setSelected(node));
    instance.on("clickEdge", ({ edge }) => setSelected(edge));
    instance.on("clickStage", () => setSelected(null));
    return () => { instance.kill(); renderer.current = null; };
  }, []);

  useEffect(() => {
    const instance = renderer.current;
    if (!instance) return;
    const transaction = transactions.find(({ id }) => id === selected);
    instance.setSetting("nodeReducer", (id, attributes) => ({ ...attributes, color: id === selected || id === transaction?.source || id === transaction?.target ? "#f2c786" : "#b3c7ff" }));
    instance.setSetting("edgeReducer", (id, attributes) => ({ ...attributes, color: id === selected ? "#f2c786" : "#849abf", size: id === selected ? 5 : 3 }));
  }, [selected]);

  return (
    <div>
      <div className="relative bg-[#0a1020]">
        <div ref={container} className="h-[360px] md:h-[440px]" role="img" aria-label="Synthetic directed graph: DEMO-A sends to DEMO-B, then DEMO-B sends to DEMO-C. Select the equivalent buttons and table rows below for keyboard access." />
        <GlassPanel className="absolute top-4 left-4 flex gap-2 p-2"><Button variant="ghost" size="sm" onClick={() => renderer.current?.getCamera().setState({ x: 0.5, y: 0.5, ratio: 1, angle: 0 })}>Reset view</Button><span className="self-center px-2 text-xs text-muted-foreground">Drag to pan · scroll to zoom</span></GlassPanel>
        <p className="absolute bottom-4 left-5 rounded-lg bg-[#0a1020]/95 px-3 py-2 text-xs text-muted-foreground" role="status">{error ?? "Blue: account / transfer · amber: selection · synthetic preview"}</p>
      </div>
      <div className="space-y-5 border-t border-white/10 p-6">
        <div className="flex flex-wrap items-center gap-3" aria-label="Select synthetic account">{accounts.map(({ id }) => <Button key={id} variant="outline" size="sm" aria-pressed={selected === id} onClick={() => setSelected(id)}>{id}</Button>)}<Button variant="ghost" size="sm" onClick={() => setSelected(null)}>Clear selection</Button></div>
        <p className="text-sm text-muted-foreground" role="status">Selected: {selected ?? "none"}. All values are synthetic; no risk classification has been computed.</p>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs tabular-nums">
            <caption className="sr-only">Synthetic transaction evidence, with keyboard-accessible selection</caption>
            <thead className="text-muted-foreground"><tr>{["Transaction", "From", "To", "Amount", "Event time"].map((label) => <th key={label} scope="col" className="px-3 py-3 font-medium">{label}</th>)}</tr></thead>
            <tbody>{transactions.map((tx) => <tr key={tx.id} className="border-t border-white/10"><td className="px-3 py-3"><button onClick={() => setSelected(tx.id)} aria-pressed={selected === tx.id} className="rounded px-1 py-2 text-primary underline underline-offset-4">{tx.id}</button></td><td className="px-3 py-3">{tx.source}</td><td className="px-3 py-3">{tx.target}</td><td className="px-3 py-3 whitespace-nowrap">{tx.amount} {tx.currency}</td><td className="px-3 py-3 whitespace-nowrap">{tx.time}</td></tr>)}</tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
