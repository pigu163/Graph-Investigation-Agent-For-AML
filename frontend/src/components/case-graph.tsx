"use client";

import { useEffect, useRef, useState } from "react";
import { MultiDirectedGraph } from "graphology";
import Sigma from "sigma";

type Row = { transaction_id: string; source_bank: string; source_account: string; destination_bank: string; destination_account: string; amount_paid: string; payment_currency: string; amount_received: string; receiving_currency: string; event_time: string };
type Snapshot = { result: { nodes: {bank_id: string; account_id: string}[]; transactions: Row[] }; scope: {truncated: boolean; truncation_reason?: string}; limitations: string[] };
const accountKey = (bank: string, account: string) => JSON.stringify([bank, account]);

export default function CaseGraph({ snapshot, selected, onSelect }: { snapshot: Snapshot; selected: string | null; onSelect: (id: string) => void }) {
  const container = useRef<HTMLDivElement>(null);
  const renderer = useRef<Sigma | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!container.current || !snapshot.result.nodes.length) return;
    const graph = new MultiDirectedGraph();
    snapshot.result.nodes.forEach((node, index) => {
      const angle = 2 * Math.PI * index / snapshot.result.nodes.length;
      graph.addNode(accountKey(node.bank_id, node.account_id), { x: Math.cos(angle), y: Math.sin(angle), label: `${node.bank_id} / ${node.account_id}`, size: 8, color: "#b3c7ff" });
    });
    snapshot.result.transactions.forEach((row) => graph.addDirectedEdgeWithKey(row.transaction_id, accountKey(row.source_bank, row.source_account), accountKey(row.destination_bank, row.destination_account), { size: 3, color: "#849abf", type: "arrow" }));
    try {
      const instance = new Sigma(graph, container.current, { enableEdgeEvents: true, labelColor: { color: "#eef2fa" } });
      renderer.current = instance;
      instance.on("clickEdge", ({ edge }) => onSelect(edge));
      return () => { instance.kill(); renderer.current = null; };
    } catch {
      queueMicrotask(() => setError("Graph rendering unavailable. Use the transaction list below."));
    }
  }, [snapshot, onSelect]);
  useEffect(() => {
    renderer.current?.setSetting("edgeReducer", (id, attrs) => ({...attrs, color: id === selected ? "#f2c786" : "#849abf", size: id === selected ? 5 : 3}));
  }, [selected, snapshot]);
  const row = snapshot.result.transactions.find((item) => item.transaction_id === selected);
  return <section className="content-panel overflow-hidden p-5">
    <h2 className="text-lg font-medium">Investigation graph</h2>
    <p className="mt-2 text-sm text-muted-foreground">{snapshot.scope.truncated ? `Graph truncated: ${snapshot.scope.truncation_reason}` : "Bounded graph returned by the investigation."} {snapshot.limitations.join(" ")}</p>
    <div ref={container} className="mt-4 h-[360px] bg-[#0a1020]" role="img" aria-label="Directed transaction graph. Use the transaction list to select an edge." />
    {error && <p role="status">{error}</p>}
    <div className="mt-4 max-h-48 overflow-auto"><ul>{snapshot.result.transactions.map((item) => <li key={item.transaction_id}><button className="text-sm text-primary underline" onClick={() => onSelect(item.transaction_id)} aria-pressed={selected === item.transaction_id}>{item.transaction_id}</button></li>)}</ul></div>
    {row && <div className="mt-4 break-all text-sm"><p>{row.transaction_id}</p><p>{row.source_bank} / {row.source_account} → {row.destination_bank} / {row.destination_account}</p><p>Paid {row.amount_paid} {row.payment_currency} · received {row.amount_received} {row.receiving_currency}</p><p>{row.event_time} · source time</p></div>}
  </section>;
}
