"use client";

import dynamic from "next/dynamic";
import { useState } from "react";
import { Button } from "@/components/ui/button";

const TransactionGraph = dynamic(() => import("@/components/transaction-graph"), {
  ssr: false,
  loading: () => <p className="p-6 text-sm text-muted-foreground" role="status">Loading graph renderer…</p>,
});

export function GraphPreview() {
  const [enabled, setEnabled] = useState(false);
  return (
    <section className="content-panel overflow-hidden" aria-labelledby="graph-preview-title">
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-white/10 p-6">
        <div><h2 id="graph-preview-title" className="font-medium">Graph component preview</h2><p className="mt-2 text-xs text-muted-foreground">Synthetic data only · checks the renderer, not AML detection.</p></div>
        <Button variant="outline" onClick={() => setEnabled(!enabled)}>{enabled ? "Unload preview" : "Load synthetic preview"}</Button>
      </div>
      {enabled ? <TransactionGraph /> : <p className="p-8 text-sm text-muted-foreground">The WebGL renderer loads only when requested. No backend graph has been fetched.</p>}
    </section>
  );
}
