import { ArrowUpRight, Database, Network, ScanLine, Search } from "lucide-react";
import Link from "next/link";

import { GlassPanel } from "@/components/glass-panel";
import { Button } from "@/components/ui/button";
import { GraphPreview } from "@/components/graph-preview";
import { LiveAlerts } from "@/components/live-alerts";
import type { Workspace } from "@/lib/workspaces";

export function WorkspacePage({ workspace }: { workspace: Workspace }) {
  if (workspace.slug === "" || workspace.slug === "alerts") return <LiveAlerts />;
  return (
    <div className="space-y-7">
      <header className="pt-3 md:pt-6"><p className="mb-3 text-xs tracking-[.2em] text-primary uppercase">Investigation platform</p><h1 className="text-3xl font-semibold tracking-tight md:text-4xl">{workspace.title}</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">{workspace.description}</p></header>
      <section className="content-panel p-7 md:p-10" aria-labelledby="connection-title">
        <span className="mb-5 inline-flex items-center gap-2 rounded-full border border-white/15 bg-white/5 px-3 py-1 text-xs text-muted-foreground"><span className="size-1.5 rounded-full bg-slate-400" aria-hidden="true" />Setup preview · no live data</span>
        <h2 id="connection-title" className="text-xl font-medium tracking-tight">Your workspace is ready to connect.</h2>
        <p className="mt-3 max-w-2xl text-sm leading-7 text-muted-foreground">{workspace.slug === "models" ? "An MLflow backend integration is not implemented here." : workspace.slug === "investigator" ? "Free-form investigation requests are not available yet. Start an investigation from an alert by selecting its sender or recipient." : "The graph explorer is not connected to a backend graph API."}</p>
        <div className="mt-8 grid gap-5 md:grid-cols-3">
          {[{ icon: Database, title: "Traceable data", body: "Transactions and features from verified API responses." }, { icon: Network, title: "Graph investigation", body: "Sigma.js + Graphology, with evidence-linked subgraphs." }, { icon: ScanLine, title: "Evidence first", body: "Visible provenance, uncertainty and integration status." }].map(({ icon: Icon, title, body }) => <div key={title} className="border-t border-white/10 pt-5"><Icon className="mb-3 size-5 text-primary" aria-hidden="true" /><h3 className="text-sm font-medium">{title}</h3><p className="mt-2 text-xs leading-6 text-muted-foreground">{body}</p></div>)}
        </div>
        {workspace.slug !== "graph" && <Button asChild variant="outline" className="mt-8"><Link href="/graph">Open graph component preview<ArrowUpRight /></Link></Button>}
        {workspace.slug === "investigator" && <Button asChild variant="outline" className="mt-8 ml-2"><Link href="/alerts">Open live alerts<ArrowUpRight /></Link></Button>}
      </section>
      {workspace.slug === "graph" && <GraphPreview />}
      {workspace.slug === "investigator" && <GlassPanel className="p-4 md:p-5"><label htmlFor="investigation-prompt" className="mb-3 block text-xs text-muted-foreground">Free-form Agent prompt · not available yet</label><div className="flex items-center gap-3"><Search className="size-5 shrink-0 text-primary" aria-hidden="true" /><input id="investigation-prompt" disabled placeholder="Investigate account A001 over the last 7 days…" className="min-w-0 flex-1 bg-transparent py-2 text-sm placeholder:text-muted-foreground disabled:cursor-not-allowed" /><Button disabled>Investigate</Button></div></GlassPanel>}
    </div>
  );
}
