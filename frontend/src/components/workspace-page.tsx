import { GraphPreview } from "@/components/graph-preview";
import { LiveAlerts } from "@/components/live-alerts";
import { Models } from "@/components/models";
import type { Workspace } from "@/lib/workspaces";

export function WorkspacePage({ workspace }: { workspace: Workspace }) {
  if (workspace.slug === "" || workspace.slug === "alerts" || workspace.slug === "investigator") return <LiveAlerts />;
  if (workspace.slug === "models") return <Models />;
  return (
    <div className="space-y-7">
      <header className="pt-3 md:pt-6"><p className="mb-3 text-xs tracking-[.2em] text-primary uppercase">Investigation platform</p><h1 className="text-3xl font-semibold tracking-tight md:text-4xl">{workspace.title}</h1><p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">{workspace.description}</p></header>
      <GraphPreview />
    </div>
  );
}
