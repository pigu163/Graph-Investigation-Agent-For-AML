import { WorkspacePage } from "@/components/workspace-page";
import { workspaces } from "@/lib/workspaces";

export default function Home() {
  return <WorkspacePage workspace={workspaces[0]} />;
}
