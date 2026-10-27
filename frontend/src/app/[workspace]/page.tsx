import { notFound } from "next/navigation";
import { WorkspacePage } from "@/components/workspace-page";
import { workspaces } from "@/lib/workspaces";

export const dynamicParams = false;

export function generateStaticParams() {
  return workspaces.filter(({ slug }) => slug).map(({ slug }) => ({ workspace: slug }));
}

export default async function Page({ params }: { params: Promise<{ workspace: string }> }) {
  const { workspace: slug } = await params;
  const workspace = workspaces.find((entry) => entry.slug === slug);
  if (!workspace) notFound();
  return <WorkspacePage workspace={workspace} />;
}
