export const workspaces = [
  { slug: "", label: "Monitoring", title: "AML monitoring", description: "Transaction activity, account features and processing freshness.", icon: "activity" },
  { slug: "alerts", label: "Risk alerts", title: "Suspicious transaction alerts", description: "Rule triggers, supporting evidence and investigation status.", icon: "shield" },
  { slug: "graph", label: "Graph explorer", title: "Transaction graph explorer", description: "Explore directed account relationships and transaction evidence.", icon: "network" },
  { slug: "investigator", label: "AI investigator", title: "Investigation workspace", description: "Ask a question. Follow the tools. Inspect the evidence.", icon: "sparkles" },
  { slug: "models", label: "Model registry", title: "MLflow model management", description: "Model versions, experiment metrics and deployment information.", icon: "layers" },
] as const;

export type Workspace = (typeof workspaces)[number];
