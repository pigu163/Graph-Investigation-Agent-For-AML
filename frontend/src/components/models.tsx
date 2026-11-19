"use client";

import { useEffect, useState } from "react";

type Model = { name: string; latest_versions?: { version: string; current_stage: string; run_id: string; status: string; metrics?: { key: string; value: number }[] }[] };

export function Models() {
  const [models, setModels] = useState<Model[]>();
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    fetch("/api/backend/models", { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error("MLflow tracking server unavailable or not configured");
        return response.json();
      })
      .then((data: { models: Model[] }) => setModels(data.models))
      .catch(() => { if (!controller.signal.aborted) setError("MLflow tracking server unavailable or not configured"); });
    return () => controller.abort();
  }, []);
  return <section className="content-panel p-7">
    <h1 className="text-2xl font-semibold">Registered models</h1>
    {error && <p role="status" className="mt-4 text-muted-foreground">{error}</p>}
    {!error && !models && <p className="mt-4">Loading models…</p>}
    {models?.length === 0 && <p className="mt-4 text-muted-foreground">No registered models.</p>}
    {models?.map((model) => <div key={model.name} className="mt-6 border-t border-white/10 pt-4">
      <h2 className="font-medium">{model.name}</h2>
      {model.latest_versions?.map((version) => <div key={version.version} className="mt-2 text-sm text-muted-foreground"><p>Version {version.version} · {version.status} · {version.current_stage} · run {version.run_id}</p>{version.metrics?.map((metric) => <p key={metric.key}>{metric.key}: {metric.value.toFixed(4)}</p>)}</div>)}
    </div>)}
  </section>;
}
