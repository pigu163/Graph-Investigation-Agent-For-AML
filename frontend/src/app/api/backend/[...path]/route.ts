import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  const endpoint = path.join("/");
  const allowed = request.method === "GET"
    ? ["status", "alerts", "alerts/stream"].includes(endpoint)
    : request.method === "POST" && endpoint === "investigations";
  if (!allowed) return new Response("Not found", { status: 404 });
  const base = (process.env.AML_API_INTERNAL_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
  const headers = new Headers();
  if (process.env.AML_API_TOKEN) headers.set("Authorization", `Bearer ${process.env.AML_API_TOKEN}`);
  if (request.method === "POST") headers.set("Content-Type", "application/json");
  try {
    const response = await fetch(`${base}/api/${endpoint}`, {
      method: request.method, headers, cache: "no-store", signal: request.signal,
      body: request.method === "POST" ? await request.text() : undefined,
    });
    return new Response(response.body, { status: response.status, headers: {
      "Content-Type": response.headers.get("Content-Type") || "application/json",
      "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no",
    } });
  } catch {
    return Response.json({ detail: "Backend unavailable" }, { status: 503 });
  }
}

export const GET = proxy;
export const POST = proxy;
