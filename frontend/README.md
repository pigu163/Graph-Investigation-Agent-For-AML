# Graph Investigation frontend foundation

Next.js App Router + React + TypeScript, Tailwind CSS v4, shadcn/ui-compatible owned components, Framer Motion, Recharts, Sigma.js and Graphology. This milestone configures the frontend; it is **not the completed monitoring or Agent application**.

## Run locally

Use Node 22 (see `.nvmrc`) and npm. From this directory:

```bash
npm ci
npm run dev
```

Open <http://localhost:3000>. `Ctrl+C` stops the server. Docker is not needed or started by these commands.

The current machine has an older system Node. A project-local Node 22 is installed in the ignored `.runtime/` directory. In a fresh terminal, use:

```bash
cd frontend
export PATH="$PWD/.runtime/node_modules/.bin:$PATH"
node --version
npm run dev
```

If that local runtime is absent and you cannot use an existing Node version manager, bootstrap it with:

```bash
npm install --prefix .runtime --no-save --package-lock=false node@22
export PATH="$PWD/.runtime/node_modules/.bin:$PATH"
npm ci
```

This leaves the system Node and Python/uv environment unchanged. The npm lockfile pins frontend dependencies.

## Run with Docker Compose

From the repository root, start just the frontend (no local Node installation required):

```bash
docker compose up -d --build --wait frontend
```

Open <http://localhost:3000>. The frontend has its own build context and runs a production Next.js standalone server as the non-root `node` user. Build-time dependencies stay in the build stage; host `node_modules`, `.next`, local runtimes and `.env` files are excluded. The image does not currently contain a `public/` directory because the app has no public assets; add a Dockerfile copy for it when introducing those assets.

Start all existing long-running services, including the frontend:

```bash
docker compose up -d --build --wait
docker compose ps
```

`producer` and `flink-submit` remain on-demand `etl` profile services in the same Compose file. They need an explicit replay/submission command rather than running on every startup. Starting the Flink cluster does not submit a feature job.

Stop and remove this project's containers and network, preserving stored data:

```bash
docker compose down
```

Frontend source changes require rebuilding the image. For hot reload while editing, use the local `npm run dev` workflow above. The frontend intentionally has no `depends_on` backend dependency: its disconnected screens and synthetic graph preview work with the streaming services stopped. Dockerization does not implement a FastAPI or MLflow integration.

## Configured surfaces and routes

- `/`: monitoring placeholder.
- `/alerts`: alert placeholder.
- `/graph`: graph placeholder with an **opt-in, visibly labeled synthetic renderer preview**, node/edge selection, directed edges, pan/zoom/reset and an accessible transaction list.
- `/investigator`: workspace placeholder with a disabled Spotlight-style prompt, pending a real Agent API.
- `/models`: MLflow integration placeholder, not a working model registry.

The shell contains reusable `GlassPanel` and shadcn/ui-style `Button` components. `globals.css` defines dark navy/graphite tokens, selective blur, ambient gradients, opaque content surfaces, focus styles, and reduced-motion/transparency fallbacks. The header's opaque-surface and reduced-motion controls last across client-side navigation; OS preferences are also respected. Controls reset on a full reload.

`components.json` configures shadcn/ui's Tailwind v4 CSS path and `@/` aliases. Add actual UI primitives when needed, for example:

```bash
npx shadcn@latest add dialog table input
```

Recharts is installed for future real monitoring charts; no fake metrics are displayed. Sigma loads client-side and only after selecting the synthetic preview. Its renderer is destroyed on unmount; a transaction list remains available when WebGL is unavailable. No force-layout worker or large graph is started in the background.

## Backend boundary

This checkout currently contains streaming/persistence services, not a FastAPI application or MLflow service. No endpoints are guessed and no API requests or credentials are embedded in the frontend. The shell truthfully displays **Backend not connected**. Compose includes the frontend alongside the existing Python, Kafka, Flink and PostgreSQL services.

Before integration, agree API contracts against implemented FastAPI routes and their request/response schemas. Include source/dataset, bank/account identifiers, transaction IDs, time range, amount/currency, pagination/graph bounds, evidence provenance and availability/freshness metadata. Then wire REST reads, supported WebSocket monitoring events and SSE/verified Agent execution events. Keep MLflow and database credentials server-side. Mark draft contracts as proposed until the backend implements them.

The product requirements in local `AGENTS.md` cover all five intended screens, Agent/graph/evidence linking, accessibility and demo labeling. Full screens, graph neighborhood APIs, alert dispatch, Agent execution, MLflow metrics, authentication and backend contracts remain future work.

## Verification

```bash
npm run lint
npm run build
npm run typecheck
```

For browser checks, visit all five routes, switch opaque surfaces and reduced motion, inspect desktop/tablet navigation, and load/unload the synthetic graph. Select accounts and transactions with both pointer and keyboard. Ensure every preview is labeled synthetic and there are no invented live metrics or operational Agent controls.

Reference: [Next.js installation](https://nextjs.org/docs/app/getting-started/installation), [shadcn/ui manual setup](https://ui.shadcn.com/docs/installation/manual), [Sigma.js quickstart](https://www.sigmajs.org/get-started/quickstart/), [Motion accessibility configuration](https://motion.dev/docs/react-motion-config).
