# Frontend

The Next.js application shows persisted Flink alerts and lets an analyst investigate the selected transaction's sender or recipient, or submit a scoped on-demand investigation. Saved reports can display their transaction graph. The standalone graph preview uses synthetic data. The Models page reads registered models and run metrics when the API has an MLflow tracking URI configured.

## Run

With Node 22, from `frontend/`:

```bash
npm ci
npm run dev
```

Or from the project root, run `docker compose up -d --build --wait frontend`. Open <http://localhost:3000>. The frontend runs independently and reports when the backend is unavailable. See the [streaming guide](../streaming/README.md) to start the alert pipeline and API.

The Next.js server proxies the following FastAPI capabilities. `AML_API_INTERNAL_URL` defaults to `http://127.0.0.1:8000` locally and points to `http://api:8000` in Compose. An optional `AML_API_TOKEN` is forwarded server-side.

| Workflow | FastAPI endpoints |
| --- | --- |
| Monitoring and alerts | `/api/status`, `/api/monitoring`, `/api/alerts`, `/api/alerts/stream`, `/api/transactions` |
| Run and review investigations | `/api/investigations`, `/api/investigations/on-demand`, `/api/investigations/{id}`, `/api/investigations/{id}/graph`, `/api/investigations/{id}/disposition` |
| Queue and retry an investigation | `/api/investigation-queue`, `/api/investigation-queue/retry` |
| MLflow registry | `/api/models` |

Open `/` for monitoring or `/alerts` to select a transaction and investigate its sender or recipient. Use `/investigator` for an on-demand request or to review a saved case. `/graph` is a synthetic preview. `/models` displays registered models and metrics through FastAPI. Alerts update over SSE. Direct investigations return the tool events and report synchronously. Queued investigations use the optional dispatcher described in the [project README](../README.md).

For development checks run `npm run lint`, `npm run typecheck` and `npm run build`. Opaque-surface and reduced-motion controls are available in the application; the graph preview also provides a keyboard-accessible transaction list.
