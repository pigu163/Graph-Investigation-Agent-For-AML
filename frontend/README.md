# Frontend

The Next.js application shows persisted Flink alerts and lets an analyst investigate the selected transaction's sender or recipient, or submit a scoped on-demand investigation. Saved reports can display their transaction graph. The standalone graph preview uses synthetic data; The models page reads registered models and run metrics when the API has an MLflow tracking URI configured.

## Run

With Node 22, from `frontend/`:

```bash
npm ci
npm run dev
```

Or from the project root, run `docker compose up -d --build --wait frontend`. Open <http://localhost:3000>. The frontend runs independently and reports when the backend is unavailable. See the [streaming guide](../streaming/README.md) to start the alert pipeline and API.

The Next.js server proxies `/api/status`, `/api/alerts`, `/api/alerts/stream` and `/api/investigations` to FastAPI. `AML_API_INTERNAL_URL` defaults to `http://127.0.0.1:8000` locally and points to `http://api:8000` in Compose. An optional `AML_API_TOKEN` is forwarded server-side.

Routes are `/`, `/alerts`, and `/investigator` for alerts and investigations, `/graph` for the synthetic preview, and `/models` for the model registry. The API returns tool events and reports synchronously and persists cases. Agent progress is not streamed.

For development checks run `npm run lint`, `npm run typecheck` and `npm run build`. Opaque-surface and reduced-motion controls are available in the application; the graph preview also provides a keyboard-accessible transaction list.
