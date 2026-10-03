Quick start:

1. Install dependencies: `npm install`
2. Dev server: `npm run dev` (open http://localhost:5173)
3. Build: `npm run build` and then `docker build -t wildfire-frontend:latest .` to build container

Notes:
- If you run the frontend inside Docker, you don't need to set the API to a host name — the nginx config proxies `/api/*` to the inference service. Use the frontend API URL `/api` or set `VITE_API_URL` if you need a different path.
- The inference API exposes monitoring endpoints at `/monitoring/*` and a drift report at `/monitoring/drift/report` (accessible via `/api/monitoring/drift/report` when using the built frontend/nginx proxy).