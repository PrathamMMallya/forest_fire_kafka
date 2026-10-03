# Wildfire Frontend

This is a small static frontend to interact with the existing FastAPI inference service in `inference/app.py`.

Features:
- Simple SPA (HTML + vanilla JS) that posts to `/predict` with JSON: `{ "features": { ... } }`.
- Shows probability and binary prediction.

Usage

1. Run the API (from repository root):

   ```bash
   uvicorn inference.app:app --reload --host 0.0.0.0 --port 8000
   ```

2. Serve frontend files. Easiest option (Python built-in server):

   ```bash
   # from repository root
   python -m http.server 8001 --directory frontend
   ```

3. Open http://localhost:8001 in your browser, ensure API URL is `http://localhost:8000` in the input box, click *Check Health*, fill sample values and *Predict*.

Notes

- CORS has been enabled in `inference/app.py` to allow browsers to make requests to the API from other origins.
- The feature list is taken from `src/config.py`:
  `['latitude','longitude','pr','rmax','rmin','sph','srad','tmmn','tmmx','vs','bi','fm100','fm1000','erc','etr','pet','vpd']`

Optional

- Docker (build & run):

  ```bash
  docker build -t wildfire-frontend:latest frontend
  docker run -p 8001:80 wildfire-frontend:latest
  ```

- You can replace the static frontend with a React/Vite app if needed for richer UX.
