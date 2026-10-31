# Wildfire Risk — Full Setup from Scratch on Windows

Complete guide: from an empty Windows machine to the running system — streaming
pipeline (Kafka + Elasticsearch + 6 services + 3D dashboard), React frontend,
FastAPI backend, and the optional MLOps backend (K8s, drift, retraining).

Commands are **PowerShell** (run it as your normal user; no admin needed except
for installing software). Git Bash / WSL alternatives are noted where relevant.

---

## 0. Prerequisites (install once)

| Software | Why | Install |
|---|---|---|
| **Docker Desktop for Windows** | runs everything | https://www.docker.com/products/docker-desktop — during install keep **"Use WSL 2 instead of Hyper-V"** checked |
| **Git for Windows** | clone the repo | https://git-scm.com/download/win (includes Git Bash) |
| **Python 3.10+** (optional) | only needed for host-side scripts (evaluation, drift runner without Docker) | https://www.python.org/downloads — tick **"Add python.exe to PATH"** |

### 0.1 Give Docker enough memory (important)

The pipeline runs ~10 containers (Kafka, Elasticsearch, 6 Python services,
frontend, backend). Elasticsearch has already been OOM-killed on tight VMs.
WSL2's default memory can be too small on 8–16 GB machines.

Create/edit `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=6GB
processors=4
```

Then restart Docker Desktop (whale icon → Restart). Skip this if your machine
has 8 GB RAM or less total — instead close other apps while the pipeline runs.

### 0.2 Git line endings (avoids broken shell scripts)

```powershell
git config --global core.autocrlf true
```

This is Git's recommended Windows setting. The compose-based path in this
guide never mounts shell scripts into containers, so CRLF can't break it — but
if you later run `scripts/*.sh` through Git Bash or WSL and see
`bad interpreter: /usr/bin/env bash\r`, run `dos2unix` on that file or
`git config core.autocrlf input` + re-clone.

### 0.3 Clone

```powershell
cd $env:USERPROFILE
git clone <your-repo-url> Wildfire_risk
cd Wildfire_risk
```

Avoid paths with spaces if you can — Docker volume mounts are happier without them.

---

## 1. Build the images (first time only, ~5–10 min)

```powershell
docker compose -f deploy/docker-compose.yml --profile app build   # streaming services
docker compose build                                               # frontend + backend
```

If either fails on network timeouts, retry — Windows Docker networking behind
VPNs/proxies sometimes needs Docker Desktop → Settings → Resources → Proxies.

## 2. Start infrastructure (Kafka + Elasticsearch)

```powershell
docker compose -f deploy/docker-compose.yml up -d kafka elasticsearch

# wait until both report healthy (~30-60s)
do {
  $k = docker inspect -f '{{.State.Health.Status}}' wildfire-kafka
  $e = docker inspect -f '{{.State.Health.Status}}' wildfire-es
  Write-Host "kafka=$k es=$e"
  Start-Sleep 5
} until ($k -eq 'healthy' -and $e -eq 'healthy')
```

## 3. Create the Kafka topics

Must run **inside a container** — the image's `create_topics` knows all 6 topics
including `wildfire.weather`. The older script in this checkout only creates 5,
and the producer will crash-loop with `UNKNOWN_TOPIC` without the 6th.

```powershell
docker compose -f deploy/docker-compose.yml --profile app up -d producer
docker exec wildfire-streaming-producer-1 python -m streaming.create_topics
```

Expected topics: `wildfire.raw` (6p) · `wildfire.weather` (6p, compacted) ·
`wildfire.features` (6p) · `wildfire.predictions` (3p) · `wildfire.alerts` (3p) ·
`wildfire.dlq` (1p).

## 4. Start the streaming pipeline

```powershell
docker compose -f deploy/docker-compose.yml --profile app up -d
```

This starts: producer (CSV replay, 50 events/sec synthetic clock) → streams
(validate/dedup/windows/join) → detector (ML scoring + alert tiers) → indexer
(→ Elasticsearch) + notifier (dispatch) + dashboard (:9991).

If the producer restarted once because it raced topic creation, its restart
policy recovers it — check with `docker ps`.

## 5. Start frontend + backend

```powershell
docker compose up -d
```

## 6. Verify everything

PowerShell note: plain `curl` is aliased to `Invoke-WebRequest` in Windows
PowerShell 5.1 — either use **`curl.exe`** (real curl, ships with Windows 10+)
or `Invoke-RestMethod` for JSON:

```powershell
curl.exe -s -o NUL -w "frontend  :8001 %{http_code}`n" http://localhost:8001/
curl.exe -s -o NUL -w "backend   :8000 %{http_code}`n" http://localhost:8000/
curl.exe -s -o NUL -w "dashboard :9991 %{http_code}`n" http://localhost:9991/

# pipeline numbers (run twice — they should grow)
(Invoke-RestMethod http://localhost:9991/api/stats) | Select-Object predictions_total, alerts_total, es_up, es_docs

# alerts searchable in Elasticsearch
(Invoke-RestMethod "http://localhost:9200/wildfire-alerts/_count").count

# browser-path integration: frontend -> nginx -> backend
Invoke-RestMethod http://localhost:8001/api/model        # @{model_name=best_RandomForest}
```

## 7. Your URLs

| What | URL |
|---|---|
| **Frontend (React)** | http://localhost:8001 |
| **Backend API (FastAPI)** | http://localhost:8000 |
| Backend Swagger docs | http://localhost:8000/docs |
| **Live 3D streaming dashboard** | http://localhost:9991 |
| Pipeline stats / metrics | http://localhost:9991/api/stats · http://localhost:9991/metrics |
| Elasticsearch | http://localhost:9200 (no auth; CORS open — index `wildfire-alerts`) |

Optional UIs:

```powershell
docker compose -f deploy/docker-compose.yml --profile ui up -d      # Kafka UI → http://localhost:8085
docker compose -f deploy/docker-compose.yml --profile kibana up -d  # Kibana   → http://localhost:5601
```

## 8. Logs

```powershell
docker compose -f deploy/docker-compose.yml --profile app logs -f   # whole streaming pipeline
docker compose logs -f                                               # frontend + backend
docker logs -f wildfire-streaming-producer-1                         # individual services
docker logs -f wildfire-kafka
docker logs -f wildfire-es
# add --tail 100 or --since 10m; drop -f to print once
```

## 9. Stop / reset

```powershell
docker compose down                                                   # frontend + backend
docker compose -f deploy/docker-compose.yml --profile app down        # streaming pipeline (data kept)
docker compose -f deploy/docker-compose.yml --profile app down -v     # FULL reset (wipes topics + ES index)
```

After `down -v`, start again from §2 (the volumes are recreated empty; Kafka
re-formats cleanly — the advertised-listeners fix in the compose file makes
this work — then redo §3 to recreate topics).

---

## 10. Optional: MLOps backend on Windows (drift → retrain loop)

Part B of the system: batch inference on Kubernetes, drift detection, retraining.

```powershell
# Docker Desktop → Settings → Kubernetes → Enable Kubernetes (do NOT use minikube/kind)
kubectl config current-context                 # expect: docker-desktop

python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements.inference.txt
# if activation is blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

python train.py                                  # trains + logs to MLflow (mlflow ui)
docker build -t wildfire-inference:latest .
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl port-forward svc/wildfire-inference 8000:8000 -n wildfire   # keep this window open

python streaming/producer.py                     # drives live traffic (new window, venv active)
python -m monitoring.drift_runner                # writes monitoring/drift/status.json
python prefect/flows/retrain_flow.py             # or: python prefect/flows/retrain_flow.py --force
```

**Known Windows caveat** (from the project README): Prefect's ephemeral server
can hang on Windows. If `retrain_flow.py` stalls at startup, run a Prefect
server explicitly in a separate window (`prefect server start`) and set
`$env:PREFECT_API_URL="http://127.0.0.1:4200/api"` before running the flow.

---

## 11. Troubleshooting (each one actually happened — fixes are already in the files)

| Symptom | Cause | Fix |
|---|---|---|
| Kafka container exits: `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0` | `KAFKA_ADVERTISED_LISTENERS` missing the CONTROLLER listener | Fixed in `deploy/docker-compose.yml` (`CONTROLLER://kafka:29093`). Don't remove it. |
| Producer crash-loops `_UNKNOWN_TOPIC` | `wildfire.weather` topic missing | Redo §3 — run `create_topics` **inside the container**, never the host script |
| ES `_count` stays 0 while alerts flow | Alert polygons have unclosed rings; ES `geo_shape` rejects each doc silently (bulk returns HTTP 200 anyway) | Permanent fix must go into the **image source** (not this checkout): close the ring in `build_bulk_body` (`streaming/indexer.py`). A hot-patch exists in the running container; rebuilding from this checkout loses it. |
| ES container exits 137 (OOMKilled) | WSL2 VM out of memory | Heap is already capped at 512m + auto-restart in compose. Still happening? Raise `memory=` in `.wslconfig` (§0.1) or close other apps |
| "Network error, cannot access your cluster" (Elasticvue/browser tool) | ES blocks browser cross-origin requests | Fixed: CORS enabled in compose. Note the value must be `"'*'"` — bare `*` is invalid YAML and crash-loops ES |
| Frontend loads, API calls fail | `VITE_API_URL` was baked as `http://inference:8000` (unreachable from the browser) | Fixed in root `docker-compose.yml`: `/api` via nginx proxy |
| Frontend 500 "redirection cycle" | custom nginx.conf dropped `root`/`index` | Fixed in `frontend/react-app/nginx.conf` |
| `Invoke-... : A parameter cannot be found 's'` | `curl` is a PowerShell alias, not real curl | Use `curl.exe` or `Invoke-RestMethod` (§6) |
| `.\venv\Scripts\Activate.ps1 cannot be loaded` | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Prefect hangs on startup | ephemeral server issue on Windows | `prefect server start` in another window + `$env:PREFECT_API_URL` (§10) |
| `bad interpreter: /usr/bin/env bash\r` (Git Bash/WSL) | CRLF line endings | §0.2 |
| Docker build fails pulling images | VPN/proxy interference | Docker Desktop → Settings → Resources → Proxies; retry |

Recover alerts after an ES wipe (alerts live in Kafka for 90 days; re-indexing
is an idempotent upsert):

```powershell
docker stop wildfire-streaming-indexer-1
Start-Sleep 30   # let the consumer group go Empty
docker exec wildfire-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 --group mosdac-wildfire-indexer `
  --topic wildfire.alerts --reset-offsets --to-earliest --execute
docker start wildfire-streaming-indexer-1
```

(The `` ` `` backtick is PowerShell's line-continuation. On CMD, use `^` instead,
or run it as one long line.)

## Notes

- The deployed streaming image runs the **ISRO MOSDAC / Open-Meteo** variant,
  newer than this checkout's `streaming/*.py` (older gridMET variant). Don't
  `build` the streaming services from this checkout unless you mean to downgrade.
- Rebuilding **frontend/backend** from this checkout is safe — their fixes are
  in the files here.
- Kafka + ES data persist in the `kafka-data` / `es-data` Docker volumes.
- ES shows status `yellow` on a single node (one unassigned replica) — cosmetic.
- Deep-dive docs: `ARCHITECTURE.md` and `architecture.html`. macOS/Linux setup:
  `SETUP.md`.
