# Wildfire Risk — Setup & Run Guide

Two stacks, both Docker Compose:

1. **Streaming pipeline** (`deploy/docker-compose.yml`) — Kafka + Elasticsearch +
   producer → streams → detector → indexer/notifier → 3D dashboard
2. **Frontend + inference backend** (root `docker-compose.yml`) — React UI + FastAPI

## URLs

| What | URL |
|---|---|
| **Frontend (React)** | http://localhost:8001 |
| **Backend API (FastAPI)** | http://localhost:8000 |
| Backend Swagger docs | http://localhost:8000/docs |
| **Live 3D streaming dashboard** | http://localhost:9991 |
| Pipeline stats (JSON) | http://localhost:9991/api/stats |
| Prometheus metrics | http://localhost:9991/metrics |
| Elasticsearch | http://localhost:9200 (no auth; CORS open for browser tools like Elasticvue — index: `wildfire-alerts`) |
| Kafka broker | localhost:9092 |

---

## 1. Prerequisites

- Docker Desktop or Rancher Desktop running
- Images built (first time only, ~5–10 min):

```bash
docker compose -f deploy/docker-compose.yml --profile app build   # streaming services
docker compose build                                               # frontend + backend
```

## 2. Start everything

```bash
# --- infra: Kafka + Elasticsearch -----------------------------------------
docker compose -f deploy/docker-compose.yml up -d kafka elasticsearch

# wait until both are healthy (~30-60s)
until [ "$(docker inspect -f '{{.State.Health.Status}}' wildfire-kafka)" = "healthy" ] && \
      [ "$(docker inspect -f '{{.State.Health.Status}}' wildfire-es)" = "healthy" ]; do sleep 5; done

# --- topics (run INSIDE a container: creates all 6 incl. wildfire.weather) --
docker compose -f deploy/docker-compose.yml --profile app up -d producer
docker exec wildfire-streaming-producer-1 python -m streaming.create_topics

# --- streaming pipeline ----------------------------------------------------
docker compose -f deploy/docker-compose.yml --profile app up -d

# --- frontend + backend ----------------------------------------------------
docker compose up -d
```

The producer may restart once if it raced the topic creation — `restart:
unless-stopped` recovers it automatically. Topics persist in the `kafka-data`
volume after the first recreate; ES data persists in `es-data`.

## 3. Verify

```bash
curl -s -o /dev/null -w "frontend  :8001 %{http_code}\n" http://localhost:8001/
curl -s -o /dev/null -w "backend   :8000 %{http_code}\n" http://localhost:8000/
curl -s -o /dev/null -w "dashboard :9991 %{http_code}\n" http://localhost:9991/

# predictions/alerts climbing (run twice; numbers should grow)
curl -s http://localhost:9991/api/stats | python3 -m json.tool | head -20

# alerts searchable in ES (indexer flushes in batches of 50)
curl -s "http://localhost:9200/wildfire-alerts/_count"

# browser-path integration: frontend -> nginx -> backend
curl -s http://localhost:8001/api/model        # {"model_name":"best_RandomForest"}
```

## 4. Logs

```bash
docker compose -f deploy/docker-compose.yml --profile app logs -f   # whole streaming pipeline
docker compose logs -f                                               # frontend + backend
docker logs -f wildfire-streaming-producer-1                         # individual services…
docker logs -f wildfire-kafka
docker logs -f wildfire-es
# modifiers: --tail 100, --since 10m, drop -f to print once
```

## 5. Optional UIs

```bash
docker compose -f deploy/docker-compose.yml --profile ui up -d      # Kafka UI → http://localhost:8085
docker compose -f deploy/docker-compose.yml --profile kibana up -d  # Kibana   → http://localhost:5601
```

## 6. Stop / reset

```bash
docker compose down                                                   # frontend + backend
docker compose -f deploy/docker-compose.yml --profile app down        # streaming pipeline (data kept in volumes)
docker compose -f deploy/docker-compose.yml --profile app down -v     # FULL reset: wipes topics + ES index too
docker start wildfire-es                                              # if ES alone died (OOM) — restart policies make this rare now
```

---

## Troubleshooting (each of these actually happened — fixes are in the files, don't revert them)

| Symptom | Cause | Fix |
|---|---|---|
| Kafka exits: `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0` | `KAFKA_ADVERTISED_LISTENERS` missing the CONTROLLER listener; Kafka falls back to the 0.0.0.0 bind address and refuses to format storage | Fixed in `deploy/docker-compose.yml` (`CONTROLLER://kafka:29093` in advertised listeners). Don't remove it. |
| Producer crash-loops `KafkaError _UNKNOWN_TOPIC` | `wildfire.weather` topic missing (6th topic, only the container's `create_topics` knows it) | Re-run topic creation from §2 **inside the container**, never the host script |
| ES `wildfire-alerts` count stays 0 while alerts flow | Alert `geometry` polygons have unclosed rings; ES `geo_shape` rejects each doc and bulk still returns HTTP 200 | Fix must live in the **image source** (not this checkout): close the ring in `build_bulk_body` (`streaming/indexer.py`) or at the producer's `boundary` source. A hot-patch is applied in the running container — rebuilding from this checkout loses it. |
| ES exits 137 (OOMKilled) | VM memory pressure; ES was the largest process with 1g heap | Fixed: heap now 512m + `restart: unless-stopped` on kafka/es in compose. If it still happens: `docker start wildfire-es` — the indexer buffers alerts and flushes on reconnect |
| Browser tool (Elasticvue): "Network error, cannot access your cluster" | ES blocks browser cross-origin requests by default | Fixed: CORS enabled in compose (`http.cors.*`; note `allow-origin` must be `"'*'"` — a bare `*` is an invalid YAML alias and crash-loops ES) |
| Frontend loads but every API call fails | `VITE_API_URL` was baked as `http://inference:8000`, unreachable from the browser | Fixed in root `docker-compose.yml`: `VITE_API_URL: "/api"` → nginx proxies to the backend |
| Frontend returns 500 "redirection cycle" | Custom `nginx.conf` replaced the stock one and dropped `root`/`index` | Fixed in `frontend/react-app/nginx.conf` |

Recover alerts after an ES/index wipe (alerts live in Kafka for 90 days; indexing
is an idempotent upsert keyed by `alert_id`):

```bash
docker stop wildfire-streaming-indexer-1
# wait ~30s for the consumer group to go Empty, then:
docker exec wildfire-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group mosdac-wildfire-indexer \
  --topic wildfire.alerts --reset-offsets --to-earliest --execute
docker start wildfire-streaming-indexer-1
```

## Notes

- The deployed streaming image runs the **ISRO MOSDAC / Open-Meteo** variant of
  `streaming/`, which is newer than the `streaming/*.py` in this checkout
  (older gridMET variant). Don't `build` the streaming services from this
  checkout unless you intend to downgrade.
- Rebuilding frontend/backend from this checkout is safe — the fixes are in the
  files here (`docker-compose.yml`, `frontend/react-app/nginx.conf`).
- ES index shows status `yellow` (1 unassigned replica on a single node) —
  cosmetic for local dev.
