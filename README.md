# 🔥 Wildfire Risk — Real-Time Kafka Streaming Pipeline + FastAPI Backend

An end-to-end wildfire risk system with two integrated parts:

1. **Real-time streaming pipeline (primary)** — Data → Kafka Producer → Kafka Topics →
   Streams (filter + window + feature engineering) → ML Fire Detection → Fire Prediction →
   Alert Topic → Elasticsearch → **3D satellite dashboard on http://localhost:9991** /
   Emergency Alerts → Performance Evaluation.
2. **Batch backend** — training, FastAPI inference, drift monitoring, Prefect
   retraining, DVC versioning (execution guide in *Part B* below).

---

# Part A — Real-Time Kafka Streaming Pipeline

## 1. Where the real-time data comes from

The streaming layer ingests **[gridMET](https://www.climatologylab.org/gridmet.html)**-derived
daily climate observations (Climatology Lab, University of Idaho — 4 km gridded surface
weather for the western US) joined with a **wildfire occurrence label** (`Wildfire: Yes/No`).
The project dataset is `Data/Wildfire_Dataset.csv`; a calibrated sample used for replay and
training lives at `eda/data/eda_sample.csv` (regenerable with `eda/generate_sample.py`).

**17 gridMET features per grid cell per day:**

```
latitude, longitude, pr (precipitation), rmax/rmin (rel. humidity),
sph (specific humidity), srad (solar radiation), tmmn/tmmx (min/max temp),
vs (wind speed), bi (burning index), fm100/fm1000 (fuel moisture 100/1000h),
erc (energy release component), etr/pet (evapotranspiration), vpd (vapor pressure deficit)
```

The producer (`streaming/producer.py`) has two modes:

| Mode | What it does |
|---|---|
| `replay` | Streams the historical CSV through Kafka with a synthetic clock (configurable speed, e.g. `--speed 60` = 60× real time) — simulates a live sensor feed |
| `live` | Generates a continuous synthetic random-walk feed anchored to 7 fire-prone region centers (Idaho, Montana, Oregon, N. California, S. California, Arizona, Washington) |

> **Honesty note:** there is no public real-time wildfire *sensor* API with labels, so the
> "real-time" feed is a simulated ingestion stream — the standard approach for demonstrating
> streaming systems. The downstream contract is just the JSON event schema, so a real live
> source (weather API socket, satellite feed) can replace the producer without touching
> any other stage.

## 2. Exploratory Data Analysis (EDA)

Run by `eda/eda.py`, charts in `eda/charts/`, findings in `eda/eda_summary.txt`:

| Chart | What it answers |
|---|---|
| `01_class_balance.png` | How imbalanced are fire vs no-fire days? |
| `02_missing_values.png` | Which features have gaps (incl. the `32767` sentinel) |
| `03_feature_distributions.png` | Shape of every feature |
| `04_boxplots_by_class.png` | Which features separate fire from no-fire |
| `05_correlation_heatmap.png` | Feature redundancy / correlations |
| `06_monthly_seasonality.png` | Fires peak Jun–Sep (fire season) |
| `07_yearly_trend.png` | Inter-annual trend |
| `08_spatial_fire_density.png` | Where fires cluster (lat/lon density) |
| `09_kde_fire_weather.png` | ERC / VPD / fuel-moisture separation on fire days |
| `10_feature_class_shift.png` | Distribution shift between classes |
| `11_outliers_iqr.png` | Outlier audit — extremes kept, they *are* the signal |
| `12_daily_fire_bursts.png` | Fire bursts per day (event-time windowing design input) |
| `13_pr_zero_inflation.png` | Zero-inflated precipitation → `dry_flag` feature |

Key findings that shaped the pipeline: strong class imbalance (recall-first evaluation),
zero-inflated `pr` → `dry_flag`, `erc`/`fm100`/`vpd` are the strongest fire-day signals,
`32767` is a missing-value sentinel (not a real reading), and fires arrive in daily bursts
(per-cell sliding windows).

## 3. Kafka architecture

Single-node KRaft broker (`localhost:9092`), five topics:

| Topic | Partitions | Config | Purpose |
|---|---|---|---|
| `wildfire.raw` | 6 | 7-day retention | Raw events, keyed by grid cell (per-cell ordering) |
| `wildfire.features` | 3 | **log-compacted** | Enriched feature vectors + window summaries — latest state per cell survives restarts |
| `wildfire.predictions` | 3 | 24-h retention | Every scored event (probability, tier, label) |
| `wildfire.alerts` | 3 | 90-day retention, keyed by region | Tiered alerts → ES, dashboard, notifier |
| `wildfire.dlq` | 1 | — | Schema-invalid events with error context |

**Streaming semantics implemented** (Kafka-Streams behavior, Python topology on the log):

- **Idempotent producer** — `enable.idempotence=true`, `acks=all`, lz4 compression
- **At-least-once + dedup = effectively exactly-once effects** — consumers commit *after*
  processing; every stage has an event-id LRU dedup store, so replays never double-alert
- **Event-time windowing** — tumbling 60 s windows (per region) + sliding 10 min / 60 s hop
  windows (per cell), not processing-time
- **Watermark** — events older than `max_ts − 90 s` are flagged `late` (counted, not dropped)
- **Dead-letter queue** — schema/range validation failures go to `wildfire.dlq` with errors
- **Log compaction** — `wildfire.features` keeps the latest feature state per cell for crash recovery

## 4. Pipeline & methodology (stage by stage)

| # | Methodology stage | Implementation |
|---|---|---|
| 1 | Data | `Data/Wildfire_Dataset.csv` / `eda/data/eda_sample.csv` |
| 2 | Kafka Producer | `streaming/producer.py` — replay or live feed, event envelope with `event_id`, `event_ts`, `ingest_ts` |
| 3 | Kafka Topics | `streaming/create_topics.py` (AdminClient) |
| 4 | Filtering & Windowing | `streaming/streams_app.py` — validation → DLQ, dedup, tumbling + sliding windows, watermark |
| 5 | Feature Engineering | same topology — median imputation (learned from first 300 events), `dry_flag`, cyclic calendar features (`month_sin/cos`), rolling aggregates (`r600s_*_mean/max/sum`) |
| 6 | ML/DL Fire Detection | `streaming/detector.py` — RandomForest on the same serving artifacts as the inference API (`artifacts/best_RandomForest.pkl` + `preprocessing_artifacts.pkl`), vectorized batch scoring |
| 7 | Fire Prediction | `wildfire.predictions` — probability, binary prediction, tier, full traceability (`event_id`, `ingest_ts` → publish `ts`) |
| 8 | Kafka Alert Topic | tier rules with per-(cell, tier) dedup gap (60 s): **WATCH** `p>0.5 & erc>45` · **WARNING** `p>0.7 & erc>60 & fm100<10` · **EMERGENCY** `p>0.9 & wind>8` |
| 9 | Elasticsearch | `streaming/indexer.py` — bulk `_bulk` indexing to `wildfire-alerts` (`location` as `geo_point`); buffers alerts while ES is down and flushes on reconnect |
| 10 | Dashboard / Emergency Alerts | `streaming/api.py` on **:9991** — 3D satellite globe + SSE live feed + Prometheus `/metrics`; `streaming/notifier.py` — console + optional `EMERGENCY_WEBHOOK_URL` |
| 11 | Performance Evaluation | `streaming/evaluate.py` → `evaluation/report.json` |
| 12 | Monitoring | `/metrics` (Prometheus format), pipeline health panel, per-stage stats files in `logs/` |

### The 3D satellite dashboard

Open **http://localhost:9991** — MapLibre GL v5 globe projection with Esri World Imagery
satellite tiles and AWS terrarium 3D terrain (no API keys). Fire alerts stream in over SSE
as pulsing tier-colored markers at their real coordinates; the camera auto-flies to each
new WARNING/EMERGENCY and tilts to pitch 55°; click any marker (or feed row) for the live
result popup — p(fire), ERC, wind, fuel moisture, cell, time. Toggles: auto-follow, 3D terrain.

## 5. Input / Output

**Input event** (one grid-cell-day reading, produced to `wildfire.raw`):

```json
{
  "event_id": "47.59_-119.57|2016-07-14|1",
  "event_ts": "2016-07-14T00:00:00+00:00",
  "ingest_ts": "2026-10-03T22:30:01.123+00:00",
  "values": {"latitude": 47.59, "longitude": -119.57, "pr": 0.0, "erc": 58.2,
             "fm100": 8.1, "vs": 6.4, "tmmx": 308.1, "vpd": 22.7, "...": "..."},
  "label": "Yes"
}
```

**Outputs:**

| Output | Where |
|---|---|
| Prediction per event (probability, tier) | `wildfire.predictions` topic |
| Tiered fire alert with location | `wildfire.alerts` topic |
| Searchable alert documents | Elasticsearch index `wildfire-alerts` (`geo_point` search, Kibana-ready) |
| Live 3D dashboard + feed + health | http://localhost:9991 (SSE) |
| Emergency dispatch lines | `notifier.py` console / webhook (WARNING & EMERGENCY) |
| Prometheus metrics | http://localhost:9991/metrics |
| Performance report | `evaluation/report.json` |

## 6. How to run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.streaming.txt
python train_stream_model.py        # creates artifacts/ (skipped if present)
```

**Option 1 — Docker Compose (recommended when Docker is available)**

```bash
./scripts/start_pipeline.sh         # kafka + es + all six services + producer
./scripts/stop_pipeline.sh [--infra]
```

**Option 2 — native (no Docker; verified path on macOS, also how this machine runs it)**

```bash
# 1) Kafka single-node KRaft
KAFKA=~/kafka-local/kafka_2.13-3.9.0
$KAFKA/bin/kafka-storage.sh format -t $($KAFKA/bin/kafka-storage.sh random-uuid) \
  -c $KAFKA/config/kraft/server.properties
$KAFKA/bin/kafka-server-start.sh -daemon $KAFKA/config/kraft/server.properties

# 2) Elasticsearch 8.x single node (xpack.security.enabled=false for local dev)
~/kafka-local/elasticsearch-8.15.0/bin/elasticsearch -d -p /tmp/es.pid

# 3) Topics + services
python -m streaming.create_topics
mkdir -p logs
for svc in streams_app detector indexer notifier api; do
  nohup python -m streaming.$svc >logs/$svc.log 2>&1 & echo $! > logs/$svc.pid
done
nohup python -m streaming.producer --mode replay --eps 50 --loop >logs/producer.log 2>&1 &
```

Then open **http://localhost:9991**.

| Port | Service |
|---|---|
| 9991 | Dashboard + API + `/metrics` |
| 9092 | Kafka broker |
| 9200 | Elasticsearch |

Useful commands:

```bash
python -m streaming.evaluate --events 5000 --eps 200   # performance report
python -m streaming.create_topics                      # (re)create topics
curl localhost:9991/api/stats                          # live pipeline stats
curl localhost:9991/api/search?q=emergency             # search alerts in ES
docker compose -f deploy/docker-compose.yml --profile ui up -d     # Kafka UI :8085
docker compose -f deploy/docker-compose.yml --profile kibana up -d # Kibana :5601
```

## 7. Performance evaluation (measured, `evaluation/report.json`)

5000-event burst at 200 eps through the live pipeline:

| Metric | Value |
|---|---|
| End-to-end latency (ingest → prediction published) | **p50 = 143 ms · p95 = 220 ms · p99 = 241 ms** |
| Detection quality vs embedded labels | recall **0.932**, precision 0.232, F1 0.372 |
| ROC-AUC / PR-AUC | **0.943** / 0.483 |
| Confusion (n=5000) | TP 245 · FP 809 · FN 18 · TN 3928 |
| Alerts emitted | WATCH 788 · WARNING 175 · EMERGENCY 52 |
| Late events / duplicates / DLQ | 0 / deduped / ~0.3 % (poison-rate injection) |

Precision is low because the classes are heavily imbalanced — the system is tuned
recall-first (miss a fire = worst outcome; a false WATCH costs a monitor task).

## 8. Tests

```bash
pytest tests/test_streams.py -v     # 13 tests: validation, sentinel handling,
                                    # tier rules, dedup, sliding windows, tumbling closes
```

## 9. Project structure (streaming part)

```
├── streaming/
│   ├── common.py          # settings, feature schema, validation, cell/region keys
│   ├── create_topics.py   # Kafka AdminClient topic creation
│   ├── producer.py        # replay / live producer (idempotent)
│   ├── streams_app.py     # filter → dedup → windows → features (Streams analogue)
│   ├── detector.py        # ML scoring + tiered alerting
│   ├── indexer.py         # Kafka → Elasticsearch bulk indexer
│   ├── notifier.py        # emergency dispatch (console / webhook)
│   ├── api.py             # dashboard :9991 (3D globe, SSE, metrics, ES proxy)
│   └── evaluate.py        # latency / throughput / quality report
├── train_stream_model.py  # trains the streaming detector artifacts
├── scripts/               # start_pipeline.sh / stop_pipeline.sh
├── deploy/                # docker-compose.yml + Dockerfile.streaming
├── tests/test_streams.py  # unit tests
├── eda/                   # eda.py + charts/ + summary
├── evaluation/report.json # measured results
└── presentation/          # project deck
```

---

# Part B — Batch FastAPI Backend (Full Execution Guide)

> ⚠️ This part documents **everything required to make the batch system work**, even if some
> steps are not ideal for production. **Follow the order exactly. Skipping steps will break
> drift detection and retraining.**

## 🧠 Critical Execution Flow (READ FIRST)

**Data drift detection will NOT work unless ALL of the following are true:**

1. FastAPI inference server is running
2. Producer is running and sending requests
3. Predictions are being logged
4. `current.parquet` is generated from live traffic

❌ If **any** step is skipped, **drift will be empty** and retraining will never trigger.

## ⚙️ Prerequisites

- Python **3.10+**, Docker Desktop (**Kubernetes ENABLED**), `kubectl`, Git
- ❌ Do NOT install Minikube or Kind — Kubernetes is provided by Docker Desktop only

## 🐍 1. Environment

```bash
python -m venv venv
pip install -r requirements.txt -r requirements.inference.txt
```

## 🧠 2. Initial Model Training

```bash
python train.py        # generates artifacts/ and models/
```

## 🚢 3. Build Inference Image

```bash
docker build -t wildfire-inference:latest .
```

## ☸️ 4. Kubernetes Deployment (Docker Desktop)

```bash
kubectl config current-context                 # expect: docker-desktop
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl port-forward svc/wildfire-inference 8000:8000 -n wildfire
# Swagger UI: http://localhost:8000/docs
```

## 🔥 5. Start FastAPI (MANDATORY FOR DRIFT)

Inference must be running before the producer: `http://localhost:8000/predict`.

## 📡 6. Start Producer (MANDATORY FOR DRIFT)

```bash
python streaming/producer.py
```

Sends live prediction requests, logs predictions, creates `monitoring/current/current.parquet`.
If the producer is not running → `current.parquet` will NOT exist → drift will be empty.

## 🔍 7. Run Drift Detection

```bash
python -m monitoring.drift_runner    # writes monitoring/drift/status.json
# e.g. {"dataset_drift": true, "drift_score": 0.92, "trigger_retrain": true}
```

## 🔁 8. Prefect Retraining Pipeline

```bash
python prefect/flows/retrain_flow.py           # drift-based
python prefect/flows/retrain_flow.py --force   # manual override
```

Reads `status.json`, retrains, registers the model in MLflow, versions data/artifacts with DVC.

## ✅ Full Required Command Sequence

```bash
python train.py
docker build -t wildfire-inference:latest .
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl port-forward svc/wildfire-inference 8000:8000 -n wildfire
python streaming/producer.py
python -m monitoring.drift_runner
python prefect/flows/retrain_flow.py
```

## 🛑 Common Failure Points

| Issue | Cause |
|---|---|
| Drift empty | Producer not running |
| `current.parquet` missing | FastAPI not running |
| Prefect stuck | Windows + ephemeral server |
| No retrain | `trigger_retrain=false` |
