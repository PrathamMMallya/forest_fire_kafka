# Wildfire Risk — System Deep Dive

Complete technical reference: data sources, features, model, Kafka design,
alerting, Elasticsearch, and the MLOps stack. Companion visual version:
`architecture.html`.

> **Two generations of this system exist — know which is which.**
> 1. **gridMET / Western-US generation** — the original project (this git
>    checkout's `streaming/*.py`, `README.md`, and the trained model artifacts
>    in `artifacts/`). Data: gridMET daily climate + wildfire labels, western US.
> 2. **ISRO MOSDAC / India generation** — what is **actually deployed** in the
>    Docker containers right now. Data: INSAT-3DR/3DS satellite hotspots over
>    India + Open-Meteo weather. Its source code lives in the built image, not
>    in this checkout.
> This document covers both, and flags wherever they differ.

---

## 1. Where the data comes from, and what each source gives

### 1.1 ISRO MOSDAC (satellite fire detection — deployed generation)

[MOSDAC](https://mosdac.gov.in) (Meteorological & Oceanographic Satellite Data
Archival Centre) is ISRO's data portal. The pipeline uses:

- **INSAT-3DR/3DS Level-2P Active Fire Hotspots** (KML files, e.g.
  `dataset/Fire-20261030T095345Z-1-001.zip`) — detected fire pixel coordinates
  and observation times.
- **Level-1C / smoke radiance rasters** (HDF5) — raw brightness temperatures
  used to resolve physical values for each hotspot.
- `mdapi.py` is the downloader: token-authenticated client for MOSDAC's
  `download_api` (datasets configured in `config.json`, e.g. `3RIMG_L2B_SST`).

From the raw channels the parser (`streaming/mosdac_parser.py`) derives **7
radiometric features** per fire pixel:

| Feature | Meaning |
|---|---|
| `latitude`, `longitude` | fire pixel location (4 km INSAT pixel) |
| `t_mir` | Mid-Wave Infrared brightness temperature (K) — the fire channel |
| `t_tir1` | Thermal Infrared brightness temperature (K) — background channel |
| `delta_t` | `t_mir − t_tir1` thermal divergence — the core fire signal |
| `frp_mw` | **Fire Radiative Power (MW)**, computed with the Wooster/Kaufman empirical MIR formula: `FRP = 4.34e-19 × 16 km² × (T_MIR⁸ − T_TIR1⁸)` |
| `solar_elev` | solar elevation angle — needed to reject **sun-glint** false alarms (hot bare ground at high sun) |

### 1.2 Open-Meteo (weather context — deployed generation)

[Open-Meteo](https://api.open-meteo.com/v1/forecast) is a free weather API.
For every fire observation (and continuously for tracked cells), the pipeline
fetches **co-temporal weather** at the same lat/lon, cached on disk
(`mosdac_weather_cache.json`) so replays don't re-hit the API. **8 weather
features:**

`temp_c, rh, wind_speed, wind_dir, precip_mm, soil_moisture, vpd` — plus
**`fwi`**, a Fire Weather Index (0–100) the pipeline computes itself:

```
dryness          = max(0, 100 − rh) × 0.35     (0–35)
wind_effect      = min(wind_speed × 3.5, 30)   (0–30)
vpd_effect       = min(vpd × 12, 30)           (0–30)
soil_suppression = max(0, soil_moisture × 50)  (0–25)
FWI = clamp(dryness + wind_effect + vpd_effect − soil_suppression, 0, 100)
```

### 1.3 gridMET (training data — original generation)

[gridMET](https://www.climatologylab.org/gridmet.html) (Climatology Lab,
University of Idaho): 4 km gridded **daily** surface weather for the western
US, joined with a historical wildfire occurrence label. Files:
`Data/Wildfire_Dataset.csv` (full) and `eda/data/eda_sample.csv` (212,500-row
calibrated sample). **17 features per grid cell per day:**

`latitude, longitude, pr (precip), rmax/rmin (rel. humidity), sph (specific
humidity), srad (solar radiation), tmmn/tmmx (min/max temp), vs (wind speed),
bi (burning index), fm100/fm1000 (100h/1000h fuel moisture), erc (energy
release component), etr/pet (evapotranspiration), vpd (vapor pressure deficit)`

**This labeled dataset is what the ML model is trained on** — it is the only
source with ground-truth `Wildfire: Yes/No` labels.

### 1.4 How the sources are combined

- **Spatial key**: every record — satellite event or weather update — is keyed
  by **Uber H3 hexagon index** (resolution 6, ~3.2 km edge, matching the 4 km
  INSAT pixel). Regions use H3 resolution 3 (~60 km).
- **Stream–Table join**: weather goes to the compacted `wildfire.weather`
  topic (a materialized KTable); satellite events flow through `wildfire.raw`.
  Because both are keyed and co-partitioned by H3 cell, `streams_app` performs
  a **shuffle-free left join**: each satellite event is enriched with the
  latest weather state of its cell.
- **Training vs serving**: the RF model was trained on labeled gridMET
  history; at serving time it scores INSAT+MOSDAC events (see the honesty note
  in §4.4).

### 1.5 India vs other countries — where locations come from

| Generation | Geography | Source of locations |
|---|---|---|
| Deployed (MOSDAC) | **India only** — validation bounds lat 5–39, lon 65–100 | Real MOSDAC hotspots; 10 fire-prone forest reserves for live simulation (Gadchiroli/Tadoba, Satpura, Pench, Sundargarh/Simlipal, Saranda, Dudhwa/Pilibhit Terai, Dibru-Saikhowa, Bandipur/Nilgiri, Nallamala, Bastar) + 10 named forest-region boxes + 7 broader land boxes for representative background sampling |
| Original (gridMET) | **Western US** | 7 fire-prone region centers (Idaho, Montana, Oregon, N/S California, Arizona, Washington) |

There is no other-country data in the deployed system; the US data exists only
in the original gridMET generation.

---

## 2. The ML model

### 2.1 What model

**RandomForestClassifier** (scikit-learn, **150 trees**), trained by
`train_stream_model.py`, artifacts in `artifacts/`:

| Artifact | Contents |
|---|---|
| `best_RandomForest.pkl` | the model (`predict_proba`) |
| `preprocessing_artifacts.pkl` | scaler, per-feature imputation medians, feature list |
| `stream_model_meta.json` | training metadata + test metrics |

### 2.2 Training attributes and target

- **Target attribute**: `Wildfire` — binary label (`Yes`/`No` → 1/0): did a
  fire occur at this grid cell on this day.
- **Input attributes (17)** — the gridMET feature set from §1.3.
- Split: **175,000 train / 37,500 test** rows from `eda/data/eda_sample.csv`.

### 2.3 Metrics (offline test set, threshold 0.5)

| Metric | Value |
|---|---|
| Accuracy | **0.827** |
| Precision | 0.224 |
| Recall | **0.854** |
| **F1** | **0.355** |
| ROC-AUC | **0.916** |
| PR-AUC | 0.400 |

### 2.4 Why precision looks "low" — and why that's deliberate

Wildfire days are rare (heavy class imbalance, ~5% positives). The system is
tuned **recall-first**: missing a real fire is the worst outcome; a false
WATCH only costs a monitor task. Recall 0.85–0.93 with precision ~0.22 is the
chosen operating point, confirmed by the live pipeline evaluation
(`evaluation/report.json`, 5,000-event burst):

| Live metric (measured) | Value |
|---|---|
| End-to-end latency (ingest → prediction) | **p50 143 ms · p95 220 ms · p99 241 ms** |
| Throughput observed | 48 eps (300 eps target burst) |
| Recall / Precision / F1 vs embedded labels | **0.932** / 0.232 / 0.372 |
| ROC-AUC / PR-AUC | **0.943** / 0.483 |
| Confusion (n=5000) | TP 245 · FP 809 · FN 18 · TN 3928 |
| Alerts emitted | WATCH 788 · WARNING 175 · EMERGENCY 52 |

### 4.4 Honesty note — what actually scores events in the deployed container

The detector loads `best_RandomForest.pkl` with its **17 gridMET feature
names**, but deployed events carry the **15 MOSDAC features**. Only
`latitude`/`longitude` overlap; the other 15 inputs are imputed with the
training medians at scoring time (`r.get(f, NaN)` → median fill). So in the
current deployment the RF probability is essentially a **spatial prior**, and
the **physical tier rules** (ΔT, FRP, T_MIR, FWI thresholds — §6) plus the
**fallback risk scorer** do most of the real detection work. A `risk`-prefixed
trained model was never produced (`Trained artifacts not found for
prefix=risk`), so risk nowcasts use a logistic physical scorer:
`z = −4.5 + 0.38·ΔT + 0.04·FWI + 0.05·max(0, T_MIR−300)`.
Retraining on the MOSDAC feature set (`detector_model.pkl` /
`risk_model.pkl`) is the intended fix — the detector loads those automatically
when present.

---

## 3. Kafka design — producers, topics, partitions, jobs

Single-node **KRaft** broker (no ZooKeeper), `apache/kafka:3.9.0`, host access
`localhost:9092`, internal `kafka:29092`. Auto-create disabled — all topics
declared explicitly by `streaming/create_topics.py`.

### 3.1 Producers

**One producer service** (`streaming/producer.py`, idempotent producer:
`enable.idempotence=true`, `acks=all`, lz4) with three modes:

| Mode | What it streams |
|---|---|
| `replay` | The compiled MOSDAC dataset CSV (`Data/MOSDAC_Wildfire_Dataset.csv`, 9,356 rows) at configurable rate (`--eps 50 --speed 60` synthetic clock) — **what runs now** |
| `mosdac` | Direct ingestion of raw MOSDAC KML files, resolving physical temperatures + co-temporal Open-Meteo weather |
| `live` | Continuous synthetic feed anchored to the 10 Indian forest reserves, using the same FRP/FWI equations as training |

It writes to **two** topics per event: the satellite observation →
`wildfire.raw`, and the cell's weather state → `wildfire.weather`.

### 3.2 Topics — what and why

| Topic | Partitions | Config | Why |
|---|---|---|---|
| `wildfire.raw` | 6 | 7-day retention | Raw satellite events, **key = H3 cell** → per-cell ordering; 6 partitions for parallel consumption |
| `wildfire.weather` | 6 | **log-compacted**, `min.cleanable.dirty.ratio=0.1` | The **KTable**: latest weather per cell. Compaction keeps only newest state per key → fast recovery, small log |
| `wildfire.features` | 6 | delete, 24 h | Joined + windowed feature vectors (stream-processor output), key = H3 cell |
| `wildfire.predictions` | 3 | 24 h | **Every ML output**: per-event detection probability/label/tier AND per-cell `risk_assessment` (LOW/MODERATE/HIGH/EXTREME) — this is where model predictions are streamed |
| `wildfire.alerts` | 3 | **90-day** retention, key = H3 region (res 3) | Tiered alerts with GeoJSON polygons → ES, dashboard, notifier. Long retention = alert archive |
| `wildfire.dlq` | 1 | 30-day retention | Dead-letter queue: schema/range-invalid events with error context, so poison messages never block the stream |

Co-partitioning `raw` and `weather` on the same H3 key with the same partition
count is what makes the Stream–Table join shuffle-free.

### 3.3 Stream jobs (consumer services)

| Service | Consumer group | Job |
|---|---|---|
| `streams_app` | `mosdac-wildfire-streams` | Validate (schema + India bounds + physical ranges) → DLQ on failure; LRU dedup; **event-time** tumbling windows (60 s, per H3 region) and sliding windows (600 s length / 60 s hop, per cell); 90 s watermark (late events flagged, counted, kept); feature engineering (rolling `r600s_*` aggregates, cyclic calendar, `dry_flag`, sun-glint flag); KTable join → `wildfire.features` |
| `detector` | `mosdac-wildfire-detector` | Consumes `wildfire.features` **and** `wildfire.weather`; vectorized batch scoring (detection bundle) → `wildfire.predictions`; tier rules → `wildfire.alerts`; risk nowcast per weather update → `wildfire.predictions` (type `risk_assessment`) |
| `indexer` | `mosdac-wildfire-indexer` | Bulk-indexes `wildfire.alerts` into Elasticsearch (batches of ≥50), buffers while ES is down, idempotent upsert by `alert_id` |
| `notifier` | `mosdac-wildfire-notifier` | Emergency dispatch for WARNING/EMERGENCY (console + optional `EMERGENCY_WEBHOOK_URL`) |
| `api` (dashboard) | `mosdac-wildfire-dashboard` | Serves :9991 — 3D satellite globe, SSE live feed, `/api/stats`, Prometheus `/metrics`, ES search proxy |
| `producer` | — | Ingestion (§3.1) |

**Delivery semantics**: at-least-once consumers (commit *after* processing) +
per-stage event-id LRU dedup ≈ effectively exactly-once effects — replays
never double-alert. Deterministic `alert_id` (cell|tier|event_ts) makes ES
indexing an idempotent upsert.

---

## 4. Where ML predictions live in the Kafka pipeline

```
MOSDAC KML/CSV ──► producer ──► wildfire.raw ─┐
Open-Meteo API ──► producer ──► wildfire.weather (KTable, compacted)
                                              ▼
                                   streams_app: validate → DLQ
                                   dedup → windows → KTable join
                                   feature engineering
                                              ▼
                                    wildfire.features ──► detector ──► wildfire.predictions  ◄── every event's p(fire), label, tier
                                              │                    ├──► wildfire.alerts      ◄── WATCH/WARNING/EMERGENCY
              wildfire.weather ───────────────┴─────────────────────► wildfire.predictions  ◄── risk_assessment per cell
                                              │
                                              ▼
                              indexer → Elasticsearch (wildfire-alerts)
                              notifier → emergency dispatch
                              api → :9991 dashboard (SSE) + /metrics
```

## 5. How alerts are triggered

After the model scores an event, the detector applies **physical tier rules**
(`streaming/detector.py`) combining probability with radiometry and weather.
Sun-glint correction: under zenith sun (`solar_elev > 55°`), ΔT must be 1.5 K
stronger.

| Tier | Rule (any clause) |
|---|---|
| **EMERGENCY** | `p ≥ 0.80` AND (ΔT_eff ≥ 11.2 K & wind ≥ 2.8 m/s · or FRP ≥ 162 MW · or T_MIR ≥ 315.5 K · or ΔT_eff ≥ 10 & thermal-surge ≥ 2.5 & FWI ≥ 32) |
| **WARNING** | `p ≥ 0.65 & ΔT_eff ≥ 9 & T_MIR ≥ 309` · or `p ≥ 0.75 & FRP ≥ 125` · or `p ≥ 0.70 & rolling-max ΔT ≥ 9.5 & FWI ≥ 30` · or `p ≥ 0.65 & night & ΔT ≥ 7.5` |
| **WATCH** | `p ≥ 0.42 & (ΔT_eff ≥ 5.8 or FWI ≥ 25)` · or `ΔT_eff ≥ 7.5 & T_MIR ≥ 303 & FWI ≥ 22` |

Each alert carries: deterministic `alert_id`, tier, H3 cell + region, lat/lon,
**GeoJSON polygon** (H3 cell boundary), probability, risk probability, model
names, radiometry block, weather block, human-readable message. Dedup: one
alert per (cell, tier) per 60 s. Alerts flow to Elasticsearch, the dashboard
(SSE), and the notifier (WARNING/EMERGENCY dispatch).

## 6. Why Elasticsearch

- **Geo search**: `location` indexed as `geo_point`, cell `geometry` as
  `geo_shape` → "all alerts within this polygon / near this point" queries.
- **Full-text + structured search** over the 90-day alert archive
  (`/api/search?q=emergency` on the dashboard proxies ES).
- **Kibana-ready** (`--profile kibana` → :5601) for operations dashboards.
- **Idempotent upserts** by `alert_id` make at-least-once indexing safe.
- Decoupling: Kafka stays the streaming backbone; ES is the query/retrieval
  tier — each does what it's best at.

## 7. MLOps stack (batch backend, Part B)

| Tool | Role |
|---|---|
| **FastAPI + uvicorn** | Inference server :8000 (`/predict`, `/model`, monitoring endpoints), Dockerized, deployable to K8s (`k8s/` manifests, `wildfire` namespace) |
| **React (Vite) + nginx** | Frontend :8001 — prediction UI + drift dashboard; nginx proxies `/api` → inference |
| **Evidently** (`DataDriftPreset`) | `monitoring/drift_runner.py` compares live prediction logs (`monitoring/current/current.parquet`) against reference → `status.json` with `share_of_drifted_columns`; `trigger_retrain` when score ≥ threshold |
| **Prefect** | `prefect/flows/retrain_flow.py` — drift-triggered (or `--force`) retraining pipeline |
| **MLflow** | Experiment tracking + model registry (`train.py` logs runs; `mlruns/`; `mlflow ui`) |
| **DVC** | Data/artifact versioning (`dvc.yaml`, `dvc.lock`) — retraining versions data alongside the model |
| **Docker / Kubernetes** | `Dockerfile` (inference image), compose for dev, K8s deployment + service + port-forward :8000 for prod-like serving |
| **GitHub Actions** | CI/CD workflows (`.github/`) |
| **Prometheus** | `/metrics` on :9991 scraped for pipeline observability |

Drift loop: inference logs live traffic → `current.parquet` → Evidently vs
reference → `status.json` → Prefect retrain → MLflow registry + DVC version →
redeploy.

---

## 8. Current live status (verified 2026-10-31)

Pipeline running in Docker: **566k+ predictions**, **44k+ alerts** indexed in
`wildfire-alerts`, end-to-end healthy on :9991 / :8001 / :8000 / :9200.
