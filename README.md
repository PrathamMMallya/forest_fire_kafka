# 🔥 ISRO MOSDAC Wildfire Monitor — Real-Time Kafka Streaming Pipeline

> **New in branch `feat-india-mosdac-pipeline`**: Full port from legacy US gridMET to **ISRO MOSDAC INSAT-3DR/3DS Satellite Radiometry + Open-Meteo Real-Time Weather Stream** over **India**, with **Uber H3 Hexagonal Co-Partitioning** on **Apache Kafka (KRaft mode)**.  
> 📖 **Comprehensive Technical Documentation**: See [`docs/MOSDAC_PIPELINE_DOCUMENTATION.md`](docs/MOSDAC_PIPELINE_DOCUMENTATION.md).

---

## 1. System Architecture & Dual-Stream Ingestion

The streaming layer ingests and co-partitions two independent real-time data streams:
1. **ISRO MOSDAC Satellite Ingestion (`raw.insat.l1c` / `wildfire.raw`)**:
   - Ingests geostationary observations from **INSAT-3DR and INSAT-3DS** at half-hourly intervals.
   - Decodes physical **Mid-Wave Infrared** ($T_{\text{MIR}}$, $3.9\,\mu\text{m}$) and **Thermal Infrared** ($T_{\text{TIR1}}$, $10.8\,\mu\text{m}$) brightness temperatures from the 50 scans in `MOSDAC Data/Fire/*.kml` and upcoming `.h5` rasters.
   - Calculates **Contextual Thermal Divergence** ($\Delta T = T_{\text{MIR}} - T_{\text{TIR1}}$), **Fire Radiative Power** ($\text{FRP}$, MW), and **Solar Elevation** angle to filter sun glint.
2. **Open-Meteo Real-Time Atmospheric Ingestion (`raw.weather.aws`)**:
   - Queries batched live meteorology (`temp_c`, `rh`, `wind_speed`, `wind_dir`, `precip_mm`, `soil_moisture`, `vpd`, `fwi`) for exact forest coordinates across India.
   - Persists responses to `Data/mosdac_weather_cache.json` for offline resilience.

Both streams are keyed by **Uber H3 Resolution 6 (`~3.2 km` edge)** so satellite pixels and weather readings for the same forest reserve land on the **same Kafka partition**, enabling low-latency stream-table joins.

### 15 Core Fused Features per H3 Hexagon:
```
latitude, longitude, t_mir, t_tir1, delta_t, frp_mw, solar_elev,
temp_c, rh, wind_speed, wind_dir, precip_mm, soil_moisture, vpd, fwi
```

The producer (`streaming/producer.py`) supports three modes:

| Mode | What it does |
|---|---|
| `replay` | Streams the compiled Indian MOSDAC dataset (`Data/MOSDAC_Wildfire_Dataset.csv`) with synthetic clock compression |
| `mosdac` | Directly parses and streams all 50 raw INSAT KML files from `MOSDAC Data/Fire/` with Open-Meteo weather |
| `live` | Continuous real-time feed anchored to 10 fire-prone Indian forest reserves (Satpura, Gadchiroli, Bastar, Simlipal, Bandipur, Corbett, etc.) |

---

## 2. Kafka KRaft Architecture

Single-node Apache Kafka in KRaft mode (no Zookeeper, Docker container `wildfire-kafka`), six topics:

| Topic | Partitions | Config | Purpose |
|---|---|---|---|
| `wildfire.raw` | 6 | 7-day retention | Satellite thermal KStream, keyed by Uber H3 resolution 6 cell ID |
| `wildfire.weather` | 6 | **log-compacted** | Meteorological KTable (`wind_speed_unit=ms`), keyed by H3 resolution 6 cell ID |
| `wildfire.features` | 6 | 24-h retention (`delete`) | Joined satellite+weather vectors + sliding/tumbling window outputs |
| `wildfire.predictions` | 3 | 24-h retention | Every scored event (probability, tier, label) |
| `wildfire.alerts` | 3 | 90-day retention | Tiered alerts (`WATCH` / `WARNING` / `EMERGENCY`) with GeoJSON polygons, keyed by H3 region ID (res 3) |
| `wildfire.dlq` | 1 | 30-day retention | Dead-letter queue for schema/range validation failures |

---

## 3. Pipeline Stages

| Stage | Implementation | Function |
|---|---|---|
| **Data Parser** | `streaming/mosdac_parser.py` | Slices MOSDAC KML `<address>` strings, decodes $T_{\text{MIR}}$ and $T_{\text{TIR1}}$, computes $\Delta T$ and physical $\text{FRP}$ ($\text{MW}$), fetches co-temporal Open-Meteo hourly weather in $\text{m/s}$ |
| **Kafka Producer** | `streaming/producer.py` | Publishes weather KTable updates to `wildfire.weather` and satellite observations to `wildfire.raw` (keyed by H3 cell ID) |
| **Topic Admin** | `streaming/create_topics.py` | AdminClient topic creation with KRaft broker |
| **Join & Windowing** | `streaming/streams_app.py` | `WeatherTableStore` Stream-Table Left Join, validation → DLQ, $O(1)$ `OrderedDict` LRU dedup, median imputation, 10-min sliding windows (`delta_t_surge`), 60s regional tumbling windows (`fire_count` + periodic flush) |
| **ML Fire Detection** | `streaming/detector.py` | Scikit-learn RandomForest (spatial `GroupShuffleSplit`) + sun-glint-aware contextual rules (`WATCH`, `WARNING`, `EMERGENCY`) |
| **Elasticsearch** | `streaming/indexer.py` | Bulk indexing to `wildfire-alerts` with `geo_point` mapping |
| **3D India Dashboard** | `streaming/api.py` on **:9991** | MapLibre GL 3D satellite globe centered on India ($[79.5^\circ\text{E}, 22.0^\circ\text{N}]$), SSE stream, Prometheus `/metrics` |
| **Emergency Dispatch** | `streaming/notifier.py` | Console alerts & webhook dispatcher for WARNING / EMERGENCY tiers |
| **Performance Eval** | `streaming/evaluate.py` | End-to-end latency percentiles (p50/p95/p99) and classification metrics |

---

## 4. How to Run

### Step 1: Install Dependencies & Prepare Model
```bash
pip install -r requirements.streaming.txt
python -m eda.generate_sample        # Generates Data/MOSDAC_Wildfire_Dataset.csv
python train_stream_model.py         # Trains RandomForest on MOSDAC features -> artifacts/
```

### Step 2: Option A — Docker Compose (Recommended)
```bash
# Starts Kafka KRaft, Elasticsearch, and all streaming services
docker compose -f deploy/docker-compose.yml --profile app up -d

# Open the 3D Satellite Dashboard
http://localhost:9991
```

### Step 3: Option B — Native Host Execution
```bash
# 1. Create Kafka topics
python -m streaming.create_topics

# 2. Start streaming microservices
python -m streaming.streams_app &
python -m streaming.detector &
python -m streaming.api &

# 3. Start Ingestion Producer
# To stream the compiled MOSDAC Indian dataset:
python -m streaming.producer --mode replay --eps 30 --speed 60

# Or to stream directly from raw MOSDAC KML files:
python -m streaming.producer --mode mosdac --kml-dir "MOSDAC Data/Fire" --eps 20

# Or to run the continuous live Indian forest stream:
python -m streaming.producer --mode live --eps 15
```

---

## 5. Verification & Unit Tests

Run the pure Python unit test suite (no broker required):
```bash
python -c "
import tests.test_streams as ts
for name in dir(ts):
    if name.startswith('test_'):
        getattr(ts, name)()
        print(f'PASS: {name}')
print('\nALL UNIT TESTS PASSED!')
"
```

To run the complete Exploratory Data Analysis (EDA) diagnostics and regenerate all 13 charts in `eda/charts/`:
```bash
python -m eda.eda
```

---

## 6. Project Structure

```
├── docs/
│   ├── MOSDAC_PIPELINE_DOCUMENTATION.md # Comprehensive system technical documentation
│   ├── SPA Lab Synopsis.pdf             # University course project synopsis
│   └── SPA EDA.pdf                      # INSAT-3DS radiometric exploratory data analysis
├── MOSDAC Data/
│   └── Fire/                            # 50 INSAT-3DR half-hourly L2P fire KML scan files
├── streaming/
│   ├── mosdac_parser.py                 # ISRO KML/HDF5 decoder + Open-Meteo weather client
│   ├── common.py                        # H3 spatial indexing, settings, 15-feature schema, validation
│   ├── create_topics.py                 # Kafka AdminClient topic provisioning
│   ├── producer.py                      # Replay, MOSDAC KML, and live Indian forest stream producer
│   ├── streams_app.py                   # Filtering, LRU dedup, H3 sliding/tumbling windows
│   ├── detector.py                      # Hybrid Contextual Thermal (dT > 10K) + ML scoring
│   ├── indexer.py                       # Bulk indexer to Elasticsearch
│   ├── notifier.py                      # Emergency console & webhook dispatcher
│   ├── api.py                           # 3D India satellite dashboard (:9991), SSE, /metrics
│   └── evaluate.py                      # End-to-end latency & quality performance report
├── train_stream_model.py                # Trains RandomForest on MOSDAC + Open-Meteo features
├── eda/
│   ├── generate_sample.py               # Compiles MOSDAC Indian fire dataset & offline weather cache
│   ├── eda.py                           # Generates 13 EDA diagnostic charts
│   └── charts/                          # Diagnostic visualization figures
├── tests/test_streams.py                # Unit tests for validation, H3 indexing, and window logic
├── deploy/                              # docker-compose.yml + Dockerfile.streaming
└── config.example.json                  # Sanitized MOSDAC API template
```
