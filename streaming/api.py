"""Dashboard + monitoring API on http://localhost:9991

FastAPI service wired into the Kafka pipeline with its own consumer group:
  * tails `wildfire.predictions` and `wildfire.alerts` into in-memory rings
  * computes live throughput, end-to-end latency percentiles, tier counts
  * serves a self-contained live dashboard (SSE) at GET /
  * proxies Elasticsearch for alert search at GET /api/search

Endpoints:
    GET /                     live dashboard (HTML)
    GET /api/health           liveness + pipeline component status
    GET /api/stats            throughput / latency / tier / dlq snapshot
    GET /api/alerts           recent alerts (ring buffer)
    GET /api/predictions      recent predictions
    GET /api/events           Server-Sent Events stream (alerts + stats)
    GET /api/search?q=...     search alerts in Elasticsearch
    GET /metrics              Prometheus-format metrics

Run:  python -m streaming.api
"""
from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests as http_requests
from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse

from streaming.common import (
    Settings, from_json, get_logger, kafka_consumer_conf, now_ms, to_json,
)

log = get_logger("api")
s = Settings()
app = FastAPI(title="Wildfire Streaming Dashboard", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


class PipelineState:
    """In-memory view of the live pipeline (fed by the Kafka consumer thread)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.alerts: deque = deque(maxlen=300)
        self.predictions: deque = deque(maxlen=300)
        self.latencies_ms: deque = deque(maxlen=2000)
        self.pred_times: deque = deque(maxlen=100000)   # for eps
        self.tier_counts = {"NONE": 0, "WATCH": 0, "WARNING": 0, "EMERGENCY": 0}
        self.total_predictions = 0
        self.total_alerts = 0
        self.late_events = 0
        self.started = now_ms()
        self.subscribers: list[asyncio.Queue] = []

    def add_prediction(self, p: dict):
        lat = None
        if p.get("ingest_ts") and p.get("ts"):
            try:
                t0 = datetime.fromisoformat(p["ingest_ts"]).timestamp()
                lat = (p["ts"] / 1000 - t0) * 1000
            except (ValueError, OSError):
                pass
        with self.lock:
            self.predictions.appendleft(p)
            self.total_predictions += 1
            self.pred_times.append(time.time())
            if lat is not None and lat < 600000:
                self.latencies_ms.append(lat)
            if p.get("late"):
                self.late_events += 1

    def add_alert(self, a: dict):
        with self.lock:
            self.alerts.appendleft(a)
            self.total_alerts += 1
            tier = a.get("tier", "WATCH")
            self.tier_counts[tier] = self.tier_counts.get(tier, 0) + 1
        self.publish(a)

    def publish(self, alert: dict):
        for q in list(self.subscribers):
            try:
                q.put_nowait(("alert", alert))
            except Exception:
                pass

    def stats(self) -> dict:
        with self.lock:
            now = time.time()
            recent = [t for t in self.pred_times if now - t <= 30]
            lats = sorted(self.latencies_ms)
            def pct(p):
                if not lats:
                    return None
                return round(lats[min(int(len(lats) * p), len(lats) - 1)], 1)
            sp = Path("logs/streams_stats.json")
            det = Path("logs/detector_stats.json")
            streams = json.loads(sp.read_text()) if sp.exists() else {}
            detector = json.loads(det.read_text()) if det.exists() else {}
            return {
                "uptime_s": int((now_ms() - self.started) / 1000),
                "predictions_total": self.total_predictions,
                "alerts_total": self.total_alerts,
                "tier_counts": dict(self.tier_counts),
                "throughput_eps_recent": round(len(recent) / 30, 1),
                "latency_ms": {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)},
                "late_events": self.late_events,
                "streams": streams,
                "detector": detector,
                "es_up": self._es_up(),
            }

    @staticmethod
    def _es_up() -> bool:
        try:
            return http_requests.head(f"{s.es_url}/wildfire-alerts", timeout=2).status_code == 200
        except Exception:
            return False


STATE = PipelineState()
SUB_Q: "queue.Queue[tuple]" = queue.Queue(maxsize=1000)


def kafka_tail_loop():
    """Background thread: tail predictions + alerts topics."""
    consumer = Consumer(kafka_consumer_conf("wildfire-dashboard", s))
    consumer.subscribe([s.topic_predictions, s.topic_alerts])
    try:
        while True:
            msgs = consumer.consume(num_messages=300, timeout=0.3)
            for m in msgs:
                if m.error():
                    continue
                try:
                    ev = from_json(m.value())
                except Exception:
                    continue
                if m.topic() == s.topic_predictions:
                    STATE.add_prediction(ev)
                else:
                    STATE.add_alert(ev)
                    try:
                        SUB_Q.put_nowait(("alert", ev))
                    except queue.Full:
                        pass
    except Exception as exc:
        log.error("tail thread died: %s", exc)
    finally:
        consumer.close()


@app.on_event("startup")
def _start():
    threading.Thread(target=kafka_tail_loop, daemon=True).start()
    log.info("dashboard API on port %s", s.api_port)


# ------------------------------------------------------------------ REST ----

@app.get("/")
def index():
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/health")
def health():
    return {"status": "healthy", "ts": now_ms(), "bootstrap": s.bootstrap}


@app.get("/api/stats")
def stats():
    return STATE.stats()


@app.get("/api/alerts")
def alerts(limit: int = Query(50, le=300)):
    with STATE.lock:
        return list(STATE.alerts)[:limit]


@app.get("/api/predictions")
def predictions(limit: int = Query(50, le=300)):
    with STATE.lock:
        return list(STATE.predictions)[:limit]


@app.get("/api/events")
async def events():
    async def gen():
        q: asyncio.Queue = asyncio.Queue()
        STATE.subscribers.append(q)
        try:
            while True:
                try:
                    kind, ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: {kind}\ndata: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            STATE.subscribers.remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/search")
def search(q: str = "", tier: str = "", size: int = 25):
    body = {"query": {"bool": {"must": [], "filter": []}}, "sort": [{"@timestamp": "desc"}],
            "size": min(size, 100)}
    if q:
        body["query"]["bool"]["must"].append({"match": {"message": q}})
    if tier:
        body["query"]["bool"]["filter"].append({"term": {"tier": tier}})
    try:
        r = http_requests.get(f"{s.es_url}/wildfire-alerts/_search",
                              json=body, timeout=5)
        r.raise_for_status()
    except Exception as exc:
        raise HTTPException(503, f"Elasticsearch unavailable: {exc}")
    hits = r.json()["hits"]["hits"]
    return {"total": r.json()["hits"]["total"]["value"],
            "results": [h["_source"] for h in hits]}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    st = STATE.stats()
    lines = [
        f"wildfire_predictions_total {st['predictions_total']}",
        f"wildfire_alerts_total {st['alerts_total']}",
        f"wildfire_throughput_eps {st['throughput_eps_recent']}",
        f"wildfire_latency_p50_ms {st['latency_ms']['p50'] or 0}",
        f"wildfire_latency_p95_ms {st['latency_ms']['p95'] or 0}",
        f"wildfire_late_events {st['late_events']}",
        f"wildfire_dlq_events {st['streams'].get('invalid', 0)}",
        "wildfire_es_up " + ("1" if st["es_up"] else "0"),
    ]
    for tier, n in st["tier_counts"].items():
        lines.append(f'wildfire_alerts_by_tier{{tier="{tier}"}} {n}')
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------- dashboard ----

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wildfire Stream Monitor</title>
<link href="https://unpkg.com/maplibre-gl@5.6.0/dist/maplibre-gl.css" rel="stylesheet">
<script src="https://unpkg.com/maplibre-gl@5.6.0/dist/maplibre-gl.js"></script>
<style>
:root{
  --bg:#0f1522; --panel:#171e2e; --panel2:#1d2537; --line:#2a3450;
  --ink:#eef1f7; --ink2:#9aa3b8; --muted:#6b7488;
  --orange:#eb6834; --blue:#2a78d6; --aqua:#1baf7a; --red:#e34948;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,'Segoe UI',Roboto,sans-serif;padding:18px}
header{display:flex;align-items:center;gap:14px;margin-bottom:14px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--aqua);box-shadow:0 0 8px var(--aqua)}
.dot.down{background:var(--red);box-shadow:0 0 8px var(--red)}
h1{font-size:19px;font-weight:700}
.sub{color:var(--ink2);font-size:12.5px}
.right{margin-left:auto;color:var(--muted);font-size:12px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
.tile{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:11px 14px}
.tile .v{font-size:22px;font-weight:700;letter-spacing:-.5px}
.tile .k{color:var(--ink2);font-size:11px;text-transform:uppercase;letter-spacing:.8px;margin-top:2px}
.tile.o .v{color:var(--orange)} .tile.b .v{color:var(--blue)} .tile.a .v{color:var(--aqua)} .tile.r .v{color:var(--red)}
.cols{display:grid;grid-template-columns:minmax(0,2.1fr) minmax(0,1fr);gap:12px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.panel h2{font-size:11.5px;text-transform:uppercase;letter-spacing:1px;color:var(--ink2);margin-bottom:9px}
#map{width:100%;height:560px;border-radius:8px}
.map-tools{display:flex;gap:10px;align-items:center;margin-bottom:8px;font-size:12px;color:var(--ink2)}
.map-tools label{display:flex;gap:5px;align-items:center;cursor:pointer}
.map-tools .legend{margin-left:auto;display:flex;gap:12px}
.map-tools .legend i{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:4px}
.feed{max-height:492px;overflow-y:auto}
.row{display:flex;gap:9px;align-items:baseline;padding:7px 10px;border-radius:8px;margin-bottom:6px;background:var(--panel2);cursor:pointer}
.row:hover{outline:1px solid var(--line)}
.badge{font-size:10px;font-weight:700;padding:2px 8px;border-radius:20px;letter-spacing:.5px;white-space:nowrap}
.WATCH{background:#3a2c14;color:#f5a65b}.WARNING{background:#46281a;color:#f5835b}.EMERGENCY{background:#4b1b1b;color:#ff7a79}
.cell{font-weight:600}.msg{color:var(--ink2);font-size:12px;flex:1}
.time{color:var(--muted);font-size:11px;white-space:nowrap}
.kv{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--line);font-size:12.5px}
.kv:last-child{border:none}.kv span:first-child{color:var(--ink2)}
.empty{color:var(--muted);text-align:center;padding:26px 0;font-size:13px}
/* fire markers */
.mk{width:16px;height:16px;border-radius:50%;border:2px solid #fff;box-shadow:0 0 12px 2px currentColor;cursor:pointer}
.mk.WATCH{background:#f5a65b;color:#f5a65b}
.mk.WARNING{background:#f5835b;color:#f5835b}
.mk.EMERGENCY{background:#ff5252;color:#ff5252}
.mk .pulse{position:absolute;inset:-7px;border-radius:50%;border:2px solid currentColor;opacity:.8;animation:pulse 1.6s ease-out infinite}
@keyframes pulse{0%{transform:scale(.5);opacity:.9}100%{transform:scale(1.6);opacity:0}}
.maplibregl-popup-content{background:#171e2e;color:#eef1f7;font-size:12.5px;border-radius:8px;padding:10px 12px}
.maplibregl-popup-tip{border-top-color:#171e2e!important;border-bottom-color:#171e2e!important}
.maplibregl-ctrl-attrib{background:rgba(15,21,34,.7)!important;color:#9aa3b8!important}
.maplibregl-ctrl-attrib a{color:#9aa3b8!important}
@media(max-width:960px){.grid{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}}
</style>
</head>
<body>
<header>
  <div class="dot" id="statusDot"></div>
  <div><h1>Real-Time Wildfire Stream Monitor</h1>
       <div class="sub">Kafka &rarr; Streams &rarr; ML Detection &rarr; Alerts &rarr; Elasticsearch &rarr; 3D Globe</div></div>
  <div class="right" id="clock"></div>
</header>

<div class="grid">
  <div class="tile b"><div class="v" id="tPred">0</div><div class="k">Predictions</div></div>
  <div class="tile a"><div class="v" id="tEps">0.0</div><div class="k">Throughput (events/s)</div></div>
  <div class="tile o"><div class="v" id="tP95">&ndash;</div><div class="k">Latency p95 (ms)</div></div>
  <div class="tile r"><div class="v" id="tAlerts">0</div><div class="k">Fire Alerts</div></div>
</div>

<div class="cols">
  <div class="panel">
    <h2>Live fire locations &mdash; 3D satellite view</h2>
    <div class="map-tools">
      <label><input type="checkbox" id="follow" checked> auto-follow new alerts</label>
      <label><input type="checkbox" id="terrain" checked> 3D terrain</label>
      <div class="legend">
        <span><i style="background:#f5a65b"></i>WATCH</span>
        <span><i style="background:#f5835b"></i>WARNING</span>
        <span><i style="background:#ff5252"></i>EMERGENCY</span>
      </div>
    </div>
    <div id="map"></div>
  </div>
  <div>
    <div class="panel">
      <h2>Live alert feed (SSE)</h2>
      <div class="feed" id="feed"><div class="empty">Waiting for alerts&hellip;</div></div>
    </div>
    <div class="panel" style="margin-top:12px">
      <h2>Pipeline health</h2>
      <div class="kv"><span>Latency p50 / p95</span><span id="hLat">&ndash;</span></div>
      <div class="kv"><span>Windows closed</span><span id="hWin">&ndash;</span></div>
      <div class="kv"><span>DLQ (invalid events)</span><span id="hDlq">&ndash;</span></div>
      <div class="kv"><span>Late events</span><span id="hLate">&ndash;</span></div>
      <div class="kv"><span>Elasticsearch</span><span id="hEs">&ndash;</span></div>
      <div class="kv"><span>Model scored</span><span id="hModel">&ndash;</span></div>
      <div class="kv"><span>WATCH / WARNING / EMERGENCY</span><span id="hTiers">0 / 0 / 0</span></div>
    </div>
  </div>
</div>

<script>
const $=id=>document.getElementById(id);

/* ---------------- 3D globe (MapLibre, satellite + terrain) --------------- */
const map = new maplibregl.Map({
  container:'map',
  style:{
    version:8,
    sources:{
      sat:{type:'raster',tiles:['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
           tileSize:256,attribution:'Imagery &copy; Esri'},
      dem:{type:'raster-dem',tiles:['https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png'],
           encoding:'terrarium',tileSize:256,attribution:'Terrain: AWS/OpenMapTiles'}
    },
    layers:[
      {id:'bg',type:'background',paint:{'background-color':'#0a0f1a'}},
      {id:'sat',type:'raster',source:'sat',paint:{'raster-opacity':1}}
    ]
  },
  center:[-112,38.5], zoom:3.4, pitch:0, bearing:0,
  projection: {'type':'globe'},  /* 3D globe */
  attributionControl:{compact:true}
});
map.addControl(new maplibregl.NavigationControl({visualizePitch:true}),'top-right');
map.on('style.load',()=>{ if($('terrain').checked) map.setTerrain({source:'dem',exaggeration:1.4}); });
$('terrain').addEventListener('change',e=>{
  map.setTerrain(e.target.checked?{source:'dem',exaggeration:1.4}:null);
});

const markers=[];  // {id, marker, alert}
function flyToLoc(loc,zoom=6){
  if(!loc||loc.lat==null)return;
  map.flyTo({center:[loc.lon,loc.lat],zoom:Math.max(map.getZoom(),zoom),
             pitch:55,duration:1800,essential:true});
}
function addMarker(a){
  if(a.location==null||a.location.lat==null)return;
  const el=document.createElement('div');
  el.className='mk '+a.tier;
  el.innerHTML='<div class="pulse"></div>';
  el.style.position='relative';
  const m=new maplibregl.Marker({element:el})
    .setLngLat([a.location.lon,a.location.lat])
    .setPopup(new maplibregl.Popup({offset:14,closeButton:false}).setHTML(
      '<b style="color:#f5835b">'+a.tier+'</b> &middot; cell '+a.cell_id+
      '<br>p(fire) = <b>'+a.probability+'</b>'+
      '<br>ERC '+a.trigger?.erc+' &middot; wind '+a.trigger?.wind+' m/s &middot; fm100 '+a.trigger?.fm100+
      '<br><span style="color:#9aa3b8">'+new Date(a.ts).toLocaleTimeString()+'</span>'))
    .addTo(map);
  markers.push({id:a.alert_id,marker:m,el});
  el.addEventListener('click',()=>flyToLoc(a.location,7));
  while(markers.length>150){const old=markers.shift();old.marker.remove();}
}
map.on('load',async()=>{
  try{
    const backfill=await (await fetch('/api/alerts?limit=60')).json();
    backfill.slice().reverse().forEach(addMarker);
  }catch(e){}
});

/* --------------------------- SSE live stream ----------------------------- */
const es=new EventSource('/api/events');
es.addEventListener('alert',e=>{
  const a=JSON.parse(e.data);
  addMarker(a);
  if($('follow').checked && (a.tier!=='WATCH'||markers.length===1)) flyToLoc(a.location);
  pushFeed(a);
});
function pushFeed(a){
  const f=$('feed'); const empty=f.querySelector('.empty'); if(empty)empty.remove();
  const d=document.createElement('div'); d.className='row';
  d.innerHTML='<span class="badge '+a.tier+'">'+a.tier+'</span>'+
    '<span class="cell">'+a.cell_id+'</span><span class="msg">'+a.message+'</span>'+
    '<span class="time">p='+(a.probability??'?')+'</span>';
  d.onclick=()=>flyToLoc(a.location,7);
  f.prepend(d); while(f.children.length>120)f.lastChild.remove();
}

/* ------------------------------ stats poll ------------------------------- */
let hist=Array(60).fill(0), lastPred=0;
async function tick(){
  try{
    const st=await (await fetch('/api/stats')).json();
    $('tPred').textContent=(st.predictions_total||0).toLocaleString();
    $('tEps').textContent=(st.throughput_eps_recent||0).toFixed(1);
    $('tP95').textContent=st.latency_ms.p95??'&ndash;';
    $('tAlerts').textContent=(st.alerts_total||0).toLocaleString();
    $('hLat').textContent=(st.latency_ms.p50??'&ndash;')+' / '+(st.latency_ms.p95??'&ndash;')+' ms';
    $('hWin').textContent=st.streams?.windows_closed??'&ndash;';
    $('hDlq').textContent=st.streams?.invalid??'&ndash;';
    $('hLate').textContent=st.late_events??'&ndash;';
    $('hEs').textContent=st.es_up?'connected':'offline';
    $('hModel').textContent=st.detector&&st.detector.scored?st.detector.scored+' events':'&ndash;';
    const t=st.tier_counts||{};
    $('hTiers').textContent=(t.WATCH||0)+' / '+(t.WARNING||0)+' / '+(t.EMERGENCY||0);
    $('statusDot').classList.toggle('down',!st.es_up&&!(st.predictions_total>0));
    hist.push(st.predictions_total-lastPred); lastPred=st.predictions_total;
    if(hist.length>60)hist.shift();
  }catch(e){}
}
setInterval(()=>{$('clock').textContent=new Date().toLocaleTimeString()},1000);
setInterval(tick,2000); tick();
</script>
</body></html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=s.api_port, log_level="info")
