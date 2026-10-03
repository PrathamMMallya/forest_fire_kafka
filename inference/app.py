from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
try:
    # when running as package (python -m uvicorn inference.app:app)
    from inference.schemas import WildfireRequest, WildfireResponse
    from inference.model_loader import WildfireModel
except Exception:
    # when running from the inference directory (python -m uvicorn app:app)
    from schemas import WildfireRequest, WildfireResponse
    from model_loader import WildfireModel

import pandas as pd
from pathlib import Path
import json
import logging
import atexit
import signal
import sys
import traceback

logger = logging.getLogger("inference")
logger.setLevel(logging.DEBUG)

# register an exit handler to help diagnose unexpected shutdowns
def _on_exit():
    try:
        sys.stderr.write("INFERENCE: atexit handler called\n")
        traceback.print_stack(file=sys.stderr)
    except Exception:
        pass

atexit.register(_on_exit)

# register basic signal handlers to capture termination signals
def _sig_handler(sig, frame):
    try:
        sys.stderr.write(f"INFERENCE: received signal {sig} ({signal.Signals(sig).name})\n")
        traceback.print_stack(frame)
    except Exception:
        pass

for s in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(s, _sig_handler)
    except Exception:
        # some platforms may not support all signals
        pass

app = FastAPI(
    title="Wildfire Risk Inference API",
    version="1.0.0",
    description="Production inference service for wildfire risk prediction"
)

# startup/shutdown diagnostics
@app.on_event("startup")
def _startup_event():
    try:
        sys.stderr.write("INFERENCE: startup event - application starting up\n")
    except Exception:
        pass

@app.on_event("shutdown")
def _shutdown_event():
    try:
        sys.stderr.write("INFERENCE: shutdown event - application is shutting down\n")
    except Exception:
        pass

# Allow cross-origin requests from frontend (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

model = WildfireModel()

# -----------------------------
# PATH FOR DRIFT MONITORING
# -----------------------------
CURRENT_DIR = Path("monitoring/current")
CURRENT_DIR.mkdir(parents=True, exist_ok=True)
CURRENT_PATH = CURRENT_DIR / "current.parquet"


@app.get("/")
def health():
    return {"status": "healthy"}


@app.post("/predict", response_model=WildfireResponse)
def predict(req: WildfireRequest):
    try:
        # features come as dict (correct)
        features_dict = req.features

        prob, pred = model.predict(features_dict)

        # -----------------------------
        # SAVE LIVE DATA FOR EVIDENTLY
        # -----------------------------
        from datetime import datetime

        row = {
            **features_dict,
            "prediction": pred,
            "probability": prob,
            "ts": datetime.utcnow().isoformat(),
        }

        new_df = pd.DataFrame([row])

        if CURRENT_PATH.exists():
            old_df = pd.read_parquet(CURRENT_PATH)
            new_df = pd.concat([old_df, new_df], ignore_index=True)

        new_df.to_parquet(CURRENT_PATH, index=False)

        model_name = getattr(model, "model_name", model.model.__class__.__name__)

        return WildfireResponse(
            wildfire_probability=prob,
            wildfire_prediction=pred,
            model_name=model_name,
            confidence=prob,
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/monitoring/current")
def get_current(n: int = 100):
    if not CURRENT_PATH.exists():
        return []
    df = pd.read_parquet(CURRENT_PATH)
    return df.tail(n).to_dict(orient="records")


@app.get("/monitoring/drift/status")
def get_drift_status():
    p = Path("monitoring/drift/status.json")
    if not p.exists():
        raise HTTPException(status_code=404, detail="No status file")
    return json.loads(p.read_text())


@app.get("/monitoring/drift/metrics")
def get_drift_metrics():
    p = Path("monitoring/drift/metrics.json")
    if not p.exists():
        raise HTTPException(status_code=404, detail="No metrics file")
    return json.loads(p.read_text())


@app.get("/monitoring/drift/report")
def get_drift_report():
    p = Path("monitoring/drift/report.html")
    if not p.exists():
        raise HTTPException(status_code=404, detail="No report file")
    # Return as FileResponse so large reports stream and do not cause memory issues
    return FileResponse(path=str(p.resolve()), media_type='text/html')


@app.get("/model")
def get_model():
    return {"model_name": getattr(model, "model_name", model.model.__class__.__name__)}


# -----------------------------
# MODELS / MLRun discovery
# -----------------------------
@app.get("/models")
def list_models():
    base = Path("mlruns")
    results = []
    if not base.exists():
        return results
    for experiment in base.iterdir():
        if not experiment.is_dir():
            continue
        for run in experiment.iterdir():
            if not run.is_dir():
                continue
            meta = {}
            mfile = run / "meta.yaml"
            if mfile.exists():
                try:
                    for ln in mfile.read_text().splitlines():
                        if ':' in ln:
                            k,v = ln.split(':',1)
                            meta[k.strip()] = v.strip().strip("'")
                except Exception:
                    pass
            metrics = {}
            metrics_dir = run / "metrics"
            if metrics_dir.exists():
                for m in metrics_dir.iterdir():
                    try:
                        # grab the last recorded value for summary
                        txt = m.read_text().strip().splitlines()[-1]
                        parts = txt.split()
                        if len(parts) >= 2:
                            metrics[m.name] = float(parts[1])
                    except Exception:
                        pass
            artifacts = []
            art_dir = run / "artifacts"
            if art_dir.exists():
                for a in art_dir.rglob('*'):
                    if a.is_file():
                        artifacts.append(str(a.relative_to(run)))
            results.append({
                "run_id": meta.get('run_id', run.name),
                "run_name": meta.get('run_name', ''),
                "experiment_id": meta.get('experiment_id', experiment.name),
                "start_time": int(meta.get('start_time', 0)),
                "metrics": metrics,
                "artifacts": artifacts
            })
    # sort by start_time desc
    results.sort(key=lambda x: x.get('start_time',0), reverse=True)
    return results


@app.get("/models/{run_id}/metrics")
def get_run_metrics(run_id: str):
    base = Path("mlruns")
    for experiment in base.iterdir():
        if not experiment.is_dir():
            continue
        run = experiment / run_id
        if run.exists() and run.is_dir():
            metrics_dir = run / "metrics"
            out = {}
            if metrics_dir.exists():
                for m in metrics_dir.iterdir():
                    try:
                        lines = [ln.strip() for ln in m.read_text().splitlines() if ln.strip()]
                        pts = []
                        for ln in lines:
                            parts = ln.split()
                            if len(parts) >= 2:
                                ts = int(parts[0])
                                val = float(parts[1])
                                pts.append([ts, val])
                        out[m.name] = pts
                    except Exception:
                        out[m.name] = []
            return out
    raise HTTPException(status_code=404, detail="Run not found")
    # sort by start_time desc
    results.sort(key=lambda x: x.get('start_time',0), reverse=True)
    return results


@app.get("/models/{run_id}")
def get_model_run(run_id: str):
    base = Path("mlruns")
    for experiment in base.iterdir():
        if not experiment.is_dir():
            continue
        run = experiment / run_id
        if run.exists() and run.is_dir():
            meta = {}
            mfile = run / "meta.yaml"
            if mfile.exists():
                try:
                    for ln in mfile.read_text().splitlines():
                        if ':' in ln:
                            k,v = ln.split(':',1)
                            meta[k.strip()] = v.strip().strip("'")
                except Exception:
                    pass
            metrics = {}
            metrics_dir = run / "metrics"
            if metrics_dir.exists():
                for m in metrics_dir.iterdir():
                    try:
                        txt = m.read_text().strip().splitlines()[-1]
                        parts = txt.split()
                        if len(parts) >= 2:
                            metrics[m.name] = float(parts[1])
                    except Exception:
                        pass
            artifacts = []
            art_dir = run / "artifacts"
            if art_dir.exists():
                for a in art_dir.rglob('*'):
                    if a.is_file():
                        artifacts.append(str(a.relative_to(run)))
            return {
                "run_id": meta.get('run_id', run.name),
                "run_name": meta.get('run_name', ''),
                "experiment_id": meta.get('experiment_id', experiment.name),
                "start_time": int(meta.get('start_time', 0)),
                "metrics": metrics,
                "artifacts": artifacts
            }
    raise HTTPException(status_code=404, detail="Run not found")


# -----------------------------
# RETRAINING endpoints
# -----------------------------
from fastapi import BackgroundTasks
import time

RETRAIN_DIR = Path("monitoring/retrain")
RETRAIN_DIR.mkdir(parents=True, exist_ok=True)
RETRAIN_STATUS = RETRAIN_DIR / "status.json"

def _do_retrain(force: bool):
    # NOTE: This is a lightweight placeholder - replace with your real pipeline trigger (Prefect/MLflow/DVC)
    import json
    now = int(time.time() * 1000)
    s = {"status":"running","started_at":now,"force":bool(force)}
    RETRAIN_STATUS.write_text(json.dumps(s))
    # simulate run
    time.sleep(3)
    finished = {"status":"completed","finished_at":int(time.time()*1000),"force":bool(force),"metrics":{"auc":0.66}}
    RETRAIN_STATUS.write_text(json.dumps(finished))

@app.post("/retrain")
def trigger_retrain(force: bool = False, background_tasks: BackgroundTasks = None):
    # start background retrain
    if background_tasks is not None:
        background_tasks.add_task(_do_retrain, force)
    else:
        # fallback synchronous
        _do_retrain(force)
    return {"status":"accepted","force":force}

@app.get("/retrain/status")
def get_retrain_status():
    if not RETRAIN_STATUS.exists():
        return {"status":"idle"}
    import json
    return json.loads(RETRAIN_STATUS.read_text())
