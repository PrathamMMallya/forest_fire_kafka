"""End-to-end pipeline performance evaluation.

Runs a timed producer burst through the live pipeline, consumes predictions
from `wildfire.predictions`, and reports:

  * end-to-end latency (producer ingest_ts -> detector publish) p50/p95/p99
  * sustained throughput (events/s) per stage
  * detection quality vs embedded labels: precision / recall / F1 / ROC-AUC / PR-AUC
  * alert breakdown by tier, DLQ + late-event counts
  * Kafka consumer lag per stage (if the streams/detector groups are active)

Writes evaluation/report.json and prints a formatted table.

Usage:  python -m streaming.evaluate --events 5000 --eps 200
"""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer

from streaming.common import Settings, from_json, get_logger, kafka_consumer_conf

log = get_logger("evaluate")


def percentile(sorted_vals: list[float], p: float):
    if not sorted_vals:
        return None
    return round(sorted_vals[min(int(len(sorted_vals) * p), len(sorted_vals) - 1)], 1)


def quality(rows: list[dict]) -> dict:
    y = [1 if r.get("label") == "Yes" else 0 for r in rows]
    if sum(y) == 0 or sum(y) == len(y):
        return {"note": "labels degenerate - quality metrics skipped"}
    yhat = [1 if r.get("prediction", 0) >= 0.5 else 0 for r in rows]
    p = [r["probability"] for r in rows]
    tp = sum(1 for a, b in zip(y, yhat) if a == 1 and b == 1)
    fp = sum(1 for a, b in zip(y, yhat) if a == 0 and b == 1)
    fn = sum(1 for a, b in zip(y, yhat) if a == 1 and b == 0)
    tn = sum(1 for a, b in zip(y, yhat) if a == 0 and b == 0)
    try:
        from sklearn.metrics import roc_auc_score, average_precision_score
        auc = round(float(roc_auc_score(y, p)), 4)
        pr_auc = round(float(average_precision_score(y, p)), 4)
    except Exception:
        auc = pr_auc = None
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return {
        "n": len(y), "positives": sum(y), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
        "roc_auc": auc, "pr_auc": pr_auc,
        "threshold": 0.5,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=5000)
    ap.add_argument("--eps", type=float, default=200.0)
    ap.add_argument("--timeout", type=float, default=90.0,
                    help="max seconds to wait for all predictions")
    ap.add_argument("--speed", type=float, default=60.0)
    args = ap.parse_args()
    s = Settings()

    log.info("burst: %d events at %.0f eps (replay clock speed %.0fx)",
             args.events, args.eps, args.speed)
    prod = subprocess.Popen(
        ["python", "-m", "streaming.producer", "--mode", "replay",
         "--eps", str(args.eps), "--speed", str(args.speed),
         "--limit", str(args.events), "--clock", "synthetic"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    consumer = Consumer(kafka_consumer_conf(f"eval-{int(time.time())}", s))
    consumer.subscribe([s.topic_predictions])

    rows: list[dict] = []
    t_start = time.time()
    first_pred_t = None
    try:
        while time.time() - t_start < args.timeout:
            msgs = consumer.consume(num_messages=500, timeout=0.5)
            for m in msgs:
                if m.error():
                    continue
                rows.append(from_json(m.value()))
                if first_pred_t is None:
                    first_pred_t = time.time()
                if len(rows) >= args.events:
                    break
            if len(rows) >= args.events:
                break
    except KeyboardInterrupt:
        pass
    finally:
        prod.terminate()
        consumer.close()

    wall = (time.time() - t_start) or 1e-9
    if not rows:
        raise SystemExit("no predictions received - is the pipeline running?")

    enriched = [e for e in (_lat(r) for r in rows) if e is not None]
    lats = sorted(e["e2e_ms"] for e in enriched)
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "config": {"events_sent": args.events, "target_eps": args.eps},
        "latency_ms": {
            "p50": percentile(lats, 0.50), "p95": percentile(lats, 0.95),
            "p99": percentile(lats, 0.99), "mean": round(sum(lats) / len(lats), 1)
            if lats else None, "n": len(lats),
        },
        "throughput": {
            "predictions_received": len(rows),
            "wall_seconds": round(wall, 2),
            "events_per_second_observed": round(len(rows) / wall, 1),
            "time_to_first_prediction_s": round(first_pred_t - t_start, 2)
            if first_pred_t else None,
        },
        "detection_quality": quality(rows),
        "alerts": _alert_stats(rows, s),
        "data_quality": {
            "late_events": sum(1 for r in rows if r.get("late")),
        },
    }
    out = Path("evaluation")
    out.mkdir(exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2))

    print("\n" + "=" * 62)
    print(" WILDFIRE STREAMING PIPELINE - PERFORMANCE REPORT")
    print("=" * 62)
    lat = report["latency_ms"]
    print(f" End-to-end latency   p50={lat['p50']}ms  p95={lat['p95']}ms  "
          f"p99={lat['p99']}ms  (n={lat['n']})")
    thr = report["throughput"]
    print(f" Throughput           {thr['events_per_second_observed']} events/s "
          f"({thr['predictions_received']} preds in {thr['wall_seconds']}s, "
          f"first pred in {thr['time_to_first_prediction_s']}s)")
    q = report["detection_quality"]
    if "precision" in q:
        print(f" Detection quality    precision={q['precision']}  recall={q['recall']}  "
              f"f1={q['f1']}  auc={q['roc_auc']}  pr_auc={q['pr_auc']}")
        print(f" Confusion            TP={q['tp']} FP={q['fp']} FN={q['fn']} TN={q['tn']}")
    tc = report["alerts"]["tier_counts"]
    print(f" Alerts               WATCH={tc.get('WATCH', 0)}"
          f"  WARNING={tc.get('WARNING', 0)}"
          f"  EMERGENCY={tc.get('EMERGENCY', 0)}")
    print(f" Data quality         late={report['data_quality']['late_events']}")
    print(f" Report saved -> evaluation/report.json")
    print("=" * 62)


def _lat(r: dict):
    """Compute end-to-end latency in ms (ingest -> detector publish)."""
    try:
        t0 = datetime.fromisoformat(r["ingest_ts"]).timestamp()
        return {**r, "e2e_ms": (r["ts"] / 1000 - t0) * 1000}
    except (KeyError, ValueError, TypeError, OSError):
        return None


def _alert_stats(rows: list[dict], s: Settings) -> dict:
    tier_counts = {"WATCH": 0, "WARNING": 0, "EMERGENCY": 0, "NONE": 0}
    for r in rows:
        tier_counts[r.get("tier", "NONE")] = tier_counts.get(r.get("tier", "NONE"), 0) + 1
    # alert topic tail (approximate window: whatever is retained)
    consumer = Consumer(kafka_consumer_conf(f"eval-alerts-{int(time.time())}", s))
    consumer.subscribe([s.topic_alerts])
    alerts = 0
    t0 = time.time()
    try:
        while time.time() - t0 < 5:
            msgs = consumer.consume(num_messages=500, timeout=0.5)
            alerts += sum(1 for m in msgs if not m.error())
    except Exception:
        pass
    consumer.close()
    return {"tier_counts": tier_counts, "alerts_on_topic": alerts}


if __name__ == "__main__":
    main()
