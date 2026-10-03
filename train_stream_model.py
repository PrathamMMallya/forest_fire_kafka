"""Train the streaming detector model and save serving artifacts.

Produces the exact artifact contract consumed by inference/model_loader.py
and streaming/detector.py:
    artifacts/best_RandomForest.pkl        - sklearn model (predict_proba)
    artifacts/preprocessing_artifacts.pkl  - {medians, scaler, feature_names}
    artifacts/stream_model_meta.json       - test metrics for the dashboard

Data: Data/Wildfire_Dataset.csv when present, else eda/data/eda_sample.csv
(schema-identical calibrated sample - regenerate with eda/generate_sample.py).

Usage:  python train_stream_model.py
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, average_precision_score, f1_score,
                             precision_score, recall_score, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler

FEATURES = [
    "latitude", "longitude", "pr", "rmax", "rmin", "sph", "srad",
    "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
]


def find_csv() -> Path:
    for p in (Path("Data/Wildfire_Dataset.csv"), Path("eda/data/eda_sample.csv")):
        if p.exists():
            return p
    raise SystemExit("no dataset found - run eda/generate_sample.py first")


def main():
    csv = find_csv()
    print(f"loading {csv}")
    df = pd.read_csv(csv, usecols=FEATURES + ["Wildfire"])
    df = df.replace(32767, np.nan)                      # gridMET sentinel
    y = (df["Wildfire"] == "Yes").astype(int).to_numpy()
    X = df[FEATURES].to_numpy(dtype="float64")

    medians = np.nanmedian(X, axis=0)
    idx = np.where(np.isnan(X))
    X[idx[0], idx[1]] = np.take(medians, idx[1])

    X_tr, X_tmp, y_tr, y_tmp = train_test_split(X, y, test_size=0.3,
                                                stratify=y, random_state=42)
    X_val, X_te, y_val, y_te = train_test_split(X_tmp, y_tmp, test_size=0.5,
                                                stratify=y_tmp, random_state=42)

    scaler = RobustScaler().fit(X_tr)
    X_tr_s = scaler.transform(X_tr)
    X_te_s = scaler.transform(X_te)

    print(f"train={X_tr_s.shape} positives={y_tr.mean() * 100:.2f}%")
    model = RandomForestClassifier(
        n_estimators=150, max_depth=10, n_jobs=-1,
        class_weight="balanced", random_state=42,
    )
    model.fit(X_tr_s, y_tr)

    prob = model.predict_proba(X_te_s)[:, 1]
    pred = (prob >= 0.5).astype(int)
    metrics = {
        "model": "RandomForest",
        "n_train": int(len(X_tr_s)), "n_test": int(len(X_te_s)),
        "test_accuracy": round(float(accuracy_score(y_te, pred)), 4),
        "test_precision": round(float(precision_score(y_te, pred)), 4),
        "test_recall": round(float(recall_score(y_te, pred)), 4),
        "test_f1": round(float(f1_score(y_te, pred)), 4),
        "test_roc_auc": round(float(roc_auc_score(y_te, prob)), 4),
        "test_pr_auc": round(float(average_precision_score(y_te, prob)), 4),
        "trained_at": pd.Timestamp.utcnow().isoformat(),
        "dataset": str(csv),
    }

    art = Path("artifacts")
    art.mkdir(exist_ok=True)
    joblib.dump(model, art / "best_RandomForest.pkl")
    joblib.dump({"medians": medians, "scaler": scaler, "feature_names": FEATURES},
                art / "preprocessing_artifacts.pkl")
    (art / "stream_model_meta.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))
    print("artifacts saved to artifacts/")


if __name__ == "__main__":
    main()
