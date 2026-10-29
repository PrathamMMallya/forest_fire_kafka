"""Train the streaming detector model on the ISRO MOSDAC + Open-Meteo dataset.

Key design guarantees (addressing audit findings):
1. Spatial Group Split (`GroupShuffleSplit` by H3 resolution-3 `region_id`):
   All observations within a ~60 km H3 region go strictly into train, val, or test.
   Zero location or multi-pass twin leakage between train and evaluation splits.
2. Validation split is actively used for hyperparameter selection and decision
   threshold calibration.
3. Saves feature importances and both validation and test metrics to
   `artifacts/stream_model_meta.json`.

Produces the serving artifacts consumed by streaming/detector.py:
    artifacts/best_RandomForest.pkl        - scikit-learn RandomForest model
    artifacts/preprocessing_artifacts.pkl  - {medians, scaler, feature_names, threshold}
    artifacts/stream_model_meta.json       - validation & test metrics + feature importances

Usage: python train_stream_model.py
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GroupShuffleSplit
from sklearn.preprocessing import RobustScaler

from streaming.common import FEATURES, region_id


def find_csv() -> Path:
    for p in (
        Path("Data/MOSDAC_Wildfire_Dataset.csv"),
        Path("eda/data/eda_sample.csv"),
        Path("Data/Wildfire_Dataset.csv"),
    ):
        if p.exists():
            return p
    raise SystemExit("No dataset found - run `python -m eda.generate_sample` first.")


def main():
    csv = find_csv()
    print(f"Loading training data from {csv}...")
    df = pd.read_csv(csv, usecols=FEATURES + ["Wildfire"])
    df = df.replace(32767, np.nan)
    y = (df["Wildfire"] == "Yes").astype(int).to_numpy()
    X = df[FEATURES].to_numpy(dtype="float64")

    # Compute H3 resolution-3 spatial group IDs so train, val, and test never share regions
    groups = np.array([
        region_id(float(lat), float(lon))
        for lat, lon in zip(df["latitude"], df["longitude"])
    ])

    gss_outer = GroupShuffleSplit(n_splits=1, test_size=0.30, random_state=42)
    tr_idx, tmp_idx = next(gss_outer.split(X, y, groups=groups))

    X_tr, y_tr, g_tr = X[tr_idx], y[tr_idx], groups[tr_idx]
    X_tmp, y_tmp, g_tmp = X[tmp_idx], y[tmp_idx], groups[tmp_idx]

    gss_inner = GroupShuffleSplit(n_splits=1, test_size=0.50, random_state=42)
    val_rel_idx, te_rel_idx = next(gss_inner.split(X_tmp, y_tmp, groups=g_tmp))
    X_val, y_val = X_tmp[val_rel_idx], y_tmp[val_rel_idx]
    X_te, y_te = X_tmp[te_rel_idx], y_tmp[te_rel_idx]

    # Learn imputation medians strictly on training split (no test leakage)
    medians = np.nanmedian(X_tr, axis=0)
    for arr in (X_tr, X_val, X_te):
        nan_pos = np.where(np.isnan(arr))
        if len(nan_pos[0]) > 0:
            arr[nan_pos[0], nan_pos[1]] = np.take(medians, nan_pos[1])

    scaler = RobustScaler().fit(X_tr)
    X_tr_s = scaler.transform(X_tr)
    X_val_s = scaler.transform(X_val)
    X_te_s = scaler.transform(X_te)

    print(
        f"Spatial Group Split -> Train: {len(X_tr_s)} ({len(set(g_tr))} H3 regions) | "
        f"Val: {len(X_val_s)} | Test: {len(X_te_s)}"
    )

    # Use validation split to select best tree depth / regularization
    best_model = None
    best_val_f1 = -1.0
    best_cfg = {}
    for depth in (6, 8, 10):
        for min_leaf in (4, 8, 12):
            cand = RandomForestClassifier(
                n_estimators=150,
                max_depth=depth,
                min_samples_leaf=min_leaf,
                max_features="sqrt",
                n_jobs=-1,
                class_weight="balanced_subsample",
                random_state=42,
            )
            cand.fit(X_tr_s, y_tr)
            val_prob = cand.predict_proba(X_val_s)[:, 1]
            val_f1 = float(f1_score(y_val, (val_prob >= 0.5).astype(int)))
            if val_f1 > best_val_f1:
                best_val_f1 = val_f1
                best_model = cand
                best_cfg = {"max_depth": depth, "min_samples_leaf": min_leaf}

    model = best_model
    # Calibrate probability threshold on validation set
    val_probs = model.predict_proba(X_val_s)[:, 1]
    best_thresh = 0.50
    for th in np.linspace(0.35, 0.65, 31):
        score = float(f1_score(y_val, (val_probs >= th).astype(int)))
        if score > best_val_f1:
            best_val_f1 = score
            best_thresh = round(float(th), 2)

    # Evaluate on held-out spatial test set
    prob = model.predict_proba(X_te_s)[:, 1]
    pred = (prob >= best_thresh).astype(int)

    importances = {
        feat: round(float(imp), 4)
        for feat, imp in sorted(
            zip(FEATURES, model.feature_importances_),
            key=lambda kv: kv[1],
            reverse=True,
        )
    }

    metrics = {
        "model": "RandomForest_MOSDAC_OpenMeteo",
        "split_strategy": "GroupShuffleSplit_by_H3_res3_region",
        "best_hyperparams": best_cfg,
        "decision_threshold": best_thresh,
        "n_train": int(len(X_tr_s)),
        "n_val": int(len(X_val_s)),
        "n_test": int(len(X_te_s)),
        "n_features": len(FEATURES),
        "val_f1": round(best_val_f1, 4),
        "test_accuracy": round(float(accuracy_score(y_te, pred)), 4),
        "test_precision": round(float(precision_score(y_te, pred)), 4),
        "test_recall": round(float(recall_score(y_te, pred)), 4),
        "test_f1": round(float(f1_score(y_te, pred)), 4),
        "test_roc_auc": round(float(roc_auc_score(y_te, prob)), 4),
        "test_pr_auc": round(float(average_precision_score(y_te, prob)), 4),
        "feature_importances": importances,
        "trained_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "dataset": str(csv),
    }

    art = Path("artifacts")
    art.mkdir(exist_ok=True)
    joblib.dump(model, art / "best_RandomForest.pkl")
    joblib.dump(
        {
            "medians": medians.tolist(),
            "scaler": scaler,
            "feature_names": FEATURES,
            "threshold": best_thresh,
        },
        art / "preprocessing_artifacts.pkl",
    )
    (art / "stream_model_meta.json").write_text(json.dumps(metrics, indent=2))
    print("\n--- Model Evaluation Metrics (Held-Out H3 Spatial Regions) ---")
    print(json.dumps(metrics, indent=2))
    print(f"\nArtifacts successfully saved to: {art.resolve()}")


if __name__ == "__main__":
    main()
