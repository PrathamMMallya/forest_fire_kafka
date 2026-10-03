import json
import pandas as pd
from pathlib import Path

from evidently.legacy.report import Report
from evidently.legacy.metric_preset import DataDriftPreset

from monitoring.config import *

# ---------------- Paths ----------------

REFERENCE_PATH = Path("monitoring/reference/reference_v1.parquet")
CURRENT_PATH = Path("monitoring/current/current.parquet")

OUTPUT_DIR = Path("monitoring/drift")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Drift Runner ----------------

def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns that Evidently tries to auto-detect
    and crashes on if partially present.
    """
    reserved = {"prediction", "target", "label"}
    return df.drop(columns=[c for c in reserved if c in df.columns])


def _align_columns(reference: pd.DataFrame, current: pd.DataFrame) -> tuple:
    """
    Ensure both dataframes have the same columns.
    Only keep columns that exist in both datasets.
    """
    ref_cols = set(reference.columns)
    cur_cols = set(current.columns)
    
    # Find common columns
    common_cols = ref_cols & cur_cols
    
    # Find missing columns
    ref_only = ref_cols - cur_cols
    cur_only = cur_cols - ref_cols
    
    if ref_only:
        print(f"⚠️ Columns only in reference: {ref_only}")
    if cur_only:
        print(f"⚠️ Columns only in current: {cur_only}")
    
    if not common_cols:
        raise ValueError("No common columns between reference and current datasets!")
    
    # Keep only common columns in both dataframes
    common_cols_list = sorted(list(common_cols))
    print(f"✓ Using {len(common_cols_list)} common columns for drift detection")
    
    return reference[common_cols_list], current[common_cols_list]


def run_drift():
    # ---- Safety checks ----
    if not CURRENT_PATH.exists():
        print("❌ current.parquet not found — skipping drift")
        return

    current = pd.read_parquet(CURRENT_PATH)
    if len(current) < MIN_SAMPLES:
        print(f"❌ Not enough samples ({len(current)}/{MIN_SAMPLES}) — skipping drift")
        return

    reference = pd.read_parquet(REFERENCE_PATH)

    # ---- CRITICAL: sanitize data ----
    reference = _sanitize(reference)
    current = _sanitize(current)
    
    # ---- Align columns ----
    try:
        reference, current = _align_columns(reference, current)
    except ValueError as e:
        print(f"❌ {e}")
        return

    # ---- Build drift report ----
    report = Report(metrics=[
        DataDriftPreset()
    ])

    try:
        report.run(
            reference_data=reference,
            current_data=current
        )
    except Exception as e:
        print(f"❌ Error running drift report: {e}")
        return

    # ---- Save HTML report ----
    try:
        report.save_html(str(OUTPUT_DIR / "report.html"))
        print(f"✓ HTML report saved to {OUTPUT_DIR / 'report.html'}")
    except Exception as e:
        print(f"⚠️ Could not save HTML report: {e}")

    # ---- Parse results ----
    try:
        result = report.as_dict()

        # Find the drift metrics in the results
        drift_result = None
        for m in result.get("metrics", []):
            if "DataDrift" in m.get("metric", ""):
                drift_result = m["result"]
                break
        
        if drift_result is None:
            print("⚠️ Could not find drift results in report")
            print(f"Available metrics: {[m.get('metric') for m in result.get('metrics', [])]}")
            return

        dataset_drift = drift_result.get("dataset_drift", False)
        drift_score = drift_result.get("share_of_drifted_columns", 0)

        status = {
            "dataset_drift": dataset_drift,
            "drift_score": drift_score,
            "trigger_retrain": drift_score >= DRIFT_THRESHOLD
        }

        # ---- Save machine-readable outputs ----
        with open(OUTPUT_DIR / "status.json", "w") as f:
            json.dump(status, f, indent=2)

        with open(OUTPUT_DIR / "metrics.json", "w") as f:
            json.dump(result, f, indent=2)

        print("✅ Drift report generated successfully")
        print(f"📊 Dataset drift: {dataset_drift}, Score: {drift_score:.2%}")
        
        if status["trigger_retrain"]:
            print(f"⚠️ Drift score {drift_score:.2%} exceeds threshold {DRIFT_THRESHOLD:.2%} - Retrain recommended!")
        
    except Exception as e:
        print(f"⚠️ Error processing results: {e}")


# ---------------- Entrypoint ----------------

if __name__ == "__main__":
    run_drift()