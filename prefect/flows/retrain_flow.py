import json
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional, List

from prefect import flow, task, get_run_logger

# -------------------------------------------------------------------
# Project paths
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATUS_PATH = PROJECT_ROOT / "monitoring" / "drift" / "status.json"


# -------------------------------------------------------------------
# Tasks
# -------------------------------------------------------------------
@task
def read_drift_status() -> Optional[dict]:
    logger = get_run_logger()

    if not STATUS_PATH.exists():
        logger.warning("status.json not found — skipping drift-based retrain")
        return None

    with open(STATUS_PATH) as f:
        status = json.load(f)

    logger.info(f"Drift status loaded: {status}")
    return status


@task
def retrain_model(models: List[str]) -> list:
    """Run specified models' training in-process where possible.

    Returns list of successfully trained model names.
    Falls back to running the existing `train.py --models ...` subprocess if imports fail.
    """
    logger = get_run_logger()
    logger.info(f"Starting model retraining for models: {models}")

    # Normalize available models
    available = {
        "randomforest": "RandomForest",
        "xgboost": "XGBoost",
        "lstm": "LSTM",
        "convlstm": "ConvLSTM",
    }

    if not models:
        requested = ["randomforest", "xgboost"]
    else:
        requested = [m.lower() for m in models]

    to_train = [available[m] for m in requested if m in available]
    skipped = [m for m in requested if m not in available]

    if skipped:
        logger.warning(f"Unknown model names skipped: {skipped}")

    trained = []

    # Ensure project root is importable so `import src...` works in-process
    sys.path.insert(0, str(PROJECT_ROOT))

    # Quick preflight import test for heavy deps — capture MemoryError early
    try:
        import numpy as _np  # type: ignore
        import scipy as _scipy  # type: ignore
        import sklearn as _skl  # type: ignore
    except MemoryError:
        logger.exception("MemoryError importing core scientific packages — aborting retrain to avoid child process crashes")
        # Re-raise so Prefect marks the task as failed with a clear error
        raise
    except Exception as e:
        # If core imports fail (not MemoryError), fallback to subprocess with PYTHONPATH set
        logger.warning("Core imports failed in-process (%s); falling back to subprocess", e)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        cmd = ["python", str(PROJECT_ROOT / "train.py"), "--models", *models]
        logger.info(f"Running subprocess fallback: {' '.join(cmd)} (with PYTHONPATH)")
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True, env=env)
        logger.info("Model retraining completed (subprocess fallback)")
        return to_train

    try:
        # Try to import project training utilities and run in-process
        from src.data_prep import DataProcessor
        import pandas as pd
        from src.config import data_config
        from src.trainer import WildfireTrainer
    except Exception:
        # Fall back to subprocess if project imports fail
        logger.exception("In-process project imports failed; falling back to subprocess call")
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        cmd = ["python", str(PROJECT_ROOT / "train.py"), "--models", *models]
        logger.info(f"Running subprocess fallback: {' '.join(cmd)} (with PYTHONPATH)")
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True, env=env)
        logger.info("Model retraining completed (subprocess fallback)")
        return to_train

    # Prepare data
    proc = DataProcessor()
    proc.prepare_tabular()

    train_df = pd.read_parquet("Data/processed/train.parquet")
    val_df = pd.read_parquet("Data/processed/val.parquet")
    test_df = pd.read_parquet("Data/processed/test.parquet")

    X_train = train_df[data_config.features]
    y_train = train_df["Wildfire"]

    X_val = val_df[data_config.features]
    y_val = val_df["Wildfire"]

    X_test = test_df[data_config.features]
    y_test = test_df["Wildfire"]

    trainer = WildfireTrainer()

    for m in to_train:
        try:
            if m == "RandomForest":
                logger.info("Training RandomForest")
                trainer.train_tabular(
                    "RandomForest",
                    X_train, y_train,
                    X_val, y_val,
                    X_test, y_test,
                )

            elif m == "XGBoost":
                logger.info("Training XGBoost")
                trainer.train_tabular(
                    "XGBoost",
                    X_train, y_train,
                    X_val, y_val,
                    X_test, y_test,
                )

            elif m == "LSTM":
                logger.info("Training LSTM")
                trainer.train_dl("LSTM", sample_ratio=1.0)

            elif m == "ConvLSTM":
                logger.info("Training ConvLSTM")
                trainer.train_dl("ConvLSTM", sample_ratio=1.0)

            trained.append(m)
        except MemoryError:
            logger.exception(f"MemoryError while training {m}; skipping and continuing")
        except Exception:
            logger.exception(f"Error while training {m}; skipping and continuing")

    logger.info(f"Model retraining finished. Trained: {trained}")
    return trained


@task
def register_model() -> bool:
    logger = get_run_logger()
    logger.info("Registering model in MLflow...")

    script_path = PROJECT_ROOT / "register_model.py"
    if not script_path.exists():
        logger.warning(f"{script_path} not found — skipping model registration")
        return False

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    try:
        subprocess.run(
            ["python", str(script_path)],
            cwd=PROJECT_ROOT,
            check=True,
            env=env,
        )
        logger.info("Model registered in MLflow")
        return True
    except subprocess.CalledProcessError:
        logger.exception("register_model script failed — continuing without registration")
        return False


@task
def dvc_version_data():
    logger = get_run_logger()
    logger.info("Versioning data & artifacts with DVC...")

    subprocess.run(["dvc", "add", "Data"], cwd=PROJECT_ROOT, check=False)
    subprocess.run(["dvc", "add", "models"], cwd=PROJECT_ROOT, check=False)
    subprocess.run(
        ["dvc", "commit", "-m", "Retrain after drift detected"],
        cwd=PROJECT_ROOT,
        check=False,
    )

    logger.info("DVC versioning complete")


# -------------------------------------------------------------------
# Flow
# -------------------------------------------------------------------
@flow(name="wildfire-retraining-pipeline")
def retrain_pipeline(
    force_retrain: bool = False,
    models: List[str] = ["RandomForest", "XGBoost"],
):
    """
    force_retrain:
        - False: retrain only if drift trigger is true
        - True : retrain regardless of drift status

    models:
        Models to retrain (safe default excludes DL)
    """
    logger = get_run_logger()

    if force_retrain:
        logger.warning("Manual retrain triggered (force_retrain=True)")
        trained = retrain_model(models)
        if trained and len(trained) > 0:
            register_model()
            dvc_version_data()
        else:
            logger.warning("No models trained successfully — skipping register & DVC versioning")
        return

    status = read_drift_status()

    if not status:
        logger.info("No drift status available — exiting pipeline")
        return

    if status.get("trigger_retrain"):
        logger.info("Drift threshold exceeded — retraining triggered")
        trained = retrain_model(models)
        if trained and len(trained) > 0:
            register_model()
            dvc_version_data()
        else:
            logger.warning("No models trained successfully — skipping register & DVC versioning")
    else:
        logger.info("No drift detected — skipping retraining")


# -------------------------------------------------------------------
# Manual execution
# -------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    force = "--force" in sys.argv

    retrain_pipeline(
        force_retrain=force,
        models=["RandomForest", "XGBoost"],  # SAFE DEFAULT
    )
