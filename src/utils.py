# src/utils.py
import logging
import joblib
from pathlib import Path

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - INFO - %(message)s'))
    logger.addHandler(handler)
    return logger

def save_artifact(obj, filename: str):
    path = Path("artifacts") / filename
    path.parent.mkdir(exist_ok=True)
    joblib.dump(obj, path)