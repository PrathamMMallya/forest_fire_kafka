import logging
import uuid
from pathlib import Path
import joblib

from src.utils import get_logger, save_artifact


def test_get_logger():
    logger = get_logger("test_logger")
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO


def test_save_artifact():
    obj = {"a": 1, "b": "x"}
    filename = f"test_artifact_{uuid.uuid4().hex}.joblib"

    save_artifact(obj, filename)

    p = Path("artifacts") / filename
    assert p.exists()

    loaded = joblib.load(p)
    assert loaded == obj

    # Cleanup
    p.unlink()
    try:
        p.parent.rmdir()
    except OSError:
        pass
