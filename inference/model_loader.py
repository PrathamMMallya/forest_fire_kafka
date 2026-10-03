from pyexpat import features
import joblib
import numpy as np
from pathlib import Path

ARTIFACTS_DIR = Path("artifacts")

class WildfireModel:
    def __init__(self):
        import sys, traceback
        model_path = ARTIFACTS_DIR / "best_RandomForest.pkl"
        try:
            sys.stderr.write(f"WildfireModel: loading model from {model_path}\n")
            self.model = joblib.load(model_path)
            # human-friendly model name (artifact stem)
            self.model_name = model_path.stem

            sys.stderr.write("WildfireModel: loading preprocessing artifacts\n")
            self.preprocess = joblib.load(
                ARTIFACTS_DIR / "preprocessing_artifacts.pkl"
            )

            self.scaler = self.preprocess["scaler"]
            self.feature_names = self.preprocess["feature_names"]
            sys.stderr.write("WildfireModel: model loaded successfully\n")
        except Exception as e:
            sys.stderr.write("WildfireModel: failed to load model or artifacts\n")
            traceback.print_exc(file=sys.stderr)
            raise
    def predict(self, features: dict):
        missing = set(self.feature_names) - set(features.keys())
        extra = set(features.keys()) - set(self.feature_names)

        if missing:
            raise ValueError(f"Missing features: {missing}")
        if extra:
            raise ValueError(f"Unexpected features: {extra}")

        X = np.array([features[f] for f in self.feature_names]).reshape(1, -1)
        X_scaled = self.scaler.transform(X)

        prob = self.model.predict_proba(X_scaled)[0][1]
        pred = int(prob >= 0.5)
        return prob, pred
