# src/trainer.py

import os
from pathlib import Path
import joblib
import mlflow
import mlflow.sklearn
import mlflow.pytorch
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import matplotlib.pyplot as plt

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    recall_score,
    roc_auc_score,
    confusion_matrix,
    ConfusionMatrixDisplay,
)
from xgboost import XGBClassifier

from .config import model_config, data_config
from .models.dl_models import LSTMModel, ConvLSTM
from .data_prep_seq import create_spatial_sequences
from .utils import get_logger, save_artifact

logger = get_logger(__name__)

ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(exist_ok=True)


# ==========================================================
# HELPER: EXPORT MODEL FOR INFERENCE
# ==========================================================
def export_model_for_serving(model, model_name: str):
    path = ARTIFACTS_DIR / f"best_{model_name}.pkl"
    joblib.dump(model, path)
    logger.info(f"Exported {model_name} for inference → {path}")


# ==========================================================
# TRAINER
# ==========================================================
class WildfireTrainer:
    def __init__(self):
        mlflow.set_experiment(model_config.experiment_name)
        mlflow.enable_system_metrics_logging()

        logger.info(f"MLflow experiment: {model_config.experiment_name}")

        if torch.cuda.is_available():
            mlflow.log_param("gpu_name", torch.cuda.get_device_name(0))

    # ======================================================
    # TABULAR MODELS
    # ======================================================
    def train_tabular(
        self,
        model_name: str,
        X_train, y_train,
        X_val, y_val,
        X_test, y_test
    ):
        run_name = (
            model_config.run_name_rf
            if "RandomForest" in model_name
            else model_config.run_name_xgb
        )

        with mlflow.start_run(run_name=f"{model_name}_{run_name}"):

            # -----------------------
            # MODEL INIT
            # -----------------------
            if "RandomForest" in model_name:
                model = RandomForestClassifier(
                    n_estimators=model_config.n_estimators_rf,
                    max_depth=model_config.max_depth_rf,
                    random_state=model_config.random_state,
                    n_jobs=-1,
                    class_weight="balanced",
                )

            elif "XGBoost" in model_name:
                model = XGBClassifier(
                    n_estimators=model_config.n_estimators_xgb,
                    max_depth=model_config.max_depth_xgb,
                    learning_rate=model_config.learning_rate_xgb,
                    random_state=model_config.random_state,
                    eval_metric="logloss",
                )

            model.fit(X_train, y_train)

            # -----------------------
            # VALIDATION
            # -----------------------
            y_val_pred = model.predict(X_val)
            y_val_prob = model.predict_proba(X_val)[:, 1]

            mlflow.log_metrics({
                "val_accuracy": accuracy_score(y_val, y_val_pred),
                "val_f1": f1_score(y_val, y_val_pred),
                "val_recall": recall_score(y_val, y_val_pred),
                "val_auc": roc_auc_score(y_val, y_val_prob),
            })

            # -----------------------
            # TEST
            # -----------------------
            y_test_pred = model.predict(X_test)
            y_test_prob = model.predict_proba(X_test)[:, 1]

            test_metrics = {
                "test_accuracy": accuracy_score(y_test, y_test_pred),
                "test_f1": f1_score(y_test, y_test_pred),
                "test_recall": recall_score(y_test, y_test_pred),
                "test_auc": roc_auc_score(y_test, y_test_prob),
            }
            mlflow.log_metrics(test_metrics)

            # -----------------------
            # CONFUSION MATRIX
            # -----------------------
            cm = confusion_matrix(y_test, y_test_pred)
            disp = ConfusionMatrixDisplay(cm)
            disp.plot(cmap="Blues")

            cm_path = ARTIFACTS_DIR / f"{model_name}_confusion_matrix.png"
            plt.savefig(cm_path)
            plt.close()

            mlflow.log_artifact(str(cm_path))

            # -----------------------
            # MLflow MODEL LOG
            # -----------------------
            mlflow.log_params(model.get_params())
            input_example = (
                X_train[:5].values if hasattr(X_train, "values") else X_train[:5]
            )

            mlflow.sklearn.log_model(
                model,
                artifact_path=f"{model_name}_model",
                input_example=input_example,
            )

            # -----------------------
            # EXPORT FOR INFERENCE
            # -----------------------
            export_model_for_serving(model, model_name)

            logger.info(
                f"{model_name} TEST — "
                f"Acc: {test_metrics['test_accuracy']:.3f}, "
                f"AUC: {test_metrics['test_auc']:.3f}"
            )

    # ======================================================
    # DEEP LEARNING MODELS
    # ======================================================
    def train_dl(self, model_name: str, sample_ratio: float = 0.1):
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Training {model_name} on {device}")

        train_loader, val_loader, test_loader = create_spatial_sequences(
            sample_ratio=sample_ratio,
            return_splits=True
        )

        run_name = (
            model_config.run_name_lstm
            if "LSTM" in model_name
            else model_config.run_name_convlstm
        )

        with mlflow.start_run(run_name=f"{model_name}_{run_name}"):

            # -----------------------
            # MODEL INIT
            # -----------------------
            if "LSTM" in model_name:
                input_size = (
                    len(data_config.features)
                    * data_config.grid_size
                    * data_config.grid_size
                )
                model = LSTMModel(input_size).to(device)

                def preprocess(x):
                    b, t, c, h, w = x.shape
                    return x.reshape(b, t, c * h * w)

            else:
                model = ConvLSTM(in_c=len(data_config.features)).to(device)

                def preprocess(x):
                    return x

            criterion = nn.BCELoss()
            optimizer = optim.Adam(model.parameters(), lr=0.001)

            best_val_loss = float("inf")
            patience_counter = 0

            # -----------------------
            # TRAIN LOOP
            # -----------------------
            for epoch in range(model_config.epochs_dl):
                model.train()
                train_loss, train_correct, train_total = 0, 0, 0

                for Xb, yb in train_loader:
                    Xb, yb = Xb.to(device), yb.to(device)
                    Xb = preprocess(Xb)

                    optimizer.zero_grad()
                    preds = model(Xb)
                    loss = criterion(preds, yb)
                    loss.backward()
                    optimizer.step()

                    train_loss += loss.item()
                    train_correct += ((preds > 0.5) == yb).sum().item()
                    train_total += yb.numel()

                train_loss /= len(train_loader)
                train_acc = train_correct / train_total

                # -----------------------
                # VALIDATION
                # -----------------------
                model.eval()
                val_loss, val_correct, val_total = 0, 0, 0

                with torch.no_grad():
                    for Xb, yb in val_loader:
                        Xb, yb = Xb.to(device), yb.to(device)
                        Xb = preprocess(Xb)
                        preds = model(Xb)

                        val_loss += criterion(preds, yb).item()
                        val_correct += ((preds > 0.5) == yb).sum().item()
                        val_total += yb.numel()

                val_loss /= len(val_loader)
                val_acc = val_correct / val_total

                mlflow.log_metrics(
                    {
                        "train_loss": train_loss,
                        "train_accuracy": train_acc,
                        "val_loss": val_loss,
                        "val_accuracy": val_acc,
                    },
                    step=epoch,
                )

                # -----------------------
                # EARLY STOPPING
                # -----------------------
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    patience_counter = 0
                    torch.save(
                        model.state_dict(),
                        ARTIFACTS_DIR / f"best_{model_name}.pt"
                    )
                else:
                    patience_counter += 1
                    if patience_counter >= model_config.patience:
                        logger.info(f"Early stopping at epoch {epoch}")
                        break

            # -----------------------
            # TEST
            # -----------------------
            model.eval()
            all_preds, all_labels = [], []

            with torch.no_grad():
                for Xb, yb in test_loader:
                    Xb, yb = Xb.to(device), yb.to(device)
                    Xb = preprocess(Xb)
                    preds = model(Xb)

                    all_preds.extend((preds > 0.5).cpu().numpy())
                    all_labels.extend(yb.cpu().numpy())

            test_acc = accuracy_score(all_labels, all_preds)
            test_f1 = f1_score(all_labels, all_preds)
            test_recall = recall_score(all_labels, all_preds)

            mlflow.log_metrics({
                "test_accuracy": test_acc,
                "test_f1": test_f1,
                "test_recall": test_recall,
            })

            # -----------------------
            # CONFUSION MATRIX
            # -----------------------
            cm = confusion_matrix(all_labels, all_preds)
            disp = ConfusionMatrixDisplay(cm)
            disp.plot(cmap="Blues")

            cm_path = ARTIFACTS_DIR / f"{model_name}_confusion_matrix.png"
            plt.savefig(cm_path)
            plt.close()

            mlflow.log_artifact(str(cm_path))
            mlflow.pytorch.log_model(model, f"{model_name}_model")

            logger.info(
                f"{model_name} TEST — Acc: {test_acc:.3f}, F1: {test_f1:.3f}"
            )
