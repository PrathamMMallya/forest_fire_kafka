# train.py
from src.data_prep import DataProcessor
from src.trainer import WildfireTrainer
from src.config import data_config
import pandas as pd


def main():
    print("Wildfire MLOps: Training ALL Models (RF, XGB, LSTM, ConvLSTM) - Nov 10, 2025\n")

    # ======================================================
    # TABULAR PIPELINE
    # ======================================================
    proc = DataProcessor()
    proc.prepare_tabular()

    # -----------------------
    # LOAD SPLITS
    # -----------------------
    train_df = pd.read_parquet("Data/processed/train.parquet")
    val_df   = pd.read_parquet("Data/processed/val.parquet")
    test_df  = pd.read_parquet("Data/processed/test.parquet")

    X_train = train_df[data_config.features]
    y_train = train_df["Wildfire"]

    X_val = val_df[data_config.features]
    y_val = val_df["Wildfire"]

    X_test = test_df[data_config.features]
    y_test = test_df["Wildfire"]

    # ======================================================
    # TRAIN MODELS
    # ======================================================
    trainer = WildfireTrainer()

    trainer.train_tabular(
        "RandomForest",
        X_train, y_train,
        X_val, y_val,
        X_test, y_test
    )

    trainer.train_tabular(
        "XGBoost",
        X_train, y_train,
        X_val, y_val,
        X_test, y_test
    )

    # ======================================================
    # DEEP LEARNING
    # ======================================================
    print("\nTraining Deep Learning Models...")

    trainer.train_dl("LSTM", sample_ratio=1.0)
    trainer.train_dl("ConvLSTM", sample_ratio=1.0)

    print("\nALL MODELS TRAINED! Run: python -m mlflow ui")


if __name__ == "__main__":
    main()
