# src/config.py
from dataclasses import dataclass
from typing import List

@dataclass
class DataConfig:
    raw_path: str = "Data/Wildfire_Dataset.csv"
    train_path: str = "Data/processed/train.parquet"
    val_path: str = "Data/processed/val.parquet"
    test_path: str = "Data/processed/test.parquet"
    features: List[str] = None
    balance: str = "smote"
    seq_length: int = 7
    batch_size: int = 64
    grid_size: int = 10  # ← ADDED: For ConvLSTM spatial grid

    def __post_init__(self):
        self.features = [
            'latitude', 'longitude', 'pr', 'rmax', 'rmin', 'sph', 'srad',
            'tmmn', 'tmmx', 'vs', 'bi', 'fm100', 'fm1000', 'erc', 'etr', 'pet', 'vpd'
        ]

data_config = DataConfig()

@dataclass
class ModelConfig:
    experiment_name: str = "wildfire_all_models"
    run_name_rf: str = "RF_SMOTEN"
    run_name_xgb: str = "XGB_SMOTEN"
    run_name_lstm: str = "LSTM_Seq7"
    run_name_convlstm: str = "ConvLSTM_Spatial"
    random_state: int = 42
    n_estimators_rf: int = 100
    max_depth_rf: int = 10
    n_estimators_xgb: int = 100
    max_depth_xgb: int = 6
    learning_rate_xgb: float = 0.1
    epochs_dl: int = 20
    patience: int = 5

model_config = ModelConfig()