# src/data_prep_seq.py
import torch
from torch.utils.data import Dataset, DataLoader
import numpy as np
import pandas as pd

from .config import data_config
from .utils import get_logger

logger = get_logger(__name__)


def bin_to_grid(df, grid_size=10, features=None):
    if features is None:
        features = data_config.features

    df = df.copy()
    df["lat_bin"] = pd.cut(df["latitude"], bins=grid_size, labels=False)
    df["lon_bin"] = pd.cut(df["longitude"], bins=grid_size, labels=False)
    df = df.dropna(subset=["lat_bin", "lon_bin"])

    df["lat_bin"] = df["lat_bin"].astype(int)
    df["lon_bin"] = df["lon_bin"].astype(int)

    grid = np.zeros((grid_size, grid_size, len(features)))

    for _, row in df.iterrows():
        i, j = int(row["lat_bin"]), int(row["lon_bin"])
        if 0 <= i < grid_size and 0 <= j < grid_size:
            grid[i, j] = row[features].values

    return grid


class SpatialSequenceDataset(Dataset):
    def __init__(self, sequences, labels):
        self.sequences = sequences  # (N, T, C, H, W)
        self.labels = labels

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        return (
            torch.from_numpy(self.sequences[idx]).float(),
            torch.tensor(self.labels[idx], dtype=torch.float),
        )


def create_spatial_sequences(
    sample_ratio=0.05,
    batch_size=None,
    return_splits=False,
):
    """
    Returns:
        - train_loader (default)
        - train_loader, val_loader, test_loader (if return_splits=True)
    """

    logger.info("Creating spatial sequences for DL models...")

    if batch_size is None:
        batch_size = data_config.batch_size

    # ======================================================
    # LOAD TRAIN DATA (WITH datetime)
    # ======================================================
    df = pd.read_parquet(data_config.train_path)
    logger.info(f"Loaded train.parquet: {df.shape}")

    # ------------------------------------------------------
    # SAMPLE DATA (FOR SPEED)
    # ------------------------------------------------------
    n = int(len(df) * sample_ratio)
    idx = np.random.choice(len(df), n, replace=False)
    df = df.iloc[idx].copy()

    # ------------------------------------------------------
    # ENSURE datetime
    # ------------------------------------------------------
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date

    # ======================================================
    # DAILY GRIDS
    # ======================================================
    daily_grids = []
    daily_labels = []

    for date, group in df.groupby("date"):
        grid = bin_to_grid(
            group,
            grid_size=data_config.grid_size,
            features=data_config.features,
        )
        label = group["Wildfire"].any()
        daily_grids.append(grid)
        daily_labels.append(label)

    sequences = np.stack(daily_grids)  # (T, H, W, C)
    labels = np.array(daily_labels)

    # ======================================================
    # SEQUENCE WINDOWING
    # ======================================================
    seq_len = data_config.seq_length
    X, y = [], []

    for i in range(len(sequences) - seq_len + 1):
        X.append(sequences[i : i + seq_len])
        y.append(labels[i + seq_len - 1])

    X = np.stack(X).transpose(0, 1, 4, 2, 3)  # (N, T, C, H, W)
    y = np.array(y)

    # ======================================================
    # SPLIT (70 / 15 / 15)
    # ======================================================
    n_total = len(X)
    train_end = int(0.7 * n_total)
    val_end = int(0.85 * n_total)

    X_train, y_train = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]

    train_ds = SpatialSequenceDataset(X_train, y_train)
    val_ds = SpatialSequenceDataset(X_val, y_val)
    test_ds = SpatialSequenceDataset(X_test, y_test)

    train_loader = DataLoader(
        train_ds, batch_size=batch_size, shuffle=True
    )
    val_loader = DataLoader(
        val_ds, batch_size=batch_size, shuffle=False
    )
    test_loader = DataLoader(
        test_ds, batch_size=batch_size, shuffle=False
    )

    logger.info(
        f"DL splits → train: {len(train_ds)}, "
        f"val: {len(val_ds)}, test: {len(test_ds)}"
    )

    if return_splits:
        return train_loader, val_loader, test_loader

    return train_loader
