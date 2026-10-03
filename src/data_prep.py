# src/data_prep.py
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTEN
from pathlib import Path
from tqdm import tqdm
from .config import data_config
from .utils import get_logger, save_artifact

logger = get_logger(__name__)

NEEDED_COLS = data_config.features + ['Wildfire', 'datetime']

class DataProcessor:
    def __init__(self):
        self.medians = None
        self.scaler = None
        self.feature_names = data_config.features

    def clean_and_save_chunked(self):
        raw_path = Path(data_config.raw_path)
        chunk_dir = Path("Data/processed/chunks")
        chunk_dir.mkdir(exist_ok=True)

        if list(chunk_dir.glob("chunk_*.parquet")):
            logger.info("Using existing cleaned chunks...")
            return

        logger.info("Cleaning CSV in chunks...")
        chunk_iter = pd.read_csv(
            raw_path,
            chunksize=50_000,
            usecols=NEEDED_COLS,
            engine='python',
            on_bad_lines='skip'
        )

        for i, chunk in enumerate(tqdm(chunk_iter, desc="Cleaning", unit="chunk")):
            chunk = chunk.replace(32767, np.nan)
            chunk["datetime"] = pd.to_datetime(chunk["datetime"], errors='coerce')
            chunk["Wildfire"] = (chunk["Wildfire"] == "Yes").astype('bool')
            chunk = chunk.dropna(subset=["datetime"])
            
            # === SAVE FULL CHUNK WITH datetime ===
            chunk.to_parquet(chunk_dir / f"chunk_{i:04d}.parquet", index=False)

    def impute_median_numpy(self, X):
        if self.medians is None:
            self.medians = np.nanmedian(X, axis=0)
        X_filled = X.copy()
        for i, median in enumerate(self.medians):
            mask = np.isnan(X_filled[:, i])
            if mask.any():
                X_filled[mask, i] = median
        return X_filled

    def prepare_tabular(self):
        Path("Data/processed").mkdir(parents=True, exist_ok=True)
        self.clean_and_save_chunked()
        chunk_dir = Path("Data/processed/chunks")
        all_files = sorted(chunk_dir.glob("chunk_*.parquet"))

        sample_files = all_files[:5]
        logger.info(f"Using {len(sample_files)} chunks (~250k rows)")

        dfs = [pd.read_parquet(f) for f in sample_files]
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded: {df.shape}")

        # === SPLIT INDICES ===
        X = df[self.feature_names].to_numpy(dtype='float32')
        y = df["Wildfire"].astype(int).to_numpy()
        idx = np.arange(len(X))
        train_idx, temp_idx, y_train, y_temp = train_test_split(
            idx, y, test_size=0.3, stratify=y, random_state=42
        )
        val_idx, test_idx, y_val, y_test = train_test_split(
            temp_idx, y_temp, test_size=0.5, stratify=y_temp, random_state=42
        )

        X_train = X[train_idx]
        X_val = X[val_idx]
        X_test = X[test_idx]

        logger.info("Imputing with numpy median...")
        X_train_imp = self.impute_median_numpy(X_train)
        X_val_imp = self.impute_median_numpy(X_val)
        X_test_imp = self.impute_median_numpy(X_test)

        # === SMOTEN ON TRAINING ONLY ===
        if data_config.balance == "smote":
            logger.info("Applying SMOTEN on 1,000 minority + 1,000 majority...")
            pos_idx = np.where(y_train == 1)[0]
            neg_idx = np.where(y_train == 0)[0]

            np.random.seed(42)
            pos_sample = np.random.choice(pos_idx, size=min(1000, len(pos_idx)), replace=False)
            neg_sample = np.random.choice(neg_idx, size=min(1000, len(neg_idx)), replace=False)
            sample_idx = np.concatenate([pos_sample, neg_sample])

            X_sample = X_train_imp[sample_idx]
            y_sample = y_train[sample_idx]

            smote = SMOTEN(sampling_strategy='auto', random_state=42, k_neighbors=3)
            X_res, y_res = smote.fit_resample(X_sample, y_sample)

            X_train_final = X_res.astype('float32')
            y_train = y_res
            train_idx = np.arange(len(X_train_final))  # New indices
        else:
            X_train_final = X_train_imp.astype('float32')

        # === SCALING ===
        from sklearn.preprocessing import RobustScaler
        self.scaler = RobustScaler()
        logger.info("Scaling...")
        X_train_scaled = self.scaler.fit_transform(X_train_final)
        X_val_scaled = self.scaler.transform(X_val_imp)
        X_test_scaled = self.scaler.transform(X_test_imp)

        # === SAVE WITH datetime ===
        # Training
        train_df = pd.DataFrame(X_train_scaled, columns=self.feature_names)
        if data_config.balance == "smote":
            # Placeholder datetime for SMOTEN samples
            train_df['datetime'] = pd.date_range("2020-01-01", periods=len(train_df), freq='D')
        else:
            train_df['datetime'] = df.iloc[train_idx]['datetime'].values
        train_df['Wildfire'] = y_train
        train_df.to_parquet(data_config.train_path, index=False)

        # Validation
        val_df = pd.DataFrame(X_val_scaled, columns=self.feature_names)
        val_df['datetime'] = df.iloc[val_idx]['datetime'].values
        val_df['Wildfire'] = y_val
        val_df.to_parquet(data_config.val_path, index=False)

        # Test
        test_df = pd.DataFrame(X_test_scaled, columns=self.feature_names)
        test_df['datetime'] = df.iloc[test_idx]['datetime'].values
        test_df['Wildfire'] = y_test
        test_df.to_parquet(data_config.test_path, index=False)

        # Save y separately
        pd.Series(y_train).to_csv("Data/processed/y_train.csv", index=False)
        pd.Series(y_val).to_csv("Data/processed/y_val.csv", index=False)
        pd.Series(y_test).to_csv("Data/processed/y_test.csv", index=False)

        save_artifact({
            "medians": self.medians,
            "scaler": self.scaler,
            "feature_names": self.feature_names
        }, "preprocessing_artifacts.pkl")

        logger.info("SUCCESS: Tabular pipeline complete! (with datetime)")
if __name__ == "__main__":
    processor = DataProcessor()
    processor.prepare_tabular()
