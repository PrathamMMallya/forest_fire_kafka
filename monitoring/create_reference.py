import pandas as pd
from pathlib import Path
from src.config import data_config

OUT = Path("monitoring/reference")
OUT.mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(data_config.train_path)

reference_cols = data_config.features + ["Wildfire"]
df[reference_cols].to_parquet(OUT / "reference_v1.parquet", index=False)

print("✅ Reference dataset created")
