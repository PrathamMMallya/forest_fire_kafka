"""Generate a calibrated synthetic sample mirroring Data/Wildfire_Dataset.csv.

NOTE: The real dataset (Data/Wildfire_Dataset.csv, DVC-tracked) is not present in
this checkout. This generator produces a statistically realistic stand-in with the
exact same schema, grounded in published gridMET climatology (Abatzoglou, 2013,
Int. J. Climatology) so EDA charts can be produced for the presentation. Point
`eda/eda.py` at the real CSV to regenerate every chart with real numbers.
"""
import numpy as np
import pandas as pd
from pathlib import Path

rng = np.random.default_rng(42)
N = 250_000
OUT = Path("eda/data/eda_sample.csv")

FEATURES = [
    "latitude", "longitude", "pr", "rmax", "rmin", "sph", "srad",
    "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
]

# --- spatial extent: mixture of real fire-prone regions + uniform background ---
CENTERS = np.array([  # Sierra Nevada, N CA Cascades, CO Rockies, N Rockies, AZ/NM, Great Basin, WY
    [37.5, -119.5], [41.3, -122.8], [39.8, -105.5], [46.8, -113.9],
    [34.3, -111.8], [40.5, -116.8], [43.5, -110.5],
])
pick = rng.integers(0, len(CENTERS), N)
lat = CENTERS[pick, 0] + rng.normal(0, 1.4, N)
lon = CENTERS[pick, 1] + rng.normal(0, 1.7, N)
bg = rng.random(N) < 0.30
lat[bg] = rng.uniform(31.5, 49.0, bg.sum())
lon[bg] = rng.uniform(-124.8, -103.0, bg.sum())
lat = np.clip(lat, 31.5, 49.0)
lon = np.clip(lon, -124.8, -103.0)

# --- dates: uniform over 2014-2023 ---
start = pd.Timestamp("2014-01-01")
days = (pd.Timestamp("2023-12-31") - start).days
doy = rng.integers(0, days, N)
dates = start + pd.to_timedelta(doy, unit="D")
month = dates.month.to_numpy()

# --- seasonal base signal (northern hemisphere) ---
seasonal = 0.5 * (1 - np.cos(2 * np.pi * (doy - 15) / 365.25))  # 0 winter -> 1 peak Jul

# --- temperature (K): season + elevation-ish noise; warmer at lower lat ---
t_base = 278 + 6.5 * (46 - lat) / 15 + 16 * seasonal
tmmx = t_base + rng.normal(4.5, 3.0, N)
tmmn = t_base - rng.normal(7.0, 2.5, N)
tmmx = np.clip(tmmx, 258, 322)
tmmn = np.clip(tmmn, 242, 305)

# --- humidity: dry season lowers RH; VPD rises with temp and dryness ---
dryness = seasonal + rng.normal(0, 0.18, N)
rmax = np.clip(rng.normal(78, 12, N) - 22 * dryness, 12, 100)
rmin = np.clip(rng.normal(38, 11, N) - 18 * dryness, 3, 90)
sph = np.clip(0.002 + 0.011 * (0.35 + 0.65 * seasonal) * (rmin / 40) + rng.normal(0, 0.0012, N), 0.0005, 0.018)

esat_x = 0.6108 * np.exp(17.27 * (tmmx - 273.15) / (tmmx - 35.85))
esat_n = 0.6108 * np.exp(17.27 * (tmmn - 273.15) / (tmmn - 35.85))
vpd = np.clip(0.5 * (esat_x * (1 - rmax / 100) + esat_n * (1 - rmin / 100)), 0.0, 7.0)

# --- precipitation: zero-inflated, wetter winter ---
p_wet = np.clip(0.30 - 0.24 * seasonal, 0.02, 0.5)
pr = np.where(rng.random(N) < p_wet, rng.gamma(1.1, 3.0, N), 0.0)
pr = np.clip(pr, 0, 120)

# --- radiation (daily mean W/m2), wind, ET ---
srad = np.clip(180 + 90 * seasonal - 40 * np.abs(lon + 114) / 11 + rng.normal(0, 22, N), 60, 380)
vs = np.clip(rng.gamma(2.2, 1.9, N) + 0.8 * dryness, 0.3, 15)
pet = np.clip(1.2 + 5.5 * seasonal + 0.08 * (tmmx - 280) + rng.normal(0, 0.6, N), 0, 12)
etr = np.clip(pet * rng.uniform(0.75, 0.98, N), 0, 11)

# --- fuel moisture: wet after rain, dry in season; lags weather simply ---
fm100 = np.clip(24 - 12 * dryness - 0.25 * (tmmx - 280) - 0.35 * vpd + rng.normal(0, 2.2, N), 1.5, 32)
fm1000 = np.clip(fm100 + rng.normal(1.5, 2.0, N), 2.0, 35)

# --- fire-danger indices: ERC/BI rise with dry, hot, windy ---
erc = np.clip(4 + 6.5 * (vpd) + 1.1 * (32 - fm100) + 1.4 * vs + 5.5 * seasonal + rng.normal(0, 4.5, N), 0, 115)
bi = np.clip(0.9 * erc * (0.6 + 0.06 * vs) + rng.normal(0, 5, N), 0, 130)

# --- fire occurrence label: logistic coupling on fire-weather physics ---
logit = (
    -6.4
    + 0.115 * (tmmx - 285)
    + 0.34 * vpd
    + 0.055 * erc
    - 0.16 * fm100
    - 0.022 * rmin
    + 0.10 * vs
    - 0.06 * np.log1p(pr)
    + 0.5 * np.clip(lat - 38, 0, 10) / 10
    + 0.55 * np.clip(-(lon + 114) / 9, 0, 1)   # mountain West / West Coast boost
    + rng.normal(0, 0.35, N)
)
p_fire = 1 / (1 + np.exp(-logit))
wildfire = (rng.random(N) < p_fire).astype(int)

df = pd.DataFrame({
    "latitude": lat.round(4), "longitude": lon.round(4),
    "pr": pr.round(2), "rmax": rmax.round(1), "rmin": rmin.round(1),
    "sph": sph.round(5), "srad": srad.round(1),
    "tmmn": tmmn.round(2), "tmmx": tmmx.round(2),
    "vs": vs.round(2), "bi": bi.round(1),
    "fm100": fm100.round(2), "fm1000": fm1000.round(2),
    "erc": erc.round(1), "etr": etr.round(2), "pet": pet.round(2),
    "vpd": vpd.round(3),
    "datetime": dates.strftime("%Y-%m-%d"),
    "Wildfire": np.where(wildfire == 1, "Yes", "No"),
})

# --- real-data artifacts the pipeline handles (src/data_prep.py: replace(32767, NaN)) ---
SENT_COLS = ["tmmx", "tmmn", "vpd", "erc", "fm100", "srad", "bi"]
for c in SENT_COLS:
    idx = rng.choice(N, size=int(N * 0.012), replace=False)
    df.loc[idx, c] = 32767.0
nan_cols = ["fm1000", "sph", "rmin"]
for c in nan_cols:
    idx = rng.choice(N, size=int(N * 0.008), replace=False)
    df.loc[idx, c] = np.nan

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
pos = df["Wildfire"].eq("Yes").mean() * 100
print(f"wrote {OUT} rows={len(df):,} fire_rate={pos:.2f}%")
print(df[list(FEATURES)].describe().T[["mean", "min", "max"]].round(2).to_string())
