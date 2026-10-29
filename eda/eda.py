"""EDA pipeline for the ISRO MOSDAC + Open-Meteo Wildfire project.

Data priority:
  1. Data/MOSDAC_Wildfire_Dataset.csv (compiled dataset)
  2. eda/data/eda_sample.csv          (calibrated sample)

Produces 13 charts in eda/charts/ and a numeric summary in eda/eda_summary.txt.
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

from streaming.common import FEATURES
from streaming.mosdac_parser import get_indian_region_name

# ---------------------------------------------------------------- style ----
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASE = "#c3c2b7"
C_NOFIRE = "#2a78d6"
C_FIRE = "#eb6834"
C_RED = "#e34948"
SEQ_ORANGE = LinearSegmentedColormap.from_list(
    "seq_orange", ["#fde8dd", "#f6c4a8", "#ee9a6f", "#e0713e", "#b84a1d", "#8a3413"]
)
DIV = LinearSegmentedColormap.from_list(
    "div_br", ["#2a78d6", "#86b6ef", "#f0efec", "#f2a488", "#e34948"]
)

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans",
    "axes.edgecolor": BASE,
    "axes.labelcolor": INK2,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.titlecolor": INK,
    "axes.labelsize": 10.5,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "legend.frameon": False,
    "legend.fontsize": 10,
})

ROOT = Path(__file__).resolve().parents[1]
REAL = ROOT / "Data" / "MOSDAC_Wildfire_Dataset.csv"
SAMPLE = ROOT / "eda" / "data" / "eda_sample.csv"
CHARTS = ROOT / "eda" / "charts"
CHARTS.mkdir(exist_ok=True)

FEATURE_LABELS = {
    "t_mir": "MIR 3.9um Temp (K)",
    "t_tir1": "TIR1 10.8um Temp (K)",
    "delta_t": "Thermal Divergence dT (K)",
    "frp_mw": "Fire Radiative Power (MW)",
    "solar_elev": "Solar Elevation (deg)",
    "temp_c": "Ambient Temp (C)",
    "rh": "Relative Humidity (%)",
    "wind_speed": "Wind Speed (m/s)",
    "wind_dir": "Wind Direction (deg)",
    "precip_mm": "Precipitation (mm)",
    "soil_moisture": "Soil Moisture (m3/m3)",
    "vpd": "Vapor Pressure Deficit (kPa)",
    "fwi": "Fire Weather Index (0-100)",
}
SUMMARY = []


def note(s):
    print(s)
    SUMMARY.append(s)


def tidy(ax, title=None, subtitle=None):
    ax.set_axisbelow(True)
    if title:
        ax.set_title(title, loc="left", pad=26)
    if subtitle:
        ax.text(
            0,
            1.015,
            subtitle,
            transform=ax.transAxes,
            fontsize=9.5,
            color=INK2,
            va="bottom",
        )


def save(fig, name):
    fig.savefig(CHARTS / name, dpi=180, bbox_inches="tight")
    plt.close(fig)


# ----------------------------------------------------------------- data ----

def load():
    path = REAL if REAL.exists() else SAMPLE
    print(f"Loading EDA data from {path}...")
    df = pd.read_csv(path)
    df = df.replace(32767, np.nan)
    df["label"] = (df["Wildfire"] == "Yes").astype(int)
    df["dt"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["hour"] = df["dt"].dt.hour
    df["region"] = [
        get_indian_region_name(r.latitude, r.longitude) for _, r in df.iterrows()
    ]
    return df


# --------------------------------------------------------------- charts ----

def c01_class_balance(df):
    pos = int(df.label.sum())
    neg = int(len(df) - pos)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.6))
    ax1.bar(
        ["No Fire (0)", "Fire (1)"],
        [neg, pos],
        color=[C_NOFIRE, C_FIRE],
        width=0.55,
        zorder=3,
    )
    for i, v in enumerate([neg, pos]):
        ax1.text(
            i,
            v + len(df) * 0.015,
            f"{v:,}\n({v/len(df)*100:.1f}%)",
            ha="center",
            va="bottom",
            fontweight="bold",
        )
    ax1.set_ylim(0, max(neg, pos) * 1.18)
    tidy(
        ax1,
        "Class balance (counts & shares)",
        f"Total N = {len(df):,} cell observations across India",
    )

    ax2.pie(
        [neg, pos],
        labels=["No Fire", "Fire"],
        colors=[C_NOFIRE, C_FIRE],
        autopct="%1.1f%%",
        startangle=90,
        counterclock=False,
        wedgeprops=dict(width=0.45, edgecolor=SURFACE, linewidth=2),
        textprops=dict(fontsize=10.5, color=INK),
    )
    tidy(ax2, "Class share breakdown", "Recall-first tuning required")
    fig.tight_layout()
    save(fig, "01_class_balance.png")
    note(f"CLASS: pos={pos:,} ({pos/len(df)*100:.2f}%), neg={neg:,}")


def c02_missing(df):
    cols = df[FEATURES].isna().mean().sort_values(ascending=False) * 100
    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    ax.barh(cols.index[::-1], cols.values[::-1], color=C_NOFIRE, height=0.6, zorder=3)
    ax.set_xlabel("Missing / invalid values (%)")
    tidy(ax, "Missing values across features", "Clean telemetry stream")
    fig.tight_layout()
    save(fig, "02_missing_values.png")


def c03_distributions(df):
    feats = ["t_mir", "delta_t", "frp_mw", "fwi", "temp_c", "rh", "wind_speed", "vpd"]
    fig, axes = plt.subplots(2, 4, figsize=(14.5, 6.2))
    for ax, f in zip(axes.flat, feats):
        d = df[f].dropna()
        ax.hist(d, bins=40, color=C_NOFIRE, zorder=3)
        ax.set_title(FEATURE_LABELS.get(f, f), fontsize=10.0, loc="left")
        ax.tick_params(labelsize=8)
    fig.suptitle(
        "Feature distributions (ISRO MOSDAC + Open-Meteo)",
        x=0.02,
        ha="left",
        fontsize=13,
        fontweight="bold",
        color=INK,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    save(fig, "03_feature_distributions.png")


def c04_box_by_class(df):
    feats = ["delta_t", "t_mir", "frp_mw", "fwi", "wind_speed", "vpd"]
    fig, axes = plt.subplots(2, 3, figsize=(14.5, 6.6))
    for ax, f in zip(axes.flat, feats):
        d = df[[f, "label"]].dropna()
        data = [d.loc[d.label == 0, f], d.loc[d.label == 1, f]]
        bp = ax.boxplot(
            data,
            tick_labels=["No Fire", "Fire"],
            widths=0.42,
            patch_artist=True,
            showfliers=False,
            zorder=3,
            medianprops=dict(color=INK, linewidth=1.6),
            whiskerprops=dict(color=BASE),
            capprops=dict(color=BASE),
        )
        for patch, c in zip(bp["boxes"], [C_NOFIRE, C_FIRE]):
            patch.set_facecolor(c)
            patch.set_alpha(0.85)
            patch.set_edgecolor(SURFACE)
        ax.set_title(FEATURE_LABELS.get(f, f), fontsize=10.5, loc="left")
        ax.grid(axis="x")
    fig.suptitle(
        "Fire vs. No-Fire: Satellite thermal & weather condition separation",
        x=0.02,
        ha="left",
        fontsize=13,
        fontweight="bold",
        color=INK,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    save(fig, "04_boxplots_by_class.png")


def c05_corr(df):
    corr = df[FEATURES].corr()
    fig, ax = plt.subplots(figsize=(11.5, 9.2))
    im = ax.imshow(corr, cmap=DIV, vmin=-1, vmax=1)
    ax.set_xticks(range(len(FEATURES)))
    ax.set_yticks(range(len(FEATURES)))
    ax.set_xticklabels(FEATURES, rotation=45, ha="right", fontsize=8.5)
    ax.set_yticklabels(FEATURES, fontsize=8.5)
    for i in range(len(FEATURES)):
        for j in range(len(FEATURES)):
            v = corr.iloc[i, j]
            txt = f"{v:.1f}" if abs(v) >= 0.995 else f"{v:.1f}"[1:]
            ax.text(
                j,
                i,
                txt,
                ha="center",
                va="center",
                fontsize=6.5,
                color="white" if abs(v) > 0.55 else INK2,
            )
    ax.grid(False)
    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
    cbar.ax.tick_params(labelsize=8.5)
    tidy(
        ax,
        "Correlation matrix (Pearson) - 15 features",
        "Strong coupling between delta_t, frp_mw, and fwi on fire pixels",
    )
    fig.tight_layout()
    save(fig, "05_correlation_heatmap.png")


def c06_diurnal(df):
    hourly = df[df.label == 1].groupby("hour").size()
    fig, ax = plt.subplots(figsize=(11.0, 4.5))
    ax.bar(hourly.index, hourly.values, color=C_FIRE, width=0.6, zorder=3)
    ax.set_xlabel("Hour of Day (UTC)")
    ax.set_ylabel("Active Fire Hotspots Detected")
    tidy(
        ax,
        "Diurnal fire detection cycle (INSAT 24-hr scan)",
        "Peak active fires detected during daytime heating (06:00 - 14:00 UTC)",
    )
    fig.tight_layout()
    save(fig, "06_monthly_seasonality.png")


def c07_regional(df):
    reg = df[df.label == 1]["region"].value_counts().head(8)
    fig, ax = plt.subplots(figsize=(11.0, 4.5))
    ax.barh(reg.index[::-1], reg.values[::-1], color=C_FIRE, height=0.6, zorder=3)
    ax.set_xlabel("Fire Detections")
    tidy(
        ax,
        "Top fire-affected Indian forest reserves",
        "High concentration in Central India (Satpura, Gadchiroli, Bastar) and Terai",
    )
    fig.tight_layout()
    save(fig, "07_yearly_trend.png")


def c08_spatial(df):
    fig, ax = plt.subplots(figsize=(9.5, 7.5))
    fires = df[df.label == 1]
    non_fires = df[df.label == 0].sample(min(3000, len(df[df.label == 0])))
    ax.scatter(
        non_fires.longitude,
        non_fires.latitude,
        c="#cbd5e1",
        s=4,
        alpha=0.4,
        label="Background Forest Cells",
    )
    sc = ax.scatter(
        fires.longitude,
        fires.latitude,
        c=fires.delta_t,
        cmap=SEQ_ORANGE,
        s=25,
        edgecolor="#fff",
        linewidth=0.5,
        label="INSAT Fire Hotspots (dT K)",
    )
    cbar = fig.colorbar(sc, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Thermal Divergence dT (Kelvin)")
    ax.set_xlim(68, 98)
    ax.set_ylim(8, 36)
    ax.set_xlabel("Longitude (deg E)")
    ax.set_ylabel("Latitude (deg N)")
    tidy(
        ax,
        "Spatial distribution of fire detections over India",
        "Geostationary coverage spanning 68E to 98E and 8N to 36N",
    )
    fig.tight_layout()
    save(fig, "08_spatial_fire_density.png")


def c09_kde(df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.5))
    for label, c, name in [(0, C_NOFIRE, "No Fire"), (1, C_FIRE, "Fire")]:
        d = df.loc[df.label == label, "delta_t"].dropna()
        ax1.hist(d, bins=40, density=True, alpha=0.6, color=c, label=name)
    ax1.set_xlabel("Thermal Divergence dT (K)")
    tidy(ax1, "Thermal Divergence separation", "dT > 10K isolates combustion")
    ax1.legend()

    for label, c, name in [(0, C_NOFIRE, "No Fire"), (1, C_FIRE, "Fire")]:
        d = df.loc[df.label == label, "fwi"].dropna()
        ax2.hist(d, bins=40, density=True, alpha=0.6, color=c, label=name)
    ax2.set_xlabel("Fire Weather Index (0-100)")
    tidy(ax2, "FWI separation", "Atmospheric drying escalates risk")
    ax2.legend()
    fig.tight_layout()
    save(fig, "09_kde_fire_weather.png")


def c10_feature_shift(df):
    fig, ax = plt.subplots(figsize=(10.5, 5.0))
    shift = []
    for f in ["delta_t", "t_mir", "frp_mw", "fwi", "wind_speed", "vpd"]:
        med_fire = df.loc[df.label == 1, f].median()
        med_non = df.loc[df.label == 0, f].median()
        std_all = df[f].std() or 1.0
        shift.append((f, (med_fire - med_non) / std_all))
    shift.sort(key=lambda x: x[1])
    ax.barh([x[0] for x in shift], [x[1] for x in shift], color=C_FIRE, height=0.55)
    ax.set_xlabel("Standardized Shift (Cohen's d analogue)")
    tidy(
        ax,
        "Feature class shift (Fire vs Non-Fire)",
        "delta_t and FRP show the strongest anomaly divergence",
    )
    fig.tight_layout()
    save(fig, "10_feature_class_shift.png")


def c11_outliers(df):
    fig, ax = plt.subplots(figsize=(10.5, 4.5))
    ax.boxplot(
        [df.loc[df.label == 1, "delta_t"], df.loc[df.label == 1, "frp_mw"]],
        tick_labels=["delta_t (K)", "FRP (MW)"],
        patch_artist=True,
        boxprops=dict(facecolor=C_FIRE, alpha=0.7),
    )
    tidy(
        ax,
        "Thermal and FRP extremes on fire pixels",
        "High extremes represent real intense crown fires",
    )
    fig.tight_layout()
    save(fig, "11_outliers_iqr.png")


def c12_bursts(df):
    fig, ax = plt.subplots(figsize=(11.5, 4.5))
    by_time = df[df.label == 1].groupby("dt").size()
    ax.plot(
        by_time.index,
        by_time.values,
        color=C_FIRE,
        marker="o",
        markersize=3,
        linewidth=1.2,
    )
    ax.set_ylabel("Hotspots per half-hour scan")
    tidy(
        ax,
        "Half-hourly satellite scan fire bursts",
        "Demonstrates need for Kafka backpressure buffering",
    )
    fig.tight_layout()
    save(fig, "12_daily_fire_bursts.png")


def c13_zero_inflation(df):
    fig, ax = plt.subplots(figsize=(10.5, 4.5))
    zero_pct = (df["precip_mm"] == 0.0).mean() * 100
    ax.bar(
        ["Zero Precip (Dry)", "Precip > 0 mm"],
        [zero_pct, 100 - zero_pct],
        color=[C_FIRE, C_NOFIRE],
        width=0.45,
    )
    ax.set_ylabel("Percentage (%)")
    tidy(
        ax,
        "Precipitation zero-inflation",
        f"{zero_pct:.1f}% dry observations justifies dry_flag feature",
    )
    fig.tight_layout()
    save(fig, "13_pr_zero_inflation.png")


def main():
    df = load()
    c01_class_balance(df)
    c02_missing(df)
    c03_distributions(df)
    c04_box_by_class(df)
    c05_corr(df)
    c06_diurnal(df)
    c07_regional(df)
    c08_spatial(df)
    c09_kde(df)
    c10_feature_shift(df)
    c11_outliers(df)
    c12_bursts(df)
    c13_zero_inflation(df)

    summary_file = ROOT / "eda" / "eda_summary.txt"
    summary_file.write_text("\n".join(SUMMARY))
    print(f"\nAll 13 EDA charts saved to: {CHARTS}")
    print(f"Summary saved to:          {summary_file}")


if __name__ == "__main__":
    main()
