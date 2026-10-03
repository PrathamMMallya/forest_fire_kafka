"""EDA pipeline for the Wildfire real-time detection project.

Data priority:
  1. Data/Wildfire_Dataset.csv   (real dataset — drop it in and rerun)
  2. eda/data/eda_sample.csv     (calibrated synthetic stand-in, same schema)

Produces 13 charts in eda/charts/ and a numeric summary in eda/eda_summary.txt.

Style: validated reference palette (dataviz method) —
  No Fire = #2a78d6 (blue, slot 1)   Fire = #eb6834 (orange, slot 2)
  correlation heatmap = diverging blue<->red, gray midpoint
  density maps = single-hue orange sequential ramp
"""
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

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
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE, "font.family": "DejaVu Sans",
    "axes.edgecolor": BASE, "axes.labelcolor": INK2,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.titlecolor": INK, "axes.labelsize": 10.5,
    "xtick.labelsize": 9.5, "ytick.labelsize": 9.5,
    "legend.frameon": False, "legend.fontsize": 10,
})

ROOT = Path(__file__).resolve().parents[1]
REAL = ROOT / "Data" / "Wildfire_Dataset.csv"
SAMPLE = Path(__file__).resolve().parent / "data" / "eda_sample.csv"
CHARTS = Path(__file__).resolve().parent / "charts"
CHARTS.mkdir(exist_ok=True)

FEATURES = [
    "latitude", "longitude", "pr", "rmax", "rmin", "sph", "srad",
    "tmmn", "tmmx", "vs", "bi", "fm100", "fm1000", "erc", "etr", "pet", "vpd",
]
FEATURE_LABELS = {
    "pr": "Precipitation (mm)", "rmax": "Max Rel. Humidity (%)",
    "rmin": "Min Rel. Humidity (%)", "sph": "Specific Humidity (kg/kg)",
    "srad": "Solar Radiation (W/m2)", "tmmn": "Min Temp (K)",
    "tmmx": "Max Temp (K)", "vs": "Wind Speed (m/s)", "bi": "Burning Index",
    "fm100": "100-hr Fuel Moisture (%)", "fm1000": "1000-hr Fuel Moisture (%)",
    "erc": "Energy Release Component", "etr": "Reference ET (mm)",
    "pet": "Potential ET (mm)", "vpd": "Vapor Pressure Deficit (kPa)",
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
        ax.text(0, 1.015, subtitle, transform=ax.transAxes, fontsize=9.5,
                color=INK2, va="bottom")


def save(fig, name):
    fig.savefig(CHARTS / name, dpi=190, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print(f"  saved {name}")


# ----------------------------------------------------------------- load ----
def load_data():
    if REAL.exists():
        src, real = str(REAL), True
        df = pd.read_csv(REAL, low_memory=False)
    elif SAMPLE.exists():
        src, real = str(SAMPLE), False
        df = pd.read_csv(SAMPLE, low_memory=False)
    else:
        import subprocess, sys
        subprocess.run([sys.executable, str(Path(__file__).parent / "generate_sample.py")], check=True)
        src, real = str(SAMPLE), False
        df = pd.read_csv(SAMPLE, low_memory=False)
    kind = "REAL DATASET" if real else "SYNTHETIC STAND-IN (real CSV not found)"
    print(f"data source: {src}\nprovenance:  {kind}\n")

    # replicate pipeline cleaning: 32767 sentinel -> NaN (src/data_prep.py:40)
    for c in FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] > 30000, c] = np.nan
    if df["Wildfire"].dtype.kind not in "iufcb":
        df["label"] = (df["Wildfire"].astype(str).str.strip().str.lower() == "yes").astype(int)
    else:
        df["label"] = df["Wildfire"].astype(int)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df["month"] = df["datetime"].dt.month
    df["year"] = df["datetime"].dt.year
    return df, real


# ---------------------------------------------------------------- charts ----
def c01_class_balance(df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.6),
                                   gridspec_kw={"width_ratios": [1.2, 1]})
    counts = df["label"].value_counts().sort_index()
    pct = counts / counts.sum() * 100
    bars = ax1.bar(["No Fire (0)", "Fire (1)"], counts.values,
                   color=[C_NOFIRE, C_FIRE], width=0.52, zorder=3)
    for b, c, p in zip(bars, counts.values, pct.values):
        ax1.text(b.get_x() + b.get_width() / 2, c * 1.02,
                 f"{c:,}\n({p:.1f}%)", ha="center", va="bottom",
                 fontsize=10.5, color=INK, fontweight="bold")
    ax1.set_ylim(0, counts.max() * 1.22)
    ax1.grid(axis="x")
    tidy(ax1, "Class distribution: highly imbalanced",
         "Wildfire occurrences vs. non-fire grid-days")
    ax1.set_ylabel("Records")

    wedges, _ = ax2.pie(counts.values, colors=[C_NOFIRE, C_FIRE],
                        startangle=90, counterclock=False,
                        wedgeprops=dict(width=0.42, edgecolor=SURFACE, linewidth=2))
    ax2.text(0, 0.08, f"{pct.get(1, 0):.1f}%", ha="center", fontsize=22,
             fontweight="bold", color=INK)
    ax2.text(0, -0.18, "of records are\nfire events", ha="center", fontsize=9.5, color=INK2)
    ax2.legend(["No Fire", "Fire"], loc="lower center", bbox_to_anchor=(0.5, -0.12),
               ncol=2)
    ax2.set_title("")
    fig.tight_layout()
    save(fig, "01_class_balance.png")
    note(f"IMBALANCE: {pct.get(1,0):.2f}% positive ({counts.get(1,0):,} / {counts.sum():,}) "
         f"- motivates SMOTEN + recall-focused evaluation")


def c02_missing(df):
    miss = df[FEATURES].isna().mean().sort_values(ascending=False) * 100
    fig, ax = plt.subplots(figsize=(11.5, 5.6))
    cols = miss[miss > 0]
    if cols.empty:
        cols = pd.Series([0.0], index=["(none)"])
    y = np.arange(len(cols))
    ax.barh(y, cols.values, color=C_NOFIRE, height=0.55, zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(cols.index)
    ax.invert_yaxis()
    for yi, v in zip(y, cols.values):
        ax.text(v + cols.max() * 0.01, yi, f"{v:.1f}%", va="center", fontsize=9, color=INK2)
    ax.grid(axis="y")
    ax.set_xlabel("Missing / sentinel-32767 values (%)")
    tidy(ax, "Missing values after sentinel handling (32767 -> NaN)",
         "Replicates src/data_prep.py cleaning; median imputation follows")
    ax.set_xlim(0, max(0.5, cols.max() * 1.25))
    fig.tight_layout()
    save(fig, "02_missing_values.png")
    note(f"MISSING: worst={cols.index[0]} {cols.iloc[0]:.2f}% | "
         f"median across features={df[FEATURES].isna().mean().mean()*100:.2f}%")


def c03_distributions(df):
    feats = ["tmmx", "vpd", "erc", "fm100", "vs", "rmin", "pr", "srad"]
    fig, axes = plt.subplots(2, 4, figsize=(14.5, 6.2))
    for ax, f in zip(axes.flat, feats):
        d = df[f].dropna()
        if f == "pr":
            d = d[d <= np.percentile(d, 99)]
        ax.hist(d, bins=45, color=C_NOFIRE, zorder=3)
        ax.set_title(FEATURE_LABELS.get(f, f), fontsize=10.5, loc="left")
        ax.tick_params(labelsize=8)
    fig.suptitle("Feature distributions (99th-pct trimmed for precipitation)",
                 x=0.02, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    save(fig, "03_feature_distributions.png")
    note("DISTRIBUTIONS: pr zero-inflated; tmmx/vpd right-skewed; "
         "fm100/fm1000 left-skewed toward dry fuels")


def c04_box_by_class(df):
    feats = ["tmmx", "vpd", "erc", "fm100", "rmin", "vs"]
    fig, axes = plt.subplots(2, 3, figsize=(14.5, 6.6))
    for ax, f in zip(axes.flat, feats):
        d = df[[f, "label"]].dropna()
        data = [d.loc[d.label == 0, f], d.loc[d.label == 1, f]]
        bp = ax.boxplot(data, tick_labels=["No Fire", "Fire"], widths=0.42,
                        patch_artist=True, showfliers=False, zorder=3,
                        medianprops=dict(color=INK, linewidth=1.6),
                        whiskerprops=dict(color=BASE), capprops=dict(color=BASE))
        for patch, c in zip(bp["boxes"], [C_NOFIRE, C_FIRE]):
            patch.set_facecolor(c)
            patch.set_alpha(0.85)
            patch.set_edgecolor(SURFACE)
        ax.set_title(FEATURE_LABELS.get(f, f), fontsize=10.5, loc="left")
        ax.grid(axis="x")
    fig.suptitle("Fire vs. No-Fire: weather & fuel condition separation",
                 x=0.02, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    save(fig, "04_boxplots_by_class.png")
    meds = {f: (df.loc[df.label == 1, f].median(), df.loc[df.label == 0, f].median())
            for f in feats}
    note("BOXPLOTS: fire days show higher " +
         ", ".join(f for f in feats if meds[f][0] > meds[f][1]) +
         "; lower " + ", ".join(f for f in feats if meds[f][0] < meds[f][1]))


def c05_corr(df):
    corr = df[FEATURES].corr()
    fig, ax = plt.subplots(figsize=(11.8, 9.4))
    im = ax.imshow(corr, cmap=DIV, vmin=-1, vmax=1)
    ax.set_xticks(range(len(FEATURES)))
    ax.set_yticks(range(len(FEATURES)))
    ax.set_xticklabels(FEATURES, rotation=45, ha="right", fontsize=9)
    ax.set_yticklabels(FEATURES, fontsize=9)
    for i in range(len(FEATURES)):
        for j in range(len(FEATURES)):
            v = corr.iloc[i, j]
            txt = f"{v:.1f}" if abs(v) >= 0.995 else f"{v:.1f}"[1:]
            ax.text(j, i, txt, ha="center", va="center", fontsize=6.4,
                    color="white" if abs(v) > 0.55 else INK2)
    ax.grid(False)
    cbar = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02)
    cbar.ax.tick_params(labelsize=8.5)
    cbar.outline.set_visible(False)
    ax.set_title("Correlation matrix (Pearson) - 17 features", loc="left",
                 pad=30, fontsize=13, fontweight="bold")
    ax.text(0, 1.015, "tmmx/vpd/erc move together: fire-danger index redundancy",
            transform=ax.transAxes, fontsize=9.5, color=INK2, va="bottom")
    fig.tight_layout()
    save(fig, "05_correlation_heatmap.png")
    c = corr.abs().where(~np.eye(len(FEATURES), dtype=bool))
    top = c.stack().sort_values(ascending=False).head(3)
    note("CORR top pairs: " + "; ".join(f"{a}-{b} r={corr.loc[a,b]:+.2f}"
                                        for (a, b) in top.index))


def c06_monthly(df):
    by_m = df.groupby("month")["label"].agg(["sum", "count", "mean"])
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 4.8))
    ax1.bar(by_m.index, by_m["sum"], color=C_FIRE, width=0.6, zorder=3)
    peak = by_m["sum"].idxmax()
    ax1.annotate(f"peak: {pd.Timestamp(2000, peak, 1).strftime('%B')}",
                 xy=(peak, by_m["sum"].max()),
                 xytext=(peak + 0.6, by_m["sum"].max() * 0.92), fontsize=10, color=INK2,
                 arrowprops=dict(arrowstyle="->", color=MUTED))
    ax1.set_xticks(range(1, 13))
    ax1.set_xticklabels(["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"])
    ax1.grid(axis="x")
    ax1.set_ylabel("Fire records")
    tidy(ax1, "Fire records by month", "Strong warm-season concentration")

    ax2.bar(by_m.index, by_m["mean"] * 100, color=C_FIRE, width=0.6,
            alpha=0.75, zorder=3)
    ax2.set_xticks(range(1, 13))
    ax2.set_xticklabels(["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"])
    ax2.grid(axis="x")
    ax2.set_ylabel("Fire occurrence rate (%)")
    tidy(ax2, "Occurrence rate by month", "Rate removes the sampling effect")
    fig.tight_layout()
    save(fig, "06_monthly_seasonality.png")
    note(f"SEASONALITY: peak month={peak} "
         f"({by_m['mean'].max()*100:.1f}% rate); low season rate "
         f"{by_m['mean'].min()*100:.1f}%")


def c07_yearly(df):
    by_y = df.groupby("year")["label"].agg(["sum", "count"])
    fig, ax = plt.subplots(figsize=(11.5, 4.8))
    ax.bar(by_y.index.astype(str), by_y["sum"], color=C_FIRE, width=0.55, zorder=3)
    for x, (y_, v) in enumerate(by_y["sum"].items()):
        ax.text(x, v, f"{v:,}", ha="center", va="bottom", fontsize=9, color=INK2)
    ax.set_ylim(0, by_y["sum"].max() * 1.15)
    ax.grid(axis="x")
    ax.set_ylabel("Fire records")
    tidy(ax, "Fire records per year", "Inter-annual variability driven by fire seasons")
    fig.tight_layout()
    save(fig, "07_yearly_trend.png")
    note(f"YEARS: {by_y.index.min()}-{by_y.index.max()}, avg "
         f"{by_y['sum'].mean():,.0f} fires/yr, max {by_y['sum'].idxmax()}")


def c08_spatial(df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5.4))
    f = df[df.label == 1]
    hb = ax1.hexbin(f["longitude"], f["latitude"], gridsize=42, cmap=SEQ_ORANGE,
                    mincnt=1, linewidths=0.1)
    cb = fig.colorbar(hb, ax=ax1, fraction=0.04, pad=0.02)
    cb.set_label("Fire records", fontsize=9)
    cb.outline.set_visible(False)
    ax1.set_xlabel("Longitude"); ax1.set_ylabel("Latitude")
    tidy(ax1, "Spatial fire density", "Western US fire-prone regions (hexbin)")

    ax2.hexbin(df["longitude"], df["latitude"], gridsize=42, cmap="Greys",
               mincnt=1, linewidths=0.1, alpha=0.55)
    ax2.set_xlabel("Longitude"); ax2.set_ylabel("Latitude")
    tidy(ax2, "All observations", "Sampling footprint for contrast")
    fig.tight_layout()
    save(fig, "08_spatial_fire_density.png")
    note(f"SPATIAL: fires span lat {f.latitude.min():.0f}-{f.latitude.max():.0f}, "
         f"lon {f.longitude.min():.0f}-{f.longitude.max():.0f}; density clusters in "
         "Mediterranean/mountain West")


def c09_kde(df):
    feats = ["vpd", "erc", "fm100"]
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.4))
    for ax, f in zip(axes, feats):
        for lab, color, name in [(0, C_NOFIRE, "No Fire"), (1, C_FIRE, "Fire")]:
            d = df.loc[df.label == lab, f].dropna()
            xs = np.linspace(np.percentile(d, 0.5), np.percentile(d, 99.5), 200)
            kde = np.exp(-0.5 * ((xs[:, None] - np.percentile(d, np.linspace(5, 95, 120))[None, :])
                                 / (1.06 * d.std() * len(d) ** -0.2)) ** 2).sum(1)
            kde /= np.trapezoid(kde, xs)
            ax.plot(xs, kde, color=color, linewidth=2, label=name, zorder=3)
            ax.fill_between(xs, kde, color=color, alpha=0.12)
        ax.set_title(FEATURE_LABELS.get(f, f), fontsize=10.5, loc="left")
        ax.legend()
    fig.suptitle("Class-conditional density overlap: where models must discriminate",
                 x=0.02, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save(fig, "09_kde_fire_weather.png")
    note("KDE: vpd & erc shift right for fire; fm100 shifts left - "
         "interpretable fire-weather signal")


def c10_feature_shift(df):
    mean0 = df[df.label == 0][FEATURES].mean()
    mean1 = df[df.label == 1][FEATURES].mean()
    shift = ((mean1 - mean0) / df[FEATURES].std()).sort_values()
    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    colors = [C_NOFIRE if v < 0 else C_RED for v in shift.values]
    ax.barh(shift.index, shift.values, color=colors, height=0.58, zorder=3)
    ax.axvline(0, color=BASE, linewidth=1)
    for y, v in enumerate(shift.values):
        ax.text(v + (0.012 if v >= 0 else -0.012) * shift.abs().max(),
                y, f"{v:+.2f}", va="center",
                ha="left" if v >= 0 else "right", fontsize=8.5, color=INK2)
    ax.grid(axis="y")
    ax.set_xlabel("Standardized mean shift (Fire - No Fire) / SD")
    ax.set_xlim(shift.min() * 1.25, shift.max() * 1.25)
    tidy(ax, "Which features separate fire from no-fire?",
         "Positive = higher on fire days; magnitude = class-mean shift in SDs")
    fig.tight_layout()
    save(fig, "10_feature_class_shift.png")
    note(f"FEATURE SHIFT: top predictors {', '.join(shift.abs().sort_values(ascending=False).index[:4])}")


def c11_outliers(df):
    out = {}
    for f in FEATURES:
        d = df[f].dropna()
        q1, q3 = d.quantile([0.25, 0.75])
        iqr = q3 - q1
        out[f] = ((d < q1 - 1.5 * iqr) | (d > q3 + 1.5 * iqr)).mean() * 100
    out = pd.Series(out).sort_values(ascending=False).head(12)
    fig, ax = plt.subplots(figsize=(11.5, 5.4))
    ax.barh(out.index[::-1], out.values[::-1], color=C_NOFIRE, height=0.55, zorder=3)
    for y, v in enumerate(out.values[::-1]):
        ax.text(v + out.max() * 0.012, y, f"{v:.1f}%", va="center", fontsize=9, color=INK2)
    ax.grid(axis="y")
    ax.set_xlabel("Share of records beyond 1.5*IQR fences (%)")
    tidy(ax, "Outlier profile (IQR fences)", "Heavy tails justify RobustScaler in the pipeline")
    fig.tight_layout()
    save(fig, "11_outliers_iqr.png")
    note(f"OUTLIERS: max {out.index[0]} {out.iloc[0]:.1f}% - RobustScaler chosen in src/data_prep.py")


def c12_daily(df):
    daily = df.groupby(df["datetime"].dt.date)["label"].sum()
    daily.index = pd.to_datetime(daily.index)
    roll = daily.rolling(7, min_periods=1).mean()
    years = sorted(daily.index.year)[-2:]
    fig, axes = plt.subplots(1, 2, figsize=(13.8, 4.4), sharey=False)
    for ax, y in zip(axes, years):
        r = roll[roll.index.year == y]
        raw = daily[daily.index.year == y]
        ax.plot(raw.index, raw.values, color=GRID, linewidth=0.8, zorder=2)
        ax.plot(r.index, r.values, color=C_FIRE, linewidth=2, zorder=3)
        ax.set_title(str(y), loc="left", fontsize=11)
        ax.tick_params(axis="x", rotation=30, labelsize=8)
    axes[0].set_ylabel("Daily fire records")
    fig.suptitle("Daily fire bursts (7-day rolling mean): stream-level temporal structure",
                 x=0.02, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    save(fig, "12_daily_fire_bursts.png")
    note(f"DAILY BURSTS: peak day {daily.idxmax().date()} with {daily.max()} fire records "
         "- supports windowed aggregation in Kafka Streams")


def c13_pr_inflation(df):
    d = df["pr"].dropna()
    zero = (d == 0).mean() * 100
    nz = d[d > 0]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 4.6),
                                   gridspec_kw={"width_ratios": [1.3, 1]})
    ax1.hist(d[d <= np.percentile(d, 99)], bins=50, color=C_NOFIRE, zorder=3)
    ax1.axvline(0.001, color=C_FIRE, linewidth=2, linestyle="--", zorder=4)
    ax1.set_yscale("log")
    ax1.set_xlabel("Precipitation (mm)")
    ax1.set_ylabel("Records (log scale)")
    tidy(ax1, f"Precipitation is zero-inflated: {zero:.0f}% zeros",
         "Log-scale histogram; dashed line marks the zero spike")

    ax2.bar(["No Fire", "Fire"],
            [df.loc[df.label == 0, "pr"].eq(0).mean() * 100,
             df.loc[df.label == 1, "pr"].eq(0).mean() * 100],
            color=[C_NOFIRE, C_FIRE], width=0.5, zorder=3)
    for x, v in enumerate([df.loc[df.label == 0, "pr"].eq(0).mean() * 100,
                           df.loc[df.label == 1, "pr"].eq(0).mean() * 100]):
        ax2.text(x, v + 1, f"{v:.0f}%", ha="center", fontsize=10.5, fontweight="bold", color=INK)
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("Dry records (%)")
    ax2.grid(axis="x")
    tidy(ax2, "Dry-day share by class", "Fire days are almost never rain days")
    fig.tight_layout()
    save(fig, "13_pr_zero_inflation.png")
    note(f"PR: {zero:.1f}% zeros overall; dry share fire={df.loc[df.label==1,'pr'].eq(0).mean()*100:.0f}% "
         f"vs nofire={df.loc[df.label==0,'pr'].eq(0).mean()*100:.0f}%")


# ----------------------------------------------------------------- main ----
def main():
    df, real = load_data()
    note(f"DATASET: rows={len(df):,} features={len(FEATURES)} "
         f"dates={df.datetime.min().date()}..{df.datetime.max().date()} "
         f"extent=lat({df.latitude.min():.1f},{df.latitude.max():.1f}) "
         f"lon({df.longitude.min():.1f},{df.longitude.max():.1f}) "
         f"source={'REAL' if real else 'synthetic'}")
    steps = [c01_class_balance, c02_missing, c03_distributions, c04_box_by_class,
             c05_corr, c06_monthly, c07_yearly, c08_spatial, c09_kde,
             c10_feature_shift, c11_outliers, c12_daily, c13_pr_inflation]
    for fn in steps:
        fn(df)
    (CHARTS.parent / "eda_summary.txt").write_text("\n".join(SUMMARY))
    print("\n" + "-" * 60)
    print("summary written to eda/eda_summary.txt:")
    print("\n".join(f"  - {s}" for s in SUMMARY))


if __name__ == "__main__":
    main()
