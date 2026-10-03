"""Methodology pipeline diagram: Data -> Kafka -> Streams -> ML/DL -> Alerts."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from pathlib import Path

SURFACE = "#ffffff"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"

GROUPS = {
    "ingest": "#2a78d6",    # blue
    "stream": "#1baf7a",    # aqua
    "ml":     "#eb6834",    # orange
    "alert":  "#e34948",    # red
    "eval":   "#4a3aa7",    # violet
}

STAGES = [
    # (row, col, title, note, group)
    (0, 0, "Data Sources", "gridMET fire-weather\n17 features · daily", "ingest"),
    (0, 1, "Kafka Producer", "JSON events\nkey = grid cell", "ingest"),
    (0, 2, "Kafka Topics", "wildfire.raw\n6 partitions · RF=3", "ingest"),
    (0, 3, "Kafka Streams", "stream app\nconsumer group", "stream"),
    (1, 0, "Filtering & Windowing", "schema check · dedup\ntumbling 1-min + sliding 24-h", "stream"),
    (1, 1, "Feature Engineering", "rolling aggregates\nindices · scaling", "stream"),
    (1, 2, "ML/DL Fire Detection", "RF · XGBoost\nLSTM · ConvLSTM", "ml"),
    (1, 3, "Fire Prediction", "probability + threshold\nclass · confidence", "ml"),
    (2, 0, "Kafka Alert Topic", "wildfire.alerts\nfan-out to consumers", "alert"),
    (2, 1, "Elasticsearch", "alert indexing\nKibana search", "alert"),
    (2, 2, "Dashboard / Emergency\nAlerts", "React dashboard\nSMS · e-mail · push", "alert"),
    (2, 3, "Performance\nEvaluation", "latency · throughput\nprecision/recall · AUC", "eval"),
]

fig, ax = plt.subplots(figsize=(16.2, 8.0), dpi=200)
fig.patch.set_facecolor(SURFACE)
ax.set_xlim(0, 16.2)
ax.set_ylim(0, 8.0)
ax.axis("off")

W, H = 3.28, 1.30
X0 = 0.42
ys = [5.62, 3.30, 0.74]  # row baselines (box tops: 6.92 / 4.60 / 2.04)

boxes = {}
for row, col, title, note, grp in STAGES:
    x = X0 + col * (W + 0.52)
    y = ys[row]
    ax.add_patch(FancyBboxPatch((x, y), W, H, boxstyle="round,pad=0.02,rounding_size=0.09",
                                linewidth=2.2, edgecolor=GROUPS[grp], facecolor=SURFACE, zorder=3))
    ax.add_patch(FancyBboxPatch((x, y + H - 0.34), W, 0.34,
                                boxstyle="round,pad=0.02,rounding_size=0.09",
                                linewidth=0, facecolor=GROUPS[grp], zorder=4))
    ax.add_patch(plt.Rectangle((x, y + H - 0.36), W, 0.12, facecolor=GROUPS[grp],
                               edgecolor="none", zorder=4))
    boxes[(row, col)] = (x, y)
    ax.text(x + W / 2, y + H - 0.17, f"{row * 4 + col + 1}. {title}",
            ha="center", va="center", fontsize=10.3, fontweight="bold",
            color="white", zorder=5)
    ax.text(x + W / 2, y + H / 2 - 0.14, note, ha="center", va="center",
            fontsize=8.6, color=INK2, zorder=5, linespacing=1.45)

def arrow(p1, p2, color=MUTED, style="-", lw=1.8, rad=0.0, ms=16):
    a = FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=ms,
                        color=color, linewidth=lw, linestyle=style,
                        connectionstyle=f"arc3,rad={rad}", zorder=2)
    ax.add_patch(a)

# horizontal arrows within rows
for row in range(3):
    for col in range(3):
        x, y = boxes[(row, col)]
        x2, y2 = boxes[(row, col + 1)]
        arrow((x + W + 0.03, y + H / 2), (x2 - 0.05, y2 + H / 2))

# row connectors via left margin (S-flow)
for row, yline in ((0, 5.11), (1, 3.02)):
    x, y = boxes[(row, 3)]
    x2, y2 = boxes[(row + 1, 0)]
    ax.add_patch(FancyArrowPatch((x + W / 2, y - 0.03), (x + W / 2, yline),
                                 arrowstyle="-", color=MUTED, linewidth=1.6, zorder=2))
    arrow((x + W / 2, yline), (0.12, yline), style=(0, (4, 3)), lw=1.4, ms=1)
    ax.add_patch(FancyArrowPatch((0.12, yline), (0.12, y2 + H / 2),
                                 arrowstyle="-", color=MUTED, linewidth=1.4,
                                 linestyle=(0, (4, 3)), zorder=2))
    arrow((0.12, y2 + H / 2), (x2 - 0.05, y2 + H / 2), style=(0, (4, 3)), lw=1.4)

# MLOps feedback band between rows 1 and 2
bx, by, bw, bh = 2.55, 2.32, 11.9, 0.42
ax.add_patch(FancyBboxPatch((bx, by), bw, bh, boxstyle="round,pad=0.02,rounding_size=0.08",
                            linewidth=1.4, edgecolor="#9a8fe8", facecolor="#f4f2fc", zorder=3))
ax.text(bx + bw / 2, by + bh / 2,
        "MLOps feedback loop:  Evidently drift monitoring  ->  Prefect retraining  ->  MLflow model registry  ->  redeploy",
        ha="center", va="center", fontsize=9.0, color="#4a3aa7", zorder=4)
x8, y8 = boxes[(1, 3)]
arrow((x8 + W / 2, y8 - 0.03), (x8 + W / 2, by + bh + 0.02),
      color="#9a8fe8", style=(0, (4, 3)), lw=1.5, ms=12)
x7, y7 = boxes[(1, 2)]
arrow((x7 + W / 2, by + bh + 0.02), (x7 + W / 2, y7 - 0.03),
      color="#9a8fe8", style=(0, (4, 3)), lw=1.5, ms=12)

# title + subtitle
ax.text(0.42, 7.50, "Proposed Methodology — Real-Time Wildfire Detection & Alerting Pipeline",
        fontsize=15, fontweight="bold", color=INK, ha="left", va="center")
ax.text(0.42, 7.14, "Kafka-centric stream processing architecture with ML/DL fire detection",
        fontsize=10.5, color=INK2, ha="left", va="center")

# legend at bottom (5 even slots)
legend_items = [
    ("Data ingestion", "ingest"), ("Stream processing", "stream"),
    ("ML / DL intelligence", "ml"), ("Alerting & serving", "alert"),
    ("Evaluation", "eval"),
]
slot = 3.1
lx0 = (16.2 - slot * len(legend_items)) / 2
for i, (name, grp) in enumerate(legend_items):
    lx = lx0 + i * slot
    ax.add_patch(plt.Rectangle((lx, 0.26), 0.17, 0.17, facecolor=GROUPS[grp], edgecolor="none"))
    ax.text(lx + 0.24, 0.345, name, fontsize=9, color=INK2, va="center")

out = Path(__file__).resolve().parent / "diagram"
out.mkdir(exist_ok=True)
fig.savefig(out / "methodology_pipeline.png", bbox_inches="tight", facecolor=SURFACE)
print("saved", out / "methodology_pipeline.png")
