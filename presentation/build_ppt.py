"""Build the Wildfire real-time detection presentation (.pptx, 16:9).

Reads: eda/charts/*.png, presentation/diagram/methodology_pipeline.png,
       eda/eda_summary.txt
Writes: presentation/Wildfire_RealTime_Fire_Detection.pptx
"""
from pathlib import Path
from PIL import Image
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

ROOT = Path(__file__).resolve().parents[1]
CHARTS = ROOT / "eda" / "charts"
DIAGRAM = ROOT / "presentation" / "diagram" / "methodology_pipeline.png"
OUT = ROOT / "presentation" / "Wildfire_RealTime_Fire_Detection.pptx"

INK = RGBColor(0x0B, 0x0B, 0x0B)
INK2 = RGBColor(0x52, 0x51, 0x4E)
MUTED = RGBColor(0x89, 0x87, 0x81)
ORANGE = RGBColor(0xEB, 0x68, 0x34)
BLUE = RGBColor(0x2A, 0x78, 0xD6)
RED = RGBColor(0xE3, 0x49, 0x48)
AQUA = RGBColor(0x1B, 0xAF, 0x7A)
VIOLET = RGBColor(0x4A, 0x3A, 0xA7)
PANEL = RGBColor(0xF6, 0xF5, 0xF2)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LINE = RGBColor(0xE1, 0xE0, 0xD9)
FONT = "Calibri"

SW, SH = Inches(13.333), Inches(7.5)
prs = Presentation()
prs.slide_width = SW
prs.slide_height = SH
BLANK = prs.slide_layouts[6]

slide_no = [0]


def slide():
    s = prs.slides.add_slide(BLANK)
    slide_no[0] += 1
    return s


def box(s, x, y, w, h, fill=None, line=None, line_w=0.75):
    from pptx.enum.shapes import MSO_SHAPE
    sh = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
    sh.fill.solid()
    sh.fill.fore_color.rgb = fill if fill else WHITE
    if line:
        sh.line.color.rgb = line
        sh.line.width = Pt(line_w)
    else:
        sh.line.fill.background()
    sh.shadow.inherit = False
    return sh


def text(s, x, y, w, h, runs, size=14, color=INK, bold=False, align=PP_ALIGN.LEFT,
         anchor=MSO_ANCHOR.TOP, spacing=1.0, space_after=4):
    """runs: str, or list of (txt, dict-overrides) paragraphs."""
    tb = s.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    if isinstance(runs, str):
        runs = [(runs, {})]
    for i, (t, ov) in enumerate(runs):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = ov.get("align", align)
        p.space_after = Pt(ov.get("space_after", space_after))
        p.line_spacing = ov.get("spacing", spacing)
        r = p.add_run()
        r.text = t
        f = r.font
        f.name = ov.get("font", FONT)
        f.size = Pt(ov.get("size", size))
        f.bold = ov.get("bold", bold)
        f.italic = ov.get("italic", False)
        f.color.rgb = ov.get("color", color)
    return tb


def header(s, kicker, title, accent=ORANGE):
    box(s, 0, 0, SW, Inches(0.10), fill=accent)
    text(s, Inches(0.55), Inches(0.32), Inches(12.2), Inches(0.3),
         kicker.upper(), size=12, color=accent, bold=True)
    text(s, Inches(0.55), Inches(0.62), Inches(12.2), Inches(0.75),
         title, size=27, bold=True, color=INK)
    text(s, Inches(12.35), Inches(7.06), Inches(0.6), Inches(0.3),
         f"{slide_no[0]:02d}", size=10, color=MUTED, align=PP_ALIGN.RIGHT)
    text(s, Inches(0.55), Inches(7.06), Inches(8), Inches(0.3),
         "Real-Time Forest Fire Detection | Kafka & Stream Processing", size=9, color=MUTED)


def pic_fit(s, path, x, y, w, h, center=True):
    iw, ih = Image.open(path).size
    ar = iw / ih
    bw, bh = w, h
    if bw / bh > ar:
        pw = bh * ar
        ph = bh
    else:
        pw = bw
        ph = bw / ar
    px = x + (bw - pw) / 2 if center else x
    py = y + (bh - ph) / 2 if center else y
    return s.shapes.add_picture(str(path), int(px), int(py), int(pw), int(ph))


def bullets(s, x, y, w, h, items, size=15, gap=8, color=INK):
    runs = []
    for it in items:
        if isinstance(it, tuple):
            head, rest = it
            runs.append((f"▪  {head}", {"bold": True, "size": size, "color": color, "space_after": 2}))
            runs.append((f"     {rest}", {"size": size - 1, "color": INK2, "space_after": gap}))
        else:
            runs.append((f"▪  {it}", {"size": size, "color": color, "space_after": gap}))
    text(s, x, y, w, h, runs)


def table(s, x, y, w, h, rows, col_w=None, header_fill=RGBColor(0x2A, 0x78, 0xD6),
          fs=11.5, header_fs=12, row_h=Inches(0.34)):
    n_r, n_c = len(rows), len(rows[0])
    gfx = s.shapes.add_table(n_r, n_c, x, y, w, h)
    tbl = gfx.table
    if col_w:
        for i, cw in enumerate(col_w):
            tbl.columns[i].width = Inches(cw)
    for ri, row in enumerate(rows):
        tbl.rows[ri].height = row_h if ri else Inches(0.38)
        for ci, val in enumerate(row):
            cell = tbl.cell(ri, ci)
            cell.margin_left = Inches(0.08)
            cell.margin_right = Inches(0.06)
            cell.margin_top = cell.margin_bottom = Inches(0.03)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            cell.fill.solid()
            if ri == 0:
                cell.fill.fore_color.rgb = header_fill
            else:
                cell.fill.fore_color.rgb = WHITE if ri % 2 else PANEL
            tf = cell.text_frame
            tf.word_wrap = True
            p = tf.paragraphs[0]
            r = p.add_run()
            r.text = str(val)
            r.font.name = FONT
            r.font.size = Pt(header_fs if ri == 0 else fs)
            r.font.bold = ri == 0
            r.font.color.rgb = WHITE if ri == 0 else INK
    return tbl


# ============================================================ 01 TITLE
s = slide()
box(s, 0, 0, SW, SH, fill=RGBColor(0x11, 0x16, 0x22))
box(s, 0, Inches(4.55), SW, Pt(3), fill=ORANGE)
text(s, Inches(0.9), Inches(2.0), Inches(11.5), Inches(0.4),
     "KAFKA & STREAM PROCESSING  •  PROJECT PRESENTATION", size=14, color=ORANGE, bold=True)
text(s, Inches(0.9), Inches(2.45), Inches(11.6), Inches(1.7),
     "Real-Time Forest Fire Detection System", size=44, bold=True, color=WHITE)
text(s, Inches(0.9), Inches(3.7), Inches(11.6), Inches(0.6),
     "A Kafka-centric streaming pipeline for ML/DL wildfire prediction and emergency alerting",
     size=18, color=RGBColor(0xC3, 0xC2, 0xB7))
text(s, Inches(0.9), Inches(4.95), Inches(11), Inches(1.4), [
    ("Presented by:  Pratham Mallya", {"size": 15, "color": WHITE, "space_after": 6}),
    ("Guide:  <Guide Name>   •   Department of <CSE/AI-ML>", {"size": 13, "color": RGBColor(0x8B, 0x93, 0xA6)}),
    ("October 2026", {"size": 13, "color": RGBColor(0x8B, 0x93, 0xA6)}),
])

# ============================================================ 02 AGENDA
s = slide()
header(s, "Agenda", "Presentation Outline")
items = [
    ("01", "Introduction", "Problem, motivation, why real-time streaming", BLUE),
    ("02", "Dataset Description & EDA Results", "gridMET fire-weather dataset, 12 exploratory analyses", AQUA),
    ("03", "Literature Review", "32 papers across 7 research themes", ORANGE),
    ("04", "Summary of Literature", "Synthesis, comparison and identified gaps", ORANGE),
    ("05", "Objectives", "Six concrete system objectives", VIOLET),
    ("06", "Proposed Methodology", "Kafka pipeline architecture & design decisions", RED),
    ("07", "Percentage of Work Completed", "Module-wise progress and remaining plan", RGBColor(0x6B, 0x6B, 0x6B)),
]
y = Inches(1.75)
for num, t, sub, c in items:
    box(s, Inches(0.9), y, Inches(0.72), Inches(0.62), fill=c)
    text(s, Inches(0.9), y + Inches(0.10), Inches(0.72), Inches(0.4),
         num, size=18, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    text(s, Inches(1.85), y + Inches(0.02), Inches(10.6), Inches(0.35),
         t, size=17, bold=True, color=INK)
    text(s, Inches(1.85), y + Inches(0.36), Inches(10.6), Inches(0.3),
         sub, size=12, color=INK2)
    y += Inches(0.74)

# ============================================================ 03 INTRO 1
s = slide()
header(s, "01 Introduction", "The Problem: Wildfires Are Faster Than Our Response")
bullets(s, Inches(0.55), Inches(1.7), Inches(7.2), Inches(4.9), [
    ("Escalating fire regime", "Climate change extends fire seasons and raises fuel dryness; recent western-US fire years burned record areas."),
    ("Detection latency is the bottleneck", "Satellite passes and manual reports lag ignition by tens of minutes to hours; every minute of delay multiplies containment cost and risk."),
    ("Fire danger is predictable from weather", "Temperature, humidity, wind, fuel moisture and energy-release indices co-move hours ahead of ignition - a strong, continuously available signal."),
    ("Data exists, but is batch-bound", "Fire-weather grids and sensor feeds arrive continuously, yet most prediction systems still train and serve in batch, far from real time."),
], size=14.5)
panel = box(s, Inches(8.15), Inches(1.75), Inches(4.6), Inches(4.85), fill=PANEL)
text(s, Inches(8.45), Inches(1.95), Inches(4.0), Inches(0.4),
     "WHY THIS PROJECT", size=12, bold=True, color=ORANGE)
text(s, Inches(8.45), Inches(2.4), Inches(4.0), Inches(4.0), [
    ("Goal", {"bold": True, "size": 13.5, "space_after": 2}),
    ("Detect fire-favourable conditions and raise alerts within seconds of data arrival, using a Kafka-native streaming pipeline.", {"size": 12.5, "color": INK2, "space_after": 12}),
    ("Approach", {"bold": True, "size": 13.5, "space_after": 2}),
    ("Kafka ingestion → Kafka Streams filtering & windowing → in-flight feature engineering → ML/DL detection → alert topic → Elasticsearch → dashboard & emergency channels.", {"size": 12.5, "color": INK2, "space_after": 12}),
    ("Deliverable", {"bold": True, "size": 13.5, "space_after": 2}),
    ("An evaluated, end-to-end real-time system with measured latency, throughput and detection quality.", {"size": 12.5, "color": INK2}),
])

# ============================================================ 04 INTRO 2
s = slide()
header(s, "01 Introduction", "Batch ML vs. Real-Time Streaming: The Core Motivation")
table(s, Inches(0.55), Inches(1.8), Inches(12.2), Inches(3.1), [
    ["Aspect", "Batch pipeline (typical)", "Real-time streaming pipeline (this project)"],
    ["Data latency", "Hours to days", "Milliseconds to seconds"],
    ["Feature freshness", "Recomputed nightly", "Rolling aggregates updated per event"],
    ["Alert path", "Post-hoc reports", "Kafka alert topic → SMS / e-mail / push instantly"],
    ["Failure mode", "Silent staleness", "Backpressure, DLQ, replay, exactly-once"],
    ["Retraining trigger", "Manual schedules", "Live drift detection on the stream"],
], col_w=[2.6, 4.6, 5.0], fs=12.5)
text(s, Inches(0.55), Inches(5.25), Inches(12.2), Inches(1.5), [
    ("Streaming is not just faster batch - it changes the architecture:", {"bold": True, "size": 14.5, "space_after": 4}),
    ("Topics decouple producers from consumers; windows formalize time; the model becomes a stateful stream operator; alerts become first-class stream data. Kafka provides all four primitives natively.",
     {"size": 13.5, "color": INK2}),
])

# ============================================================ 05 DATASET
s = slide()
header(s, "02 Dataset & EDA", "Dataset: gridMET-Based Daily Fire-Weather Records")
rows = [["Group", "Features", "Meaning"]]
rows.append(["Geospatial", "latitude, longitude", "CONUS grid cell (~4 km) location"])
rows.append(["Meteorology", "pr, rmax, rmin, sph, srad, tmmn, tmmx, vs",
             "Precipitation, humidity bounds, specific humidity, radiation, min/max temperature, wind speed"])
rows.append(["Fuel moisture", "fm100, fm1000", "100-hr / 1000-hr dead-fuel moisture content"])
rows.append(["Fire-danger indices", "bi, erc, vpd, etr, pet",
             "Burning index, energy release component, vapour pressure deficit, evapotranspiration"])
rows.append(["Label", "Wildfire (Yes/No)", "Fire occurrence for the cell-day"])
table(s, Inches(0.55), Inches(1.75), Inches(8.3), Inches(3.6), rows,
      col_w=[1.7, 2.6, 4.0], fs=11, header_fs=12)
stats = [
    ("250K", "cell-day records (training sample of the full grid)"),
    ("17 + 1", "numeric features + binary label"),
    ("2014–2023", "ten years of daily observations"),
    ("≈5.6%", "positive fire rate - heavily imbalanced"),
]
yy = Inches(1.75)
for v, d in stats:
    box(s, Inches(9.15), yy, Inches(3.6), Inches(1.02), fill=PANEL)
    text(s, Inches(9.35), yy + Inches(0.10), Inches(3.2), Inches(0.4),
         v, size=19, bold=True, color=ORANGE)
    text(s, Inches(9.35), yy + Inches(0.52), Inches(3.3), Inches(0.44),
         d, size=10.5, color=INK2)
    yy += Inches(1.16)
text(s, Inches(0.55), Inches(5.6), Inches(12.2), Inches(1.2), [
    ("Source: gridMET gridded surface meteorology (Abatzoglou, 2013) joined with fire-occurrence labels; cleaning follows the project pipeline - 32767 sentinel values mapped to NaN, median imputation, SMOTEN balancing on the training split only.",
     {"size": 12.5, "color": INK2}),
    ("Note: EDA charts on the following slides were produced from a schema-compatible calibrated sample; rerun eda/eda.py against the full CSV to regenerate them with production numbers.",
     {"size": 10, "italic": True, "color": MUTED}),
])

# ============================================================ 06-11 EDA
def eda_slide(kicker, title, charts, findings, accent=AQUA):
    s = slide()
    header(s, kicker, title, accent=accent)
    n = len(charts)
    if n == 2:
        w = Inches(6.05)
        pic_fit(s, CHARTS / charts[0], Inches(0.55), Inches(1.62), w, Inches(3.55))
        pic_fit(s, CHARTS / charts[1], Inches(6.75), Inches(1.62), w, Inches(3.55))
    fy = Inches(5.35)
    box(s, Inches(0.55), fy, Inches(12.2), Inches(1.5), fill=PANEL)
    text(s, Inches(0.8), fy + Inches(0.12), Inches(1.5), Inches(0.3),
         "FINDINGS", size=11.5, bold=True, color=accent)
    text(s, Inches(0.8), fy + Inches(0.45), Inches(11.7), Inches(1.0),
         findings, size=13, color=INK)
    return s

eda_slide("02 Dataset & EDA", "EDA 1/6 - Target Imbalance & Data Quality",
          ["01_class_balance.png", "02_missing_values.png"],
          "Only ~5.6% of cell-days are fire events → accuracy alone is meaningless; the pipeline uses SMOTEN balancing and is evaluated recall-first.  •  Sentinel value 32767 plus sporadic NaNs affect up to ~1.2% of feature cells → median imputation and RobustScaler are mandatory before modelling.")

eda_slide("02 Dataset & EDA", "EDA 2/6 - Feature Distributions & Class Separation",
          ["03_feature_distributions.png", "04_boxplots_by_class.png"],
          "Temperature, VPD and ERC are right-skewed with heavy upper tails on fire days; fuel moisture shifts left. Fire days show systematically higher tmmx/vpd/ERC/wind and lower rmin/fm100 - a physically interpretable, learnable signal.")

eda_slide("02 Dataset & EDA", "EDA 3/6 - Correlation Structure & Seasonality",
          ["05_correlation_heatmap.png", "06_monthly_seasonality.png"],
          "pet-etr (r=+0.99), fm100-fm1000 (r=+0.96) and tmmx-vpd-erc (r≈0.8-0.9) are near-redundant → regularized models and index-based feature selection.  •  Fire occurrence peaks in July at ~21.6% (vs ≈0% in winter) → seasonal features and month-aware validation splits.")

eda_slide("02 Dataset & EDA", "EDA 4/6 - Spatial Density & Discriminative Features",
          ["08_spatial_fire_density.png", "10_feature_class_shift.png"],
          "Fire density concentrates in the mountain West / Mediterranean climate zones (Sierra Nevada, Cascades, Rockies, AZ-NM) → location is informative but must not leak.  •  Standardized class-mean shift ranks VPD (+1.54 SD), ERC (+1.50), BI (+1.35) and fm100 (-1.39) as the strongest single separators.", accent=BLUE)

eda_slide("02 Dataset & EDA", "EDA 5/6 - Fire-Weather Signal & Precipitation Behaviour",
          ["09_kde_fire_weather.png", "13_pr_zero_inflation.png"],
          "Class-conditional densities of VPD and ERC separate cleanly while fm100 mirrors the shift → the deployed threshold on model probability can be tuned against ERC-style operating points.  •  Precipitation is zero-inflated (82% zeros; 92% of fire days are dry) → a dry-day flag is a cheap, powerful binary feature.")

eda_slide("02 Dataset & EDA", "EDA 6/6 - Outliers & Stream-Level Temporal Structure",
          ["11_outliers_iqr.png", "12_daily_fire_bursts.png"],
          "Up to ~18% of precipitation records fall beyond IQR fences → RobustScaler in the production pipeline.  •  Fire arrivals come in multi-day bursts (7-day rolling mean) → exactly the temporal structure that Kafka Streams windowed aggregation (1-min tumbling, 24-h sliding) is designed to capture.", accent=BLUE)

# ============================================================ 12 EDA SUMMARY
s = slide()
header(s, "02 Dataset & EDA", "EDA Conclusions That Shape the Design")
concl = [
    ("Imbalance is structural", "5.6% positive rate → SMOTEN on train split only; recall-first metrics; precision-recall curves over ROC."),
    ("Fire-weather indices dominate", "VPD, ERC, BI and fuel moisture carry most of the signal → they become the engineered streaming features."),
    ("Near-redundant features exist", "pet↔etr r=+0.99 → drop or combine in-flight to keep stream payloads lean."),
    ("Strong seasonality & geography", "July peak ~21.6%; western-US clustering → calendar and spatial features, grouped keys for Kafka partitioning."),
    ("Bursty arrivals", "Multi-day fire bursts → windowed aggregation and alert deduplication are required, not optional."),
    ("Data quality is manageable", "32767 sentinels + <1.2% missing → validation + median imputation as the first Kafka Streams operator."),
]
y = Inches(1.8)
for t, d in concl:
    box(s, Inches(0.55), y, Inches(0.09), Inches(0.72), fill=ORANGE)
    text(s, Inches(0.85), y + Inches(0.0), Inches(11.7), Inches(0.34),
         t, size=14.5, bold=True, color=INK)
    text(s, Inches(0.85), y + Inches(0.33), Inches(11.7), Inches(0.38),
         d, size=12.5, color=INK2)
    y += Inches(0.85)

# ============================================================ 13 LIT MAP
s = slide()
header(s, "03 Literature Review", "Review Map: 34 Papers, 7 Themes")
themes = [
    ("A", "ML for fire occurrence & susceptibility", "5 papers", "Cortez'07 · Jain'20 · Sayad'19 · Shmuel'22 · Bot'22", BLUE),
    ("B", "Deep learning & spatiotemporal spread", "9 papers", "Shi'15 · Radke'19 · Huot'22 · Marjani'24 · +5", AQUA),
    ("C", "Real-time vision & edge detection", "5 papers", "Alkhatib'14 · Barmpoutis'20 · YOLOv11'25 · DPMNet'25 · EdgeUAV'25", ORANGE),
    ("D", "IoT & wireless sensor networks", "3 papers", "Chauhan'13 · Mohapatra'16 · Davis'23", ORANGE),
    ("E", "Kafka & stream processing frameworks", "5 papers", "Kreps'11 · Zaharia'12 · Isah'19 · Nasiri'19 · vanDongen'21", RED),
    ("F", "Concept drift in data streams", "3 papers", "Wares'19 · Krawczyk'18 · Korycki'22", VIOLET),
    ("G", "Fire weather & data foundations", "4 papers", "Abatzoglou'13 · Sakr'10 · Martin-Gomez'26 · Akdagli'26", RGBColor(0x6B, 0x6B, 0x6B)),
]
y = Inches(1.72)
for tag, name, count, who, c in themes:
    box(s, Inches(0.55), y, Inches(0.62), Inches(0.60), fill=c)
    text(s, Inches(0.55), y + Inches(0.13), Inches(0.62), Inches(0.35),
         tag, size=16, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    text(s, Inches(1.4), y + Inches(0.02), Inches(8.6), Inches(0.34),
         name, size=15, bold=True, color=INK)
    text(s, Inches(1.4), y + Inches(0.35), Inches(8.6), Inches(0.3),
         who, size=11.5, color=INK2)
    text(s, Inches(10.6), y + Inches(0.13), Inches(2.1), Inches(0.34),
         count, size=13, bold=True, color=c, align=PP_ALIGN.RIGHT)
    y += Inches(0.72)

# ============================================================ 14-20 LIT DETAIL
def lit_slide(kicker, title, theme_color, papers):
    s = slide()
    header(s, kicker, title, accent=theme_color)
    n = len(papers)
    top, bottom = 1.62, 6.98
    step = (bottom - top) / n
    ch = step - 0.08
    sz_c = 10.5 if n >= 5 else 11.5
    sz_b = 9.5 if n >= 5 else 10.5
    for i, (cite, contrib, rel) in enumerate(papers):
        y = Inches(top + i * step)
        box(s, Inches(0.55), y, Inches(12.2), Inches(ch), fill=PANEL)
        box(s, Inches(0.55), y, Inches(0.07), Inches(ch), fill=theme_color)
        text(s, Inches(0.8), y + Inches(0.07), Inches(11.75), Inches(ch - 0.12), [
            (cite, {"size": sz_c, "bold": True, "color": INK, "space_after": 2}),
            (f"Findings:  {contrib}", {"size": sz_b, "color": INK2, "space_after": 2}),
            (f"Relevance:  {rel}", {"size": sz_b, "color": INK2, "space_after": 0}),
        ])
    return s

lit_slide("03 Literature Review", "Theme A - Machine Learning for Fire Occurrence & Susceptibility", BLUE, [
    ("Cortez & Morais (2007). A Data Mining Approach to Predict Forest Fires using Meteorological Data. New Trends in Artificial Intelligence (EPIA).",
     "Data-mining baseline on the Fire-Weather-Index dataset; SVM tuned via grid search; sparse-small-fire problem documented.",
     "Direct ancestor of our tabular setup - fire occurrence from daily weather only."),
    ("Jain et al. (2020). A review of machine learning applications in wildfire science and management. Environmental Reviews, 28(4).",
     "Landmark survey (700+ citations) mapping ML to occurrence, spread, detection and fuels; flags data quality and validation gaps.",
     "Frames our model-choice rationale and evaluation protocol."),
    ("Sayad, Mousannif & Al Moatassime (2019). Predictive modeling of wildfires: a new dataset and machine learning approach. Fire Safety Journal, 104.",
     "Fuses GIS + weather + remotely-sensed features; ANN and LDA compared; releases a reusable dataset.",
     "Validates multi-source feature fusion like our gridMET+label join."),
    ("Shmuel & Heifetz (2022). Global wildfire susceptibility mapping based on machine learning models. Forests, 13(6).",
     "Benchmarks susceptibility models globally; identifies dominant drivers (dryness, temperature, human pressure).",
     "Supports using fire-danger indices (ERC, VPD) as first-class features."),
    ("Bot & Borges (2022). A Systematic Review of Applications of Machine Learning Techniques for Wildfire Fire Management. Inventions, 7(3).",
     "Systematic PRISMA-style review of ML for wildfire management tasks and metrics.",
     "Basis for our recall-first metric policy under imbalance."),
])

lit_slide("03 Literature Review", "Theme B - Deep Learning & Spatiotemporal Fire Prediction (1/2)", AQUA, [
    ("Shi, Chen, Wang, Yeung, Wong & Woo (2015). Convolutional LSTM Network: A Machine Learning Approach for Precipitation Nowcasting. NeurIPS 28.",
     "Introduces ConvLSTM - convolutional recurrent cells that encode spatiotemporal grids; the nowcasting paradigm.",
     "Architecture of our ConvLSTM spatial model (10×10 grid)."),
    ("Radke, Hessler & Long (2019). FireCast: Leveraging Deep Learning to Predict Wildfire Spread. IJCAI 2019.",
     "CNN+RNN hybrid over weather stacks beats logistic-regression baselines for 2-3h spread prediction.",
     "Evidence that DL beats classical ML on the same features."),
    ("Huot et al. (2022). Next Day Wildfire Spread: A Machine Learning Dataset to Predict Wildfire Spreading from Remote-Sensing Data. IEEE TGRS, 60.",
     "Google's 1-km next-day dataset: weather, vegetation, topography; strong baselines (logistic regression is surprisingly competitive).",
     "Template for our feature set and next-cell-day prediction task."),
    ("Marjani et al. (2024). CNN-BiLSTM: A Novel Deep Learning Model for Near-Real-Time Daily Wildfire Spread. Remote Sensing, 16(8).",
     "Hybrid convolutional + bidirectional LSTM for daily spread at 1 km with near-real-time intent.",
     "Direct architectural cousin of our LSTM/ConvLSTM serving tier."),
])

lit_slide("03 Literature Review", "Theme B - Deep Learning & Spatiotemporal Fire Prediction (2/2)", AQUA, [
    ("Andrianarivony & Akhloufi (2024). Machine Learning and Deep Learning for Wildfire Spread Prediction: A Review. Fire, 7(2).",
     "Taxonomy of spread-prediction architectures (CNN, RNN, GNN, transformers) and datasets.",
     "Justifies evaluating several model families rather than one."),
    ("Nguyen Van & Lee (2025). Optimizing Stacked Ensemble Machine Learning Models for Accurate Wildfire Spread Prediction. Remote Sensing, 17(9).",
     "Stacked ensembles with base-learner diversity improve spread accuracy and calibration.",
     "Motivates ensembling RF + XGBoost for the streaming tier."),
    ("Anastasiou & Kondylatos (2026). Wildfire Spread Forecasting With Deep Learning. IEEE Access.",
     "Large-scale DL forecasting of spread with multi-source inputs and ablations.",
     "Confirms DL readiness for operational cadences."),
    ("Kim & Hu (2025). Assessment of deep learning models integrated with weather and environmental variables for wildfire spread prediction. Natural Hazards.",
     "Ablation study quantifying how weather covariates improve DL spread models.",
     "Guides which rolling aggregates our windowing must emit."),
    ("Zakari & Malik (2025). An Enhanced Wildfire Spread Prediction Using Multimodal Satellite Imagery and Deep Learning. Remote Sens. Appl.: Soc. Environ.",
     "Multimodal satellite fusion (optical + thermal) with DL improves detection-to-prediction pipelines.",
     "Roadmap for adding imagery to our weather-only stream."),
])

lit_slide("03 Literature Review", "Theme C - Real-Time Fire & Smoke Detection: Vision and Edge", ORANGE, [
    ("Alkhatib (2014). A Review on Forest Fire Detection Techniques. International Journal of Distributed Sensor Networks.",
     "Compares satellite, camera, and sensor-node detection; discusses latency and false-alarm trade-offs.",
     "Core motivation for sub-minute, stream-native detection."),
    ("Barmpoutis et al. (2020). A Review on Early Forest Fire Detection Systems Using Optical Remote Sensing. Sensors, 20(22).",
     "Survey of optical/IR early-detection systems; benchmarks detection delay and accuracy.",
     "Sets the latency targets our alerting tier must beat."),
    ("Tao & Li (2025). Improved Lightweight YOLOv11 Algorithm for Real-Time Forest Fire Detection. Electronics, 14.",
     "Pruned YOLOv11 variant keeps accuracy while meeting real-time budgets on edge hardware.",
     "Shows engineering pattern for low-latency inference operators."),
    ("Wang & Li (2025). DPMNet: A Remote Sensing Forest Fire Real-Time Detection Network Driven by Dual-Perspective Multiscale Features. IEEE TCAS-I.",
     "Dual-perspective multiscale detector for satellite fire pixels at video rate.",
     "Candidate future fusion input for the same Kafka backbone."),
    ("Mu & Yang (2025). Edge Computing-Based Real-Time Forest Fire Detection Using UAV Thermal and Color Imagery. IEEE JSTARS.",
     "Edge-deployed fusion of thermal + RGB on UAVs; on-board inference to cut round-trip latency.",
     "Reinforces compute-at-the-edge vs central - our producer-side filtering."),
])

lit_slide("03 Literature Review", "Theme D - IoT & Wireless Sensor Network Detection", ORANGE, [
    ("Chauhan, Semwal et al. (2013). Artificial neural network-based forest fire detection system using wireless sensor networks. IEEE INDICON.",
     "ANN over multi-sensor node readings (temperature, humidity, smoke) for on-node fire classification.",
     "Prototype of feature-based (not pixel) fire classification - our setup."),
    ("Mohapatra & Khilar (2016). Forest fire monitoring and detection of faulty nodes using wireless sensor networks. IEEE TENCON.",
     "Fault-tolerant WSN monitoring; separates sensor faults from real events to cut false alarms.",
     "Maps to our schema-validation and DLQ stage in Kafka Streams."),
    ("Davis, Shekaramiz et al. (2023). Desert/Forest Fire Detection Using Machine/Deep Learning Techniques. Fire, 6(3).",
     "Benchmark of ML/DL detectors across sensor and imagery modalities in arid and forest biomes.",
     "Confirms robustness of weather-feature ML across biomes."),
])

lit_slide("03 Literature Review", "Theme E - Kafka & Distributed Stream Processing", RED, [
    ("Kreps, Narkhede & Rao (2011). Kafka: A Distributed Messaging System for Log Processing. NetDB'11.",
     "The Kafka paper: partitioned, replicated append-only log; consumer groups; O(1) disk IO at scale.",
     "The backbone of stages 2-4 and 9 of our methodology."),
    ("Zaharia, Das, Li, Hunter, Shenker & Stoica (2012). Discretized Streams: Fault-Tolerant Streaming Computation at Scale. NSDI'12.",
     "Formalized micro-batch windowing with deterministic replay for fault tolerance.",
     "Theoretical grounding for our tumbling/sliding window design."),
    ("Isah, Abughofa, Mahfuz, Ajerla, Zulkernine & Khan (2019). A Survey of Distributed Data Stream Processing Frameworks. IEEE Access, 7.",
     "Comparative survey of Kafka, Spark, Flink, Storm semantics (delivery, state, windows).",
     "Basis for choosing Kafka Streams over external compute."),
    ("Nasiri, Nasehi & Divsalar (2019). Evaluation of distributed stream processing frameworks for IoT applications in Smart Cities. Journal of Big Data, 6.",
     "Benchmark: throughput/latency of Kafka-centric stacks under IoT event rates.",
     "Informs partition counts and latency expectations."),
    ("van Dongen & Van Den Poel (2021). A Performance Analysis of Fault Recovery in Stream Processing Frameworks. IEEE Access, 9.",
     "Quantifies recovery time and state-restore behaviour across frameworks.",
     "Drives our replication and checkpointing settings."),
])

lit_slide("03 Literature Review", "Theme F - Concept Drift in Data Streams", VIOLET, [
    ("Wares, Isaacs & Brooks (2019). Data stream mining: methods and challenges for handling concept drift. SN Applied Sciences, 1.",
     "Survey of drift types (sudden/gradual/seasonal) and detection methods for streams.",
     "Weather-to-fire relationships drift seasonally - motivates Evidently + Prefect loop."),
    ("Krawczyk et al. (2018). Combining active learning with concept drift detection for data stream mining. IEEE ICMLA.",
     "Labels selectively when drift is detected - cheaper adaptation.",
     "Blueprint for our drift-triggered retraining budget."),
    ("Korycki & Krawczyk (2022). Adversarial concept drift detection under poisoning attacks for robust data streams. Machine Learning, 111.",
     "Robust drift detection that resists manipulated streams.",
     "Security consideration for our public alert channel."),
])

lit_slide("03 Literature Review", "Theme G - Fire Weather & Data Foundations", RGBColor(0x6B, 0x6B, 0x6B), [
    ("Abatzoglou (2013). Development of gridded surface meteorological data for ecological applications and modelling. Int. J. of Climatology, 33.",
     "gridMET: 4-km daily gridded meteorology for CONUS - our dataset's source.",
     "Defines every feature our producer emits."),
    ("Sakr et al. (2010). Artificial intelligence for forest fire prediction. IEEE/ASME AIM.",
     "ANN/SVM occurrence prediction with limited meteorological inputs.",
     "Early proof that weather-only models are viable."),
    ("Martin-Gomez et al. (2026). Daily Fire Danger Prediction in Catalonia using Machine Learning. (in press).",
     "Operational daily fire-danger forecasting with ML at regional scale.",
     "Operational-cadence precedent for our daily predictions."),
    ("Akdagli et al. (2026). Machine learning for one day ahead prediction of high fire weather index conditions. Scientific Reports.",
     "One-day-ahead FWI exceedance prediction with ML.",
     "Analogous target: threshold exceedance → alerting."),
])

# ============================================================ 21 LIT SUMMARY TABLE
s = slide()
header(s, "04 Summary of Literature", "What 34 Papers Tell Us - Synthesis Table")
table(s, Inches(0.5), Inches(1.65), Inches(12.35), Inches(4.9), [
    ["Theme", "Representative works", "Consensus finding", "Implication for this project"],
    ["ML occurrence", "Cortez'07; Jain'20; Sayad'19", "Daily weather carries strong predictive signal; imbalance is the norm",
     "Weather-only features + SMOTEN + recall-first metrics"],
    ["DL / spatiotemporal", "Shi'15; Radke'19; Huot'22; Marjani'24", "Conv+recurrent hybrids outperform classical ML on grids",
     "ConvLSTM on 10×10 grids alongside RF/XGB"],
    ["Real-time detection", "Alkhatib'14; Barmpoutis'20; Tao'25", "Detection value decays in minutes; edge/stream processing required",
     "Sub-minute Kafka latency budget; DLQ validation"],
    ["IoT / WSN", "Chauhan'13; Mohapatra'16", "Sensor faults masquerade as events; validation cuts false alarms",
     "Schema-check + dedup as first stream operator"],
    ["Kafka / stream proc.", "Kreps'11; Zaharia'12; Isah'19; Nasiri'19", "Partitioned logs + windows give scale, replay, fault tolerance",
     "Kafka Streams with exactly-once, keyed by grid cell"],
    ["Drift / adaptation", "Wares'19; Krawczyk'18; Korycki'22", "Fire-weather relationships drift; adaptation must be triggered, not scheduled",
     "Evidently drift → Prefect retraining → MLflow registry"],
    ["Fire weather data", "Abatzoglou'13; FWI literature", "GridMET-style indices (ERC, VPD, fm100) are operational standards",
     "These indices are our engineered stream features"],
], col_w=[1.9, 2.9, 3.9, 3.65], fs=10.5, header_fs=11.5, row_h=Inches(0.52))

# ============================================================ 22 GAPS
s = slide()
header(s, "04 Summary of Literature", "Research Gaps → Our Contributions")
gaps = [
    ("Gap 1 - Prediction is batch, alerts are decoupled", "Most papers train offline and serve on demand; few wire the model into the message bus itself.",
     "Model served as a stateful stream operator; alerts emitted to wildfire.alerts automatically."),
    ("Gap 2 - Windowing & feature engineering rarely evaluated together", "Window semantics are described but their effect on detection quality is seldom measured.",
     "Explicit tumbling/sliding windows whose aggregates feed the model; measured end-to-end."),
    ("Gap 3 - Class imbalance handled casually in real-time settings", "Resampling is routine offline but rarely re-visited once serving on a live, drifting stream.",
     "Drift-aware retraining with SMOTEN refreshed on the current window."),
    ("Gap 4 - Latency rarely reported alongside accuracy", "Papers report AUC/F1 without the latency/throughput budget that operations need.",
     "Evaluation includes p50/p95 latency and sustained throughput, not just detection quality."),
    ("Gap 5 - MLOps loop missing from fire-DL literature", "Monitoring, retraining and versioning are seldom part of the published architecture.",
     "Evidently → Prefect → MLflow → K8s rollout is part of the design, not an afterthought."),
]
y = Inches(1.72)
for t, d, c in gaps:
    box(s, Inches(0.55), y, Inches(12.2), Inches(0.98), fill=PANEL)
    text(s, Inches(0.8), y + Inches(0.06), Inches(11.7), Inches(0.3),
         t, size=13, bold=True, color=RED)
    text(s, Inches(0.8), y + Inches(0.38), Inches(5.9), Inches(0.55),
         d, size=10.5, color=INK2)
    text(s, Inches(7.0), y + Inches(0.38), Inches(5.5), Inches(0.55),
         [("→  ", {"bold": True, "size": 10.5, "color": AQUA}), ], )
    text(s, Inches(7.35), y + Inches(0.38), Inches(5.25), Inches(0.55),
         c, size=10.5, color=INK2)
    y += Inches(1.06)

# ============================================================ 23 OBJECTIVES
s = slide()
header(s, "05 Objectives", "Six System Objectives", accent=VIOLET)
objs = [
    ("O1 - Continuous ingestion", "Stream gridMET fire-weather events via Kafka producers into partitioned, keyed topics with schema validation.", BLUE),
    ("O2 - Filtering & windowing", "Implement Kafka Streams operators: schema check, dedup, tumbling 1-min and sliding 24-h windows.", AQUA),
    ("O3 - In-flight feature engineering", "Derive rolling aggregates and fire-weather indices on the stream; keep payloads lean by dropping redundant features.", AQUA),
    ("O4 - Real-time ML/DL detection", "Serve RF/XGBoost and LSTM/ConvLSTM models with <2 s end-to-end latency and recall-first thresholds.", ORANGE),
    ("O5 - Tiered alerting & delivery", "Publish probability-tiered alerts (WATCH/WARNING/EMERGENCY) to the alert topic, index in Elasticsearch, surface on dashboard + emergency channels.", RED),
    ("O6 - Evaluation & adaptation", "Measure latency/throughput/detection quality continuously; retrain on drift via Evidently + Prefect + MLflow.", VIOLET),
]
y = Inches(1.75)
for i, (t, d, c) in enumerate(objs, 1):
    box(s, Inches(0.55), y, Inches(12.2), Inches(0.80), fill=PANEL)
    box(s, Inches(0.55), y, Inches(0.62), Inches(0.80), fill=c)
    text(s, Inches(0.55), y + Inches(0.22), Inches(0.62), Inches(0.4),
         f"O{i}", size=15, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    text(s, Inches(1.4), y + Inches(0.08), Inches(11.2), Inches(0.32),
         t, size=13.5, bold=True, color=INK)
    text(s, Inches(1.4), y + Inches(0.42), Inches(11.2), Inches(0.34),
         d, size=11.5, color=INK2)
    y += Inches(0.88)

# ============================================================ 24 METHODOLOGY DIAGRAM
s = slide()
header(s, "06 Proposed Methodology", "End-to-End Pipeline Architecture", accent=RED)
pic_fit(s, DIAGRAM, Inches(0.35), Inches(1.55), Inches(12.65), Inches(5.35))

# ============================================================ 25 KAFKA DESIGN
s = slide()
header(s, "06 Proposed Methodology", "Kafka Design Decisions", accent=RED)
table(s, Inches(0.55), Inches(1.7), Inches(12.2), Inches(4.4), [
    ["Element", "Design", "Rationale"],
    ["wildfire.raw topic", "6 partitions, replication 3, key = grid-cell id", "Per-cell ordering; parallelism across brokers"],
    ["wildfire.features topic", "3 partitions, compacted", "Latest window state per cell recoverable on restart"],
    ["wildfire.alerts topic", "3 partitions, key = region, retention 90 d", "Fan-out to ES, dashboard, SMS consumers independently"],
    ["Windowing", "Tumbling 1-min (aggregates); sliding 24-h (indices); session 30-min per cell", "Matches bursty fire arrivals found in EDA 6/6"],
    ["Delivery semantics", "Idempotent producer + transactions → exactly-once", "No duplicate emergency alerts, no lost events"],
    ["Failure handling", "DLQ topic for schema failures; watermark 2-min for late data", "Fault-tolerant like Mohapatra'16; replayable like Zaharia'12"],
    ["Consumer group", "stream-processors (Kafka Streams app)", "Horizontal scale; state stores for windowed features"],
], col_w=[2.5, 5.1, 4.6], fs=11, header_fs=12, row_h=Inches(0.5))

# ============================================================ 26 MODEL STACK
s = slide()
header(s, "06 Proposed Methodology", "Model Stack, Serving & Alert Logic", accent=RED)
table(s, Inches(0.55), Inches(1.7), Inches(7.4), Inches(3.3), [
    ["Model", "Input", "Role", "Status (MLflow)"],
    ["Random Forest + SMOTEN", "17 tabular features", "Fast baseline on stream", "AUC 0.65, recall 0.69"],
    ["XGBoost + SMOTEN", "17 tabular features", "Primary tabular detector", "AUC 0.67, recall 0.65"],
    ["LSTM (seq=7)", "7-day feature sequence", "Temporal pattern detector", "Training logged"],
    ["ConvLSTM (10×10 grid)", "Spatial grid sequence", "Spatial spread detector", "Training logged"],
], col_w=[2.1, 1.9, 1.9, 1.5], fs=10.5, header_fs=11, row_h=Inches(0.52))
box(s, Inches(8.3), Inches(1.7), Inches(4.45), Inches(3.3), fill=PANEL)
text(s, Inches(8.55), Inches(1.85), Inches(4.0), Inches(0.3),
     "ALERT TIERS (on Fire Prediction)", size=11.5, bold=True, color=RED)
tiers = [
    ("WATCH", "p > 0.50  and ERC > 45", BLUE),
    ("WARNING", "p > 0.70  and ERC > 60  and fm100 < 10", ORANGE),
    ("EMERGENCY", "p > 0.90  and wind > 8 m/s", RED),
]
ty = Inches(2.3)
for name, rule, c in tiers:
    box(s, Inches(8.55), ty, Inches(3.95), Inches(0.72), fill=WHITE, line=c, line_w=1.5)
    text(s, Inches(8.75), ty + Inches(0.08), Inches(3.6), Inches(0.3),
         name, size=12.5, bold=True, color=c)
    text(s, Inches(8.75), ty + Inches(0.38), Inches(3.6), Inches(0.3),
         rule, size=11, color=INK2)
    ty += Inches(0.86)
text(s, Inches(0.55), Inches(5.35), Inches(12.2), Inches(1.4), [
    ("Serving path: Kafka Streams → feature store topic → FastAPI inference (Docker/K8s, HPA) → probability + class → alert topic. Thresholds are tuned for recall ≥ 0.70 at acceptable precision, then adjusted per ERC operating point from EDA.",
     {"size": 12.5, "color": INK2}),
])

# ============================================================ 27 WORK COMPLETED
s = slide()
header(s, "07 Work Completed", "Module-wise Progress ≈ 55% Overall")
mods = [
    ("Data preprocessing & cleaning (chunked, sentinel/NaN, scaling)", 100, AQUA),
    ("EDA & feature analysis (13 charts, this deck)", 100, AQUA),
    ("ML models - RF & XGBoost with SMOTEN (MLflow-tracked)", 100, BLUE),
    ("DL models - LSTM & ConvLSTM (training + eval)", 80, BLUE),
    ("Inference service - FastAPI + Docker + K8s + HPA", 100, ORANGE),
    ("Monitoring & retraining - Evidently drift + Prefect + MLflow", 90, ORANGE),
    ("Dashboard - React frontend (live feed, drift, retrain pages)", 90, ORANGE),
    ("Kafka producer & raw topic (replaces HTTP stub)", 20, RED),
    ("Kafka Streams - filtering, windowing, feature engineering", 10, RED),
    ("Alert topic + Elasticsearch + emergency channels", 0, RED),
    ("End-to-end streaming performance evaluation", 25, VIOLET),
]
y = Inches(1.62)
for name, pct, c in mods:
    text(s, Inches(0.55), y, Inches(6.4), Inches(0.3), name, size=11.5, color=INK)
    bar_w = Inches(4.2)
    box(s, Inches(7.15), y + Inches(0.04), bar_w, Inches(0.22), fill=RGBColor(0xE9, 0xE8, 0xE3))
    box(s, Inches(7.15), y + Inches(0.04), Emu(int(bar_w * pct / 100)), Inches(0.22), fill=c)
    text(s, Inches(11.5), y, Inches(1.2), Inches(0.3), f"{pct}%",
         size=11.5, bold=True, color=c)
    y += Inches(0.44)
box(s, Inches(0.55), Inches(6.6), Inches(12.2), Inches(0.55), fill=PANEL)
text(s, Inches(0.8), Inches(6.72), Inches(11.7), Inches(0.35),
     "Overall: the batch ML/MLOps core is complete and deployed; the Kafka streaming and alerting layers are the active build.",
     size=12.5, bold=True, color=INK)

# ============================================================ 28 PLAN
s = slide()
header(s, "07 Work Completed", "Remaining Plan - Four Phases to Completion")
phases = [
    ("Phase 1 (2 wks)", "Kafka foundation", "KRaft cluster (3 brokers) in Docker; wildfire.raw producer from gridMET feed; schema registry; DLQ.", RED),
    ("Phase 2 (3 wks)", "Stream processing", "Kafka Streams app: validation, dedup, 1-min tumbling + 24-h sliding windows, rolling feature aggregates.", ORANGE),
    ("Phase 3 (2 wks)", "Alerting & search", "Fire Prediction → wildfire.alerts; Elasticsearch indexing; Kibana board; dashboard + SMS/e-mail dispatch.", BLUE),
    ("Phase 4 (2 wks)", "Evaluation", "Latency p50/p95, throughput at 1k/10k events/s, detection quality on replayed data, drift-triggered retrain E2E.", VIOLET),
]
y = Inches(1.8)
for t, name, d, c in phases:
    box(s, Inches(0.55), y, Inches(2.2), Inches(1.05), fill=c)
    text(s, Inches(0.55), y + Inches(0.22), Inches(2.2), Inches(0.6),
         t, size=13, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    box(s, Inches(2.95), y, Inches(9.8), Inches(1.05), fill=PANEL)
    text(s, Inches(3.2), y + Inches(0.10), Inches(9.3), Inches(0.32),
         name, size=13.5, bold=True, color=INK)
    text(s, Inches(3.2), y + Inches(0.44), Inches(9.3), Inches(0.55),
         d, size=11.5, color=INK2)
    y += Inches(1.2)
text(s, Inches(0.55), Inches(6.7), Inches(12.2), Inches(0.5),
     "Target: fully streaming end-to-end demo + evaluation report within 9 weeks.",
     size=13, bold=True, color=INK2)

# ============================================================ 29-30 REFERENCES
REFS_A = [
    "Abatzoglou, J. T. (2013). Development of gridded surface meteorological data for ecological applications and modelling. Int. J. of Climatology, 33, 121-131.",
    "Alkhatib, A. A. A. (2014). A review on forest fire detection techniques. Int. J. of Distributed Sensor Networks, 10(3).",
    "Anastasiou, N., & Kondylatos, S. (2026). Wildfire spread forecasting with deep learning. IEEE Access.",
    "Andrianarivony, H. S., & Akhloufi, M. A. (2024). Machine learning and deep learning for wildfire spread prediction: A review. Fire, 7(2).",
    "Barmpoutis, P., Papaioannou, P., Dimitropoulos, K., & Voulodimos, D. (2020). A review on early forest fire detection systems using optical remote sensing. Sensors, 20(22), 6442.",
    "Bot, K., & Borges, J. G. (2022). A systematic review of applications of machine learning techniques for wildfire fire management. Inventions, 7(3), 84.",
    "Chauhan, A., Semwal, S., et al. (2013). Artificial neural network-based forest fire detection system using wireless sensor networks. IEEE INDICON.",
    "Cortez, P., & Morais, A. (2007). A data mining approach to predict forest fires using meteorological data. New Trends in Artificial Intelligence (EPIA), 512-523.",
    "Davis, M., Shekaramiz, M., et al. (2023). Desert/forest fire detection using machine/deep learning techniques. Fire, 6(3), 113.",
    "Huot, F., Hu, R. L., Lakshmanan, V., & Bhatia, A. (2022). Next day wildfire spread: A machine learning dataset to predict wildfire spreading from remote-sensing data. IEEE TGRS, 60.",
    "Isah, H., Abughofa, T., Mahfuz, S., Ajerla, D., Zulkernine, F., & Khan, S. (2019). A survey of distributed data stream processing frameworks. IEEE Access, 7, 154300-154316.",
    "Jain, P., Coogan, S. C. P., Subramanian, S. G., Crowley, M., Taylor, S., & Flannigan, M. D. (2020). A review of machine learning applications in wildfire science and management. Environmental Reviews, 28(4), 478-505.",
]
REFS_B = [
    "Kim, J., & Hu, Y. (2025). Assessment of deep learning models integrated with weather and environmental variables for wildfire spread prediction. Natural Hazards.",
    "Korycki, L., & Krawczyk, B. (2022). Adversarial concept drift detection under poisoning attacks for robust data streams. Machine Learning, 111.",
    "Krawczyk, B., Pfahringer, B., et al. (2018). Combining active learning with concept drift detection for data stream mining. IEEE ICMLA.",
    "Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. Proc. NetDB'11.",
    "Marjani, M., Mahdianpari, M., & Mohammadimanesh, F. (2024). CNN-BiLSTM: A novel deep learning model for near-real-time daily wildfire spread prediction. Remote Sensing, 16(8), 1357.",
    "Martin-Gomez, V., et al. (2026). Daily fire danger prediction in Catalonia using machine learning. (in press).",
    "Mohapatra, S., & Khilar, P. M. (2016). Forest fire monitoring and detection of faulty nodes using wireless sensor networks. IEEE TENCON.",
    "Mu, L., & Yang, Y. (2025). Edge computing-based real-time forest fire detection using UAV thermal and color imagery. IEEE JSTARS.",
    "Nasiri, H., Nasehi, S., & Divsalar, M. (2019). Evaluation of distributed stream processing frameworks for IoT applications in Smart Cities. Journal of Big Data, 6, 61.",
    "Nguyen Van, L., & Lee, G. (2025). Optimizing stacked ensemble machine learning models for accurate wildfire spread prediction. Remote Sensing, 17(9).",
    "Radke, D., Hessler, A., & Long, D. (2019). FireCast: Leveraging deep learning to predict wildfire spread. IJCAI 2019, 4575-4581.",
    "Sakr, G. E., Elhajj, I. H., Mitri, G., & Wehbe, C. S. (2010). Artificial intelligence for forest fire prediction. IEEE/ASME Int. Conf. on Advanced Intelligent Mechatronics.",
    "Sayad, Y. O., Mousannif, H., & Al Moatassime, H. (2019). Predictive modeling of wildfires: A new dataset and machine learning approach. Fire Safety Journal, 104, 130-146.",
    "Shi, X., Chen, Z., Wang, H., Yeung, D.-Y., Wong, W.-K., & Woo, W.-C. (2015). Convolutional LSTM network: A machine learning approach for precipitation nowcasting. NeurIPS 28.",
    "Shmuel, A., & Heifetz, E. (2022). Global wildfire susceptibility mapping based on machine learning models. Forests, 13(6), 908.",
    "Tao, Y., & Li, B. (2025). Improved lightweight YOLOv11 algorithm for real-time forest fire detection. Electronics, 14.",
    "van Dongen, G., & Van Den Poel, D. (2021). A performance analysis of fault recovery in stream processing frameworks. IEEE Access, 9.",
    "Wang, G., & Li, H. (2025). DPMNet: A remote sensing forest fire real-time detection network driven by dual-perspective multiscale features. IEEE TCAS-I.",
    "Wares, S., Isaacs, J., & Brooks, E. (2019). Data stream mining: methods and challenges for handling concept drift. SN Applied Sciences, 1, 242.",
    "Zaharia, M., Das, T., Li, H., Hunter, T., Shenker, S., & Stoica, I. (2012). Discretized streams: Fault-tolerant streaming computation at scale. NSDI'12.",
    "Zakari, R., & Malik, O. A. (2025). An enhanced wildfire spread prediction using multimodal satellite imagery and deep learning. Remote Sens. Appl.: Soc. Environ.",
    "Akdagli, A., et al. (2026). Machine learning for one day ahead prediction of high fire weather index conditions. Scientific Reports.",
]

def ref_slide(refs, part, start_idx, end_idx):
    s = slide()
    header(s, "References", f"References ({part}) - Papers {start_idx}-{end_idx}", accent=MUTED)
    runs = []
    for i, r in enumerate(refs, start_idx):
        runs.append((f"[{i}]  {r}", {"size": 10.5, "color": INK2, "space_after": 7, "spacing": 1.05}))
    text(s, Inches(0.6), Inches(1.7), Inches(12.2), Inches(5.2), runs)

ref_slide(REFS_A, "1/2", 1, len(REFS_A))
ref_slide(REFS_B, "2/2", len(REFS_A) + 1, len(REFS_A) + len(REFS_B))

# ============================================================ 31 THANK YOU
s = slide()
box(s, 0, 0, SW, SH, fill=RGBColor(0x11, 0x16, 0x22))
box(s, 0, Inches(4.3), SW, Pt(3), fill=ORANGE)
text(s, Inches(0.9), Inches(3.0), Inches(11.5), Inches(1.0),
     "Thank You", size=48, bold=True, color=WHITE)
text(s, Inches(0.9), Inches(4.6), Inches(11.5), Inches(1.5), [
    ("Questions & discussion", {"size": 16, "color": RGBColor(0xC3, 0xC2, 0xB7), "space_after": 8}),
    ("Real-Time Forest Fire Detection System  |  Kafka · Kafka Streams · ML/DL · Elasticsearch", {"size": 13, "color": RGBColor(0x8B, 0x93, 0xA6)}),
])

prs.save(OUT)
print(f"saved {OUT} with {len(prs.slides.slides if hasattr(prs.slides,'slides') else prs.slides._sldIdLst)} slides")
