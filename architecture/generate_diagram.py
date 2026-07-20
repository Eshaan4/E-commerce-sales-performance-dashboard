"""
generate_diagram.py – Generate architecture diagram using matplotlib.
Creates architecture/pipeline_architecture.png
"""
import os
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
except ImportError:
    os.system("pip install matplotlib -q")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.patches import FancyBboxPatch


def draw_box(ax, x, y, w, h, text, color, text_color="white", fontsize=9, bold=False):
    box = FancyBboxPatch((x - w/2, y - h/2), w, h,
                          boxstyle="round,pad=0.05",
                          facecolor=color, edgecolor="white",
                          linewidth=1.5, zorder=3)
    ax.add_patch(box)
    weight = "bold" if bold else "normal"
    ax.text(x, y, text, ha="center", va="center", fontsize=fontsize,
            color=text_color, fontweight=weight, zorder=4, wrap=True,
            multialignment="center")


def draw_arrow(ax, x1, y1, x2, y2, color="#555555"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=2),
                zorder=2)


def main():
    fig, ax = plt.subplots(figsize=(20, 13))
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#0f1117")
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 13)
    ax.axis("off")

    # ── Title ──────────────────────────────────────────────────
    ax.text(10, 12.4, "Data Engineering PoC – Medallion Architecture",
            ha="center", va="center", fontsize=16, color="white",
            fontweight="bold")
    ax.text(10, 12.0, "Olist E-Commerce Dataset  |  Bronze → Silver → Gold",
            ha="center", va="center", fontsize=11, color="#aaaaaa")

    # ── Colors ─────────────────────────────────────────────────
    C_SOURCE  = "#2563eb"
    C_INGEST  = "#7c3aed"
    C_BRONZE  = "#92400e"
    C_VALID   = "#065f46"
    C_SILVER  = "#1e40af"
    C_GOLD    = "#92400e"
    C_GOLD2   = "#b45309"
    C_BI      = "#be185d"
    C_AIRFLOW = "#1e3a5f"
    C_META    = "#374151"

    # ── Column X positions ─────────────────────────────────────
    X = [1.5, 4.0, 6.5, 9.0, 11.5, 14.5, 17.5]

    # ─── Row 1: Source Systems ────────────────────────────────
    ax.text(X[0], 11.0, "SOURCE SYSTEMS", ha="center", color="#60a5fa",
            fontsize=8, fontweight="bold")
    sources = ["CSV\nFiles", "JSON\nFiles", "XML\nFiles", "REST API\n(Optional)"]
    sy = 9.8
    for i, s in enumerate(sources):
        draw_box(ax, X[0], sy - i * 0.9, 1.8, 0.65, s, C_SOURCE, fontsize=8)

    # ─── Row 1: Ingestion ─────────────────────────────────────
    ax.text(X[1], 11.0, "INGESTION", ha="center", color="#c084fc", fontsize=8, fontweight="bold")
    ing = ["csv_reader.py", "json_reader.py", "xml_reader.py", "postgres_reader.py"]
    for i, s in enumerate(ing):
        draw_box(ax, X[1], sy - i * 0.9, 2.0, 0.65, s, C_INGEST, fontsize=8)

    # ─── Bronze Layer ─────────────────────────────────────────
    ax.text(X[2], 11.0, "BRONZE LAYER", ha="center", color="#fbbf24",
            fontsize=8, fontweight="bold")
    bronze_items = ["bronze.customers", "bronze.orders", "bronze.products",
                    "bronze.order_items", "bronze.payments"]
    for i, b in enumerate(bronze_items):
        draw_box(ax, X[2], 10.0 - i * 0.85, 2.2, 0.65, b, C_BRONZE, fontsize=8)

    # Bronze audit label
    draw_box(ax, X[2], 5.6, 2.2, 0.55,
             "load_ts | batch_id\nsource_file | partition", "#78350f", fontsize=7)

    # ─── Validation ───────────────────────────────────────────
    ax.text(X[3], 11.0, "VALIDATION", ha="center", color="#34d399",
            fontsize=8, fontweight="bold")
    val_items = ["Null Checks", "Type Checks", "PK Uniqueness",
                 "Domain Checks", "Row Count", "Rejected Handler"]
    for i, v in enumerate(val_items):
        draw_box(ax, X[3], 10.0 - i * 0.85, 2.0, 0.65, v, C_VALID, fontsize=8)

    # ─── Silver Layer ─────────────────────────────────────────
    ax.text(X[4], 11.0, "SILVER LAYER", ha="center", color="#93c5fd",
            fontsize=8, fontweight="bold")
    silver_items = ["silver.customers\n(SCD Type 2)", "silver.orders",
                    "silver.products\n(SCD Type 1)", "silver.order_items",
                    "silver.payments", "silver.sellers\n(SCD Type 2)"]
    for i, s in enumerate(silver_items):
        draw_box(ax, X[4], 10.2 - i * 0.85, 2.2, 0.72, s, C_SILVER, fontsize=7.5)

    # ─── Gold Layer ───────────────────────────────────────────
    ax.text(X[5], 11.0, "GOLD LAYER", ha="center", color="#fcd34d",
            fontsize=8, fontweight="bold")
    gold_items = ["dim_customer", "dim_product", "dim_seller",
                  "fact_sales", "revenue_mart", "kpi_summary"]
    gold_colors = [C_GOLD2, C_GOLD2, C_GOLD2, "#92400e", "#78350f", "#78350f"]
    for i, g in enumerate(gold_items):
        draw_box(ax, X[5], 10.2 - i * 0.85, 2.0, 0.72, g, gold_colors[i], fontsize=8)

    # ─── BI / Output ──────────────────────────────────────────
    ax.text(X[6], 11.0, "OUTPUT / BI", ha="center", color="#f9a8d4",
            fontsize=8, fontweight="bold")
    bi_items = ["SQL Queries", "BI Dashboards", "KPI Reports", "Data Exports"]
    for i, b in enumerate(bi_items):
        draw_box(ax, X[6], 10.0 - i * 0.85, 2.0, 0.65, b, C_BI, fontsize=8)

    # ─── Arrows between columns ───────────────────────────────
    for y_pos in [9.8, 9.0, 8.3, 7.6]:
        draw_arrow(ax, X[0]+0.9, y_pos, X[1]-1.0, y_pos, "#4b5563")
    for y_pos in [9.8, 9.0, 8.3, 7.6]:
        draw_arrow(ax, X[1]+1.0, y_pos, X[2]-1.1, y_pos, "#4b5563")
    for i in range(5):
        y = 10.0 - i * 0.85
        draw_arrow(ax, X[2]+1.1, y, X[3]-1.0, y, "#22c55e")
    for i in range(5):
        y = 10.0 - i * 0.85
        draw_arrow(ax, X[3]+1.0, y, X[4]-1.1, y, "#60a5fa")
    for i in range(6):
        y = 10.2 - i * 0.85
        draw_arrow(ax, X[4]+1.1, y, X[5]-1.0, y, "#fbbf24")
    for i in range(4):
        y = 10.0 - i * 0.85
        draw_arrow(ax, X[5]+1.0, y, X[6]-1.0, y, "#f472b6")

    # ─── Bottom: Airflow + Metadata ───────────────────────────
    ax.text(5.0, 3.8, "ORCHESTRATION", ha="center", color="#93c5fd",
            fontsize=9, fontweight="bold")
    airflow_items = ["DAG: Bronze Ingestion\n(daily @00:00)",
                     "DAG: Silver Transform\n(daily @01:00)",
                     "DAG: Gold Aggregation\n(daily @02:00)"]
    for i, a in enumerate(airflow_items):
        draw_box(ax, 2.0 + i * 3.0, 3.0, 2.6, 0.9, a, C_AIRFLOW, fontsize=8)

    ax.text(13.5, 3.8, "CROSS-CUTTING", ha="center", color="#d1d5db",
            fontsize=9, fontweight="bold")
    meta_items = ["Metadata\nTracker", "DQ\nResults", "Error\nLogging",
                  "Watermark\nTracking", "Audit\nTables"]
    for i, m in enumerate(meta_items):
        draw_box(ax, 10.5 + i * 2.0, 3.0, 1.7, 0.9, m, C_META, fontsize=8)

    # ─── Dashed border for layers ─────────────────────────────
    for label, x, color in [
        ("BRONZE", 6.5, "#fbbf24"), ("SILVER", 11.5, "#60a5fa"), ("GOLD", 14.5, "#fcd34d")
    ]:
        rect = plt.Rectangle((x - 1.4, 4.5), 2.8, 6.8,
                              fill=False, edgecolor=color, linewidth=1,
                              linestyle="--", zorder=1, alpha=0.3)
        ax.add_patch(rect)

    # ─── Tech stack legend ────────────────────────────────────
    legend = [
        mpatches.Patch(color=C_SOURCE,  label="Source: CSV / JSON / XML"),
        mpatches.Patch(color=C_BRONZE,  label="Bronze: Immutable Raw"),
        mpatches.Patch(color=C_SILVER,  label="Silver: SCD1 / SCD2"),
        mpatches.Patch(color=C_GOLD2,   label="Gold: Dimensional Model"),
        mpatches.Patch(color=C_AIRFLOW, label="Airflow DAGs"),
    ]
    ax.legend(handles=legend, loc="lower center", ncol=5,
              facecolor="#1f2937", edgecolor="#374151",
              labelcolor="white", fontsize=8,
              bbox_to_anchor=(0.5, 0.01))

    plt.tight_layout(pad=0.5)
    out_path = Path(__file__).parent / "pipeline_architecture.png"
    plt.savefig(str(out_path), dpi=150, facecolor="#0f1117", bbox_inches="tight")
    print(f"✔ Architecture diagram saved: {out_path}")
    plt.close()


if __name__ == "__main__":
    main()
