"""BI-layer figures for the report/workbook: docs/figures/bi_architecture.png and docs/figures/bi_refresh_kpis.png."""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN

FIG = ROOT / "docs" / "figures"
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.spines.top": False, "axes.spines.right": False})


def box(ax, x, y, w, h, t, fc, fs=8, bold=False, color="black"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.1", fc=fc, ec="#44546A", lw=1, zorder=2))
    ax.text(x + w / 2, y + h / 2, t, ha="center", va="center", fontsize=fs, weight="bold" if bold else "normal", zorder=3,
            color=color)


def arrow(ax, x1, y1, x2, y2, label=None, dy=0.18):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", color="#44546A", lw=1.3), zorder=1)
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + dy, label, ha="center", fontsize=6.8, color="#333")


def architecture():
    fig, ax = plt.subplots(figsize=(11.5, 4.6))
    ax.set_xlim(0, 25); ax.set_ylim(0, 10); ax.axis("off")
    for x, t, c in [(0.3, "BRONZE", "#E8EEF7"), (4.6, "SILVER", "#FDF0DC"), (8.9, "GOLD", "#E9E3F5"),
                    (13.4, "SERVING", "#DCEFE3"), (18.1, "BI", "#FBE3E3")]:
        ax.text(x + 1.9, 9.5, t, ha="center", fontsize=10, weight="bold", color="#1F3864")
    box(ax, 0.3, 5.6, 3.8, 3.2, "Landing Excel files\n(file-watcher)\n+ streamed sales\nevents (API)", "#E8EEF7")
    box(ax, 4.6, 5.6, 3.8, 3.2, "Postgres staging\n7 ODS tables\nvalidated, typed,\nappend-only", "#FDF0DC")
    box(ax, 8.9, 5.6, 4.0, 3.2, "DuckDB SDI mart\nstar schema /\nfact constellation\n+ aggregates", "#E9E3F5")
    box(ax, 13.4, 5.6, 4.2, 3.2, "Postgres sdi_bi\n12 star tables + 7 BI\ntables + 2 views\natomic swap · bi_reader", "#DCEFE3")
    box(ax, 18.1, 5.6, 6.5, 3.2, "Apache Superset\n9 datasets · 47 charts\n4 dashboards · native filters\ncross-filter · drill · 30 s refresh", "#FBE3E3")
    arrow(ax, 4.1, 7.2, 4.6, 7.2); arrow(ax, 8.4, 7.2, 8.9, 7.2); arrow(ax, 12.9, 7.2, 13.4, 7.2); arrow(ax, 17.6, 7.2, 18.1, 7.2)
    steps = [("publish_bi_mart", "build BI tables,\ncopy + swap schema"), ("snapshot_bi_kpis", "21 KPIs per refresh\n-> ops.bi_kpi_snapshot"),
             ("sync_superset", "dashboards as code\n(idempotent)"), ("warm_bi_dashboards", "fetch every chart,\ntimed -> ops.query_log")]
    ax.text(12.5, 4.6, "Airflow DAG sdi_06_bi_refresh (triggered after every transform + last step of sdi_99_refresh_cycle)",
            ha="center", fontsize=8.5, weight="bold")
    for i, (t, d) in enumerate(steps):
        x = 1.2 + i * 6.0
        box(ax, x, 2.0, 4.6, 1.9, f"{t}\n{d}", "#2F5597", fs=7.6, color="white")
        if i < 3:
            arrow(ax, x + 4.6, 2.95, x + 6.0, 2.95)
    ax.text(12.5, 0.8, "Grafana (DE observability) reads ops.*: BI chart fetch times, refresh log, KPI snapshots",
            ha="center", fontsize=8, style="italic")
    fig.savefig(FIG / "bi_architecture.png", dpi=170, bbox_inches="tight"); plt.close(fig)


def refresh_kpis():
    with psycopg2.connect(PG_DSN) as c:
        k = pd.read_sql("SELECT refresh_tag, kpi_name, kpi_value FROM sdi_bi.bi_kpi_history", c)
    pick = [("Sales transactions in mart", "Sales transactions", "{:,.0f}"), ("Months of history", "Months of history", "{:.0f}"),
            ("Supply gap (packs)", "Supply gap (packs)", "{:,.0f}"), ("Unfulfilled demand (packs)", "Unfulfilled demand (packs)", "{:,.0f}"),
            ("Net revenue USD (normal postings)", "Net revenue (USD)", "${:,.0f}"), ("Medicines on sale", "Medicines on sale", "{:.0f}")]
    fig, axes = plt.subplots(1, 6, figsize=(12, 2.4))
    for ax, (kpi, title, fmt) in zip(axes, pick):
        d = k[k.kpi_name == kpi].set_index("refresh_tag").reindex(["BASE", "R1", "R2"]).kpi_value
        bars = ax.bar(d.index, d.values, color=["#9DB3D9", "#2F5597", "#1F3864"])
        lo = d.min() * 0.97 if d.min() > 0 else 0
        ax.set_ylim(lo, d.max() * 1.02)
        ax.set_title(title, fontsize=8.5)
        ax.tick_params(labelsize=7.5); ax.set_yticks([])
        for b, v in zip(bars, d.values):
            ax.text(b.get_x() + b.get_width() / 2, v, fmt.format(v), ha="center", va="bottom", fontsize=6.6)
    fig.suptitle("Dashboards after each refresh: BASE -> R1 -> R2 (ops.bi_kpi_snapshot)", fontsize=9.5, weight="bold", y=1.04)
    fig.tight_layout()
    fig.savefig(FIG / "bi_refresh_kpis.png", dpi=170, bbox_inches="tight"); plt.close(fig)


if __name__ == "__main__":
    architecture()
    if "--arch-only" not in sys.argv:
        refresh_kpis()
    print("ok")
