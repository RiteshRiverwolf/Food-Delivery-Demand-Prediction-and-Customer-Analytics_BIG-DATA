"""Stage 5 - Charts for the report, drawn from the small Gold summary CSVs.

Usage: python 05_charts.py [--reports outputs/pipeline]
"""
import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
SEQ = LinearSegmentedColormap.from_list("blue", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.size": 10, "text.color": INK, "axes.labelcolor": INK_2, "axes.edgecolor": GRID,
    "xtick.color": INK_2, "ytick.color": INK_2, "axes.titleweight": "bold", "axes.titlesize": 12,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.axisbelow": True,
})


def demand_heatmap(src, out):
    df = pd.read_csv(f"{src}/demand_by_time.csv")
    grid = df.pivot(index="day_name", columns="order_hour", values="orders")
    days = df.drop_duplicates("order_dow").sort_values("order_dow")["day_name"]
    grid = grid.loc[days] / 1000
    fig, ax = plt.subplots(figsize=(11, 3.6))
    im = ax.imshow(grid, aspect="auto", cmap=SEQ)
    ax.set_xticks(range(24), [f"{h:02d}" for h in range(24)])
    ax.set_yticks(range(7), grid.index)
    ax.grid(False)
    ax.set_xlabel("Hour of day")
    peak = df.loc[df["orders"].idxmax()]
    ax.set_title(f"Order demand by day and hour (thousands of orders). "
                 f"Peak: {peak.day_name} {int(peak.order_hour):02d}:00, {int(peak.orders):,} orders", loc="left")
    ax.add_patch(plt.Rectangle((peak.order_hour - 0.5, peak.order_dow - 0.5), 1, 1, fill=False, ec="#eb6834", lw=2))
    cb = fig.colorbar(im, ax=ax, pad=0.01)
    cb.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(f"{out}/01_demand_heatmap.png", dpi=200)


def department_chart(src, out):
    df = pd.read_csv(f"{src}/department_kpis.csv").sort_values("items_ordered")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 6), sharey=True, gridspec_kw={"wspace": 0.08})
    a1.barh(df["department"], df["pct_of_items"], color=BLUE, height=0.7)
    a1.set_title("Share of all items ordered (%)", loc="left")
    for y, v in enumerate(df["pct_of_items"]):
        if v >= 3:
            a1.text(v + 0.4, y, f"{v:.1f}%", va="center", fontsize=8, color=INK_2)
    a2.barh(df["department"], df["reorder_rate"] * 100, color=BLUE, height=0.7)
    a2.set_title("Reorder rate (%)", loc="left")
    a2.axvline(df["reorder_rate"].mean() * 100, color=INK_2, lw=1, ls="--")
    a2.text(df["reorder_rate"].mean() * 100 + 1, -0.9, "average", fontsize=8, color=INK_2)
    for a in (a1, a2):
        a.grid(axis="y", visible=False)
        a.tick_params(axis="y", length=0)
    fig.suptitle("Departments: where demand and loyalty come from", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(f"{out}/02_departments.png", dpi=200)


def segment_chart(src, out):
    df = pd.read_csv(f"{src}/customer_segments.csv").iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 4))
    y = range(len(df))
    ax.barh([i + 0.19 for i in y], df["pct_customers"], height=0.36, color=BLUE, label="% of customers")
    ax.barh([i - 0.19 for i in y], df["pct_items"], height=0.36, color=ORANGE, label="% of items ordered")
    for i, (c, it) in enumerate(zip(df["pct_customers"], df["pct_items"])):
        ax.text(c + 0.5, i + 0.19, f"{c:.1f}%", va="center", fontsize=8, color=INK_2)
        ax.text(it + 0.5, i - 0.19, f"{it:.1f}%", va="center", fontsize=8, color=INK_2)
    ax.set_yticks(list(y), df["segment"])
    ax.grid(axis="y", visible=False)
    ax.legend(frameon=False, loc="lower right")
    ax.set_title("Customer segments: share of customers vs share of demand", loc="left")
    fig.tight_layout()
    fig.savefig(f"{out}/03_customer_segments.png", dpi=200)


def churn_chart(src, out):
    df = pd.read_csv(f"{src}/churn_signal_by_segment.csv").sort_values("pct_slowing_down")
    fig, ax = plt.subplots(figsize=(9, 3.4))
    ax.barh(df["segment"], df["pct_slowing_down"], color=BLUE, height=0.6)
    for i, (p, n) in enumerate(zip(df["pct_slowing_down"], df["slowing_down"])):
        ax.text(p + 0.2, i, f"{p:.1f}%  ({n:,} customers)", va="center", fontsize=8, color=INK_2)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("% of segment whose last 3 order gaps average 1.5x their usual gap (capped at 30 days)")
    ax.set_title("Early churn signal: customers who are slowing down", loc="left")
    ax.set_xlim(0, df["pct_slowing_down"].max() * 1.35)
    fig.tight_layout()
    fig.savefig(f"{out}/04_churn_signal.png", dpi=200)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports", default="outputs/pipeline")
    args = parser.parse_args()
    src = os.path.join(args.reports, "processing")
    out = os.path.join(args.reports, "figures")
    os.makedirs(out, exist_ok=True)
    for chart in (demand_heatmap, department_chart, segment_chart, churn_chart):
        chart(src, out)
    print(f"charts -> {out}")


if __name__ == "__main__":
    main()
