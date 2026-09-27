"""Render load-test charts from loadtest/results/*.json into docs/."""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "loadtest" / "results"
DOCS = ROOT / "docs"

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"  # validated categorical slots 1-3
REFERENCE = "#a3a29c"  # muted ink for the "before" comparison line
SCENARIO = {
    "uniform": ("Uniform (1,000 accounts)", BLUE),
    "hot": ("Hot source (all debit one account)", ORANGE),
    "hot_dest": ("Hot destination (all credit one account)", AQUA),
}


def _style(ax, title, xlabel, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=11, fontweight="bold", pad=10)
    ax.set_xlabel(xlabel, color=INK_2, fontsize=9)
    ax.set_ylabel(ylabel, color=INK_2, fontsize=9)
    ax.tick_params(colors=INK_2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def sweep_chart():
    runs = json.loads((RESULTS / "sweep.json").read_text())
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2), facecolor=SURFACE)
    for scen, (name, color) in SCENARIO.items():
        rs = [r for r in runs if r["scenario"] == scen]
        xs = [r["concurrency"] for r in rs]
        a1.plot(xs, [r["throughput_rps"] for r in rs], "-o", color=color, lw=2, ms=5,
                markeredgecolor=SURFACE, label=name)
        a2.plot(xs, [r["p99_ms"] for r in rs], "-o", color=color, lw=2, ms=5,
                markeredgecolor=SURFACE, label=name)
    for ax in (a1, a2):
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 4, 16, 32, 64, 128], ["1", "4", "16", "32", "64", "128"])
    _style(a1, "Throughput vs. concurrent clients", "concurrent clients", "transfers / second")
    a1.set_ylim(0, None)
    _style(a2, "p99 latency vs. concurrent clients",
           "concurrent clients", "latency (ms, log scale)")
    a2.set_yscale("log")
    a2.legend(frameon=False, fontsize=8, labelcolor=INK_2)
    a1.legend(frameon=False, fontsize=8, labelcolor=INK_2, loc="lower left")
    fig.tight_layout()
    fig.savefig(DOCS / "load-sweep.png", dpi=150, facecolor=SURFACE)


def diagnosis_chart():
    hold = json.loads((RESULTS / "lock_hold.json").read_text())
    hist = json.loads((RESULTS / "history.json").read_text())
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2), facecolor=SURFACE)

    added = [0, 2, 5, 10]
    measured = [r["throughput_rps"] for r in hold]
    h0 = 1000 / measured[0]  # implied lock hold time (ms) at +0
    predicted = [1000 / (h0 + s) for s in added]
    xs = range(len(added))
    a1.bar([x - 0.18 for x in xs], measured, width=0.34, color=ORANGE, label="measured",
           edgecolor=SURFACE, linewidth=2)
    a1.bar([x + 0.18 for x in xs], predicted, width=0.34, color="#86b6ef",
           label=f"predicted 1000 / ({h0:.1f} ms + added)", edgecolor=SURFACE, linewidth=2)
    for x, m in zip(xs, measured):
        a1.annotate(f"{m:.0f}", (x - 0.18, m), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontsize=8, color=INK_2)
    a1.set_xticks(list(xs), [f"+{s} ms" for s in added])
    _style(a1, "Hot account: throughput tracks 1 / lock-hold time",
           "extra time spent while holding the account lock", "transfers / second")
    a1.legend(frameon=False, fontsize=8, labelcolor=INK_2)

    before = json.loads((RESULTS / "v1_sum_balance" / "history.json").read_text())
    ns = [r["history"] for r in hist]
    top = 0
    for runs, label, color, lw in (
        (before, "before: balance = SUM(history)", REFERENCE, 1.5),
        (hist, "after: running balance on each entry", BLUE, 2.5),
    ):
        tp = [r["throughput_rps"] for r in runs]
        top = max(top, *tp)
        a2.plot(range(len(tp)), tp, "-o", color=color, lw=lw, ms=6,
                markeredgecolor=SURFACE, label=label)
        for i, v in enumerate(tp):
            a2.annotate(f"{v:.0f}/s", (i, v), xytext=(0, 8), textcoords="offset points",
                        ha="center", fontsize=8, color=INK_2)
    a2.set_xticks(range(len(ns)), [f"{n:,}" for n in ns])
    a2.set_ylim(0, top * 1.25)
    a2.legend(frameon=False, fontsize=8, labelcolor=INK_2, loc="lower left")
    _style(a2, "Hot account vs. length of its history",
           "prior transfers on the hot account", "transfers / second")
    fig.tight_layout()
    fig.savefig(DOCS / "load-diagnosis.png", dpi=150, facecolor=SURFACE)


if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    sweep_chart()
    diagnosis_chart()
    print("wrote docs/load-sweep.png, docs/load-diagnosis.png")
