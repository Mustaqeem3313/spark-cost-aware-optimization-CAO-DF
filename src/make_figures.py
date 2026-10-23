"""Paper figures (Phase 10). No Spark needed.

Builds every data figure of the paper from the committed result files, in IEEE
single-column size (3.5 in wide), and writes each as PDF to ``paper/figures/``
and as PNG to ``results/figures/``:

* ``fig_a1_boxplots``    -- run-time spread of every A1 setting, per workload (box plots);
* ``fig_a2_bars``        -- A2 before / after / compaction times, per workload (bar chart);
* ``fig_breakeven_map``  -- saving per run vs one-time cost, with break-even lines (scatter);
* ``fig_tc_examples``    -- total cost over future runs for two example workloads (line chart);
* ``fig_proxy_scatter``  -- saving under P1 vs P2 for every workload and action (scatter);
* ``fig_validation``     -- measured total cost of each strategy on the unseen workloads (bars).

Usage::

    python src/make_figures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.cost_model import N_GRID, PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS, total_cost  # noqa: E402
from src.decision_framework import load_validation_cells, measured_options  # noqa: E402
from src.paths import PROJECT_ROOT, RESULTS_CSV_PATH, RESULTS_FIGURES_DIR, RESULTS_PROCESSED_DIR  # noqa: E402

PAPER_FIGURES_DIR = PROJECT_ROOT / "paper" / "figures"
VALIDATION_TITLES = {
    "V_medium_1f_s85": "500K orders, 1 file, 85% shuffle",
    "V_medium_25f_s50": "500K orders, 25 files, 50% shuffle",
    "V_small_25f_s15": "50K orders, 25 files, 15% shuffle",
    "V_large_5f_s50": "2M orders, 5 files, 50% shuffle",
}
COLUMN_WIDTH_IN = 3.5
COLORS = {"static": "tab:red", "A1": "tab:blue", "A2": "tab:green", "oracle": "tab:gray",
          "perf": "tab:orange", "val": "black"}

plt.rcParams.update({
    "font.size": 7, "axes.titlesize": 7.5, "axes.labelsize": 7, "legend.fontsize": 6.5,
    "xtick.labelsize": 6.5, "ytick.labelsize": 6.5, "axes.linewidth": 0.6,
    "pdf.fonttype": 42, "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
})


def _save(fig: plt.Figure, name: str) -> None:
    PAPER_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(PAPER_FIGURES_DIR / f"{name}.pdf")
    fig.savefig(RESULTS_FIGURES_DIR / f"{name}.png", dpi=200)
    plt.close(fig)
    print(f"[make_figures] {name}")


def _measured(action: str) -> pd.DataFrame:
    df = pd.read_csv(RESULTS_CSV_PATH)
    keep = ~df["is_warmup"].astype(str).str.lower().isin(["true", "1"]) & (df["experiment_action"] == action)
    return df[keep]


def _label(workload: str) -> str:
    return workload.split("_", 1)[1].replace("_", " ")


def a1_boxplots() -> None:
    runs = _measured("A1")
    order = ["A1_small_low", "A1_small_high", "A1_large_low", "A1_large_high"]
    settings = sorted(runs["shuffle_partitions"].unique())
    fig, axes = plt.subplots(2, 2, figsize=(COLUMN_WIDTH_IN, 3.0), sharex=True, sharey=True)
    for ax, workload in zip(axes.flat, order, strict=True):
        data = [runs[(runs["workload_id"] == workload) & (runs["shuffle_partitions"] == s)]["wall_clock_seconds"]
                for s in settings]
        box = ax.boxplot(data, widths=0.6, patch_artist=True, showfliers=True,
                         flierprops={"markersize": 2.5, "markeredgewidth": 0.5},
                         medianprops={"color": "black", "linewidth": 0.8},
                         boxprops={"linewidth": 0.5}, whiskerprops={"linewidth": 0.5},
                         capprops={"linewidth": 0.5})
        best = int(np.argmin([d.median() for d in data]))
        for i, patch in enumerate(box["boxes"]):
            patch.set_facecolor(COLORS["static"] if settings[i] == 200 else
                                COLORS["A1"] if i == best else "lightsteelblue")
        ax.set_yscale("log")
        ax.set_yticks([0.3, 0.5, 1, 2, 4], ["0.3", "0.5", "1", "2", "4"])
        ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        ax.set_title(_label(workload).replace("low", "low shuffle").replace("high", "high shuffle"))
        ax.set_xticks(range(1, len(settings) + 1), [str(s) for s in settings], rotation=0)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    for ax in axes[1]:
        ax.set_xlabel("shuffle partitions")
    for ax in axes[:, 0]:
        ax.set_ylabel("run time (s, log)")
    fig.tight_layout(h_pad=0.6, w_pad=0.4)
    _save(fig, "fig_a1_boxplots")


def a2_bars() -> None:
    cells = pd.read_csv(RESULTS_PROCESSED_DIR / "a2_cell_summary.csv")
    order = ["A2_small_low", "A2_small_medium", "A2_small_high",
             "A2_large_low", "A2_large_medium", "A2_large_high"]
    files = {"low": "1", "medium": "10", "high": "50"}
    runs = _measured("A2")
    stages = [("fragmented", "before (fragmented)", "lightcoral"),
              ("compacted", "after (compacted)", COLORS["A2"]),
              ("compaction", "compaction job", "khaki")]
    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 1.9))
    x = np.arange(len(order))
    width = 0.27
    for i, (stage, label, color) in enumerate(stages):
        med, low, high = [], [], []
        for workload in order:
            values = runs[(runs["workload_id"] == workload) & (runs["config_label"] == stage)]["wall_clock_seconds"]
            cell = cells[(cells["workload_id"] == workload) & (cells["config_label"] == stage)]
            m = float(cell["median_wall_s"].iloc[0])
            med.append(m)
            low.append(m - values.quantile(0.25))
            high.append(values.quantile(0.75) - m)
        ax.bar(x + (i - 1) * width, med, width, yerr=[low, high], label=label, color=color,
               edgecolor="black", linewidth=0.3, error_kw={"elinewidth": 0.5, "capsize": 1.2})
    ax.set_xticks(x, [files[w.split("_")[2]] for w in order])
    ax.axvline(2.5, color="dimgray", linewidth=0.5, linestyle="--")
    for xc, text in ((1, "small data (50K orders)"), (4, "large data (2M orders)")):
        ax.annotate(text, (xc, 7.6), ha="center", fontsize=6.5)
    ax.set_xlabel("number of input files")
    ax.set_ylabel("median run time (s)")
    ax.set_ylim(0, 8.2)
    ax.legend(ncol=3, frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.0), columnspacing=1.0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    _save(fig, "fig_a2_bars")


def _validation_points(proxy: str) -> pd.DataFrame:
    cells = load_validation_cells(PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS)
    rows = []
    for workload in cells["workload_id"].unique():
        options = measured_options(cells, workload, proxy)
        static = options["Nothing"][1]
        for option, action in (("Tune", "A1"), ("Compact", "A2")):
            copt, crun = options[option]
            rows.append({"workload_id": workload, "action": action,
                         "saving_pct": 1 - crun / static, "copt_ratio": copt / static})
    return pd.DataFrame(rows)


def breakeven_map(proxy: str = "P1") -> None:
    table = pd.read_csv(RESULTS_PROCESSED_DIR / "breakeven_table.csv")
    table = table[(table["proxy"] == proxy) & table["action"].isin(["A1_search", "A2"])].copy()
    table["copt_ratio"] = table["copt_usd"] / table["crun_static_usd"]
    val = _validation_points(proxy)
    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.3))
    s = np.linspace(0.005, 1.0, 400)
    for n_star, style, at in ((1, "-", (99, 0.86)), (3, "--", (99, 2.62)), (10, ":", (33, 3.0))):
        ax.plot(100 * s, n_star * s, color="dimgray", linestyle=style, linewidth=0.7)
        ax.annotate(f"$N^*\\leq{n_star}$", at, fontsize=6, color="dimgray", ha="right", va="bottom")
    ax.axvspan(-15, 10, color="lightgray", alpha=0.5, linewidth=0)
    ax.annotate("below\nnoise\nfloor", (-2.5, 3.3), fontsize=6, ha="center", va="top", color="dimgray")
    for action, marker in (("A1", "o"), ("A2", "s")):
        study = table[table["action"].str.startswith(action)]
        ax.scatter(100 * study["saving_pct"], study["copt_ratio"], marker=marker, s=16,
                   color=COLORS[action], label=f"{'A1 tune' if action == 'A1' else 'A2 compact'} (study)",
                   zorder=3, edgecolor="black", linewidth=0.3)
        unseen = val[val["action"] == action]
        ax.scatter(100 * unseen["saving_pct"], unseen["copt_ratio"], marker=marker, s=16,
                   facecolor="white", edgecolor=COLORS[action], linewidth=0.9,
                   label=f"{'A1' if action == 'A1' else 'A2'} (unseen)", zorder=3)
    ax.set_xlim(-15, 100)
    ax.set_ylim(0, 3.4)
    ax.set_xlabel("saving per run (% of a static run)")
    ax.set_ylabel("one-time cost (static runs)")
    ax.legend(frameon=True, framealpha=0.9, edgecolor="none", loc="center", ncol=2, bbox_to_anchor=(0.52, 0.5))
    ax.grid(linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    _save(fig, "fig_breakeven_map")


def tc_examples(proxy: str = "P1") -> None:
    table = pd.read_csv(RESULTS_PROCESSED_DIR / "breakeven_table.csv")
    table = table[table["proxy"] == proxy]
    examples = [("A1_large_high", ["A1_search", "A1_rule"]), ("A2_large_high", ["A2"]), ("A2_large_low", ["A2"])]
    styles = {"A1_search": (COLORS["A1"], "-", "tune (search)"), "A1_rule": (COLORS["A1"], ":", "tune (known setting)"),
              "A2": (COLORS["A2"], "-", "compact")}
    titles = {"A1_large_high": "A1 large, high shuffle", "A2_large_high": "A2 large, 50 files",
              "A2_large_low": "A2 large, 1 file"}
    n = np.arange(0, 11)
    fig, axes = plt.subplots(1, 3, figsize=(COLUMN_WIDTH_IN, 1.55), sharey=True)
    for ax, (workload, actions) in zip(axes, examples, strict=True):
        rows = table[(table["workload_id"] == workload) & table["action"].isin(actions)]
        static = rows["crun_static_usd"].iloc[0]
        ax.plot(n, n, color=COLORS["static"], linewidth=0.9, label="static")
        for _, row in rows.iterrows():
            color, line, label = styles[row["action"]]
            ax.plot(n, total_cost(row["copt_usd"], row["crun_action_usd"], n) / static, color=color,
                    linestyle=line, linewidth=0.9, label=label)
        ax.set_title(titles[workload], fontsize=6.5)
        ax.set_xlabel("future runs N")
        ax.grid(linewidth=0.3, alpha=0.5)
        ax.tick_params(labelsize=6)
        ax.set_xticks([0, 5, 10])
    axes[0].set_ylabel("total cost\n(static runs)")
    handles, labels = [], []
    for ax in axes:
        for h, label in zip(*ax.get_legend_handles_labels(), strict=True):
            if label not in labels:
                handles.append(h)
                labels.append(label)
    fig.legend(handles, labels, ncol=4, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.1))
    fig.tight_layout(w_pad=0.3)
    _save(fig, "fig_tc_examples")


def proxy_scatter() -> None:
    table = pd.read_csv(RESULTS_PROCESSED_DIR / "breakeven_table.csv")
    table = table[table["action"].isin(["A1_search", "A2"])]
    wide = table.pivot_table(index=["workload_id", "action"], columns="proxy", values="saving_pct").reset_index()
    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN * 0.62, 1.9))
    ax.plot([-10, 100], [-10, 100], color="dimgray", linewidth=0.6, linestyle="--")
    for action, marker, color, label in (("A1_search", "o", COLORS["A1"], "A1 tune"),
                                         ("A2", "s", COLORS["A2"], "A2 compact")):
        part = wide[wide["action"] == action]
        ax.scatter(100 * part["P1"], 100 * part["P2"], marker=marker, s=14, color=color, label=label,
                   edgecolor="black", linewidth=0.3, zorder=3)
    ax.set_xlabel("saving under P1 (%)")
    ax.set_ylabel("saving under P2 (%)")
    ax.set_xlim(-10, 100)
    ax.set_ylim(-10, 100)
    ax.legend(frameon=False, loc="lower right")
    ax.grid(linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    _save(fig, "fig_proxy_scatter")


def validation_bars(proxy: str = "P1") -> None:
    table = pd.read_csv(RESULTS_PROCESSED_DIR / "validation_decisions.csv")
    table = table[table["proxy"] == proxy]
    workloads = list(dict.fromkeys(table["workload_id"]))
    strategies = [("tc_performance_only_usd", "performance-only", COLORS["perf"]),
                  ("tc_framework_usd", "CAO-DF", COLORS["A1"]),
                  ("tc_oracle_usd", "oracle (hindsight)", COLORS["oracle"])]
    fig, axes = plt.subplots(2, 2, figsize=(COLUMN_WIDTH_IN, 2.9), sharey=True)
    x = np.arange(len(N_GRID))
    width = 0.26
    for ax, workload in zip(axes.flat, workloads, strict=True):
        rows = table[table["workload_id"] == workload].set_index("n_runs").loc[list(N_GRID)]
        for i, (column, label, color) in enumerate(strategies):
            ax.bar(x + (i - 1) * width, rows[column] / rows["tc_static_usd"], width, label=label, color=color,
                   edgecolor="black", linewidth=0.3)
        ratios = rows["tc_framework_usd"] / rows["tc_static_usd"]
        for xi, choice, ratio in zip(x, rows["framework_choice"], ratios, strict=True):
            ax.annotate(choice[0], (xi, min(ratio, 1.6) + 0.04), ha="center", fontsize=6, color=COLORS["A1"],
                        weight="bold")
        perf = rows["tc_performance_only_usd"] / rows["tc_static_usd"]
        for xi, ratio in zip(x, perf, strict=True):
            if ratio > 1.75:
                ax.annotate(f"{ratio:.1f}\u00d7", (xi - width, 1.25), ha="center", fontsize=5.5, color="white",
                            weight="bold", rotation=90)
        ax.axhline(1.0, color=COLORS["static"], linewidth=0.8, label="static")
        ax.set_xticks(x, [str(n) for n in N_GRID])
        ax.set_title(VALIDATION_TITLES.get(workload, workload), fontsize=6.5)
        ax.set_ylim(0, 1.8)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    for ax in axes[1]:
        ax.set_xlabel("future runs N")
    for ax in axes[:, 0]:
        ax.set_ylabel("cost / static cost")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=4, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.04))
    fig.tight_layout(rect=(0, 0, 1, 0.95), h_pad=0.5)
    _save(fig, "fig_validation")


def main() -> None:
    a1_boxplots()
    a2_bars()
    tc_examples()
    proxy_scatter()
    if (RESULTS_PROCESSED_DIR / "validation_decisions.csv").exists():
        breakeven_map()
        validation_bars()
    else:
        print("[make_figures] validation results missing -- run src/decision_framework.py first")


if __name__ == "__main__":
    main()
