"""Cost model and break-even analysis (Phase 8, RQ3). No Spark needed.

Converts every measured execution into an *estimated* cost from published cloud
pricing, then computes for each workload and action

* ``Crun_static`` -- cost of one run in the static configuration,
* ``Crun(a)``     -- cost of one run after action ``a``,
* ``Copt(a)``     -- one-time cost of applying ``a``,
* ``TC(a, N) = Copt(a) + N * Crun(a)`` and the break-even point
  ``N* = ceil(Copt / (Crun_static - Crun(a)))`` (infinite when there is no saving).

Cost proxies (see ``Cost_Model_Assumptions.md``):

* **P1, time-based**: ``wall_clock_seconds * vCPUs * price_per_vCPU_hour / 3600``
* **P2, resource-based**: ``executor_run_time_seconds * price_per_vCPU_hour / 3600``

Actions and their one-time cost:

* **A1_search** -- shuffle-partition tuning found by running every candidate setting
  once; ``Copt`` is the sum of one run at each setting, counted as pure overhead.
* **A1_rule** -- the same setting chosen without a search (``Copt = 0``); a lower bound.
* **A2** -- file compaction; ``Copt`` is the measured compaction job.

Every per-run quantity is the median over the measured repetitions of its cell.
Because ``Copt`` and ``Crun`` use the same proxy, ``N*`` does not depend on the
price; the sensitivity table demonstrates this explicitly.

Outputs (``results/processed/``): ``cost_per_cell.csv``, ``breakeven_table.csv``,
``cost_sensitivity.csv``; figures ``total_cost_curves_{p1,p2}.{pdf,png}`` in
``results/figures/`` (the P1 PDF is also copied to ``paper/figures/``).

Usage::

    python src/cost_model.py
    python src/cost_model.py --price 0.0505 --vcpus 12
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from src.paths import PROJECT_ROOT, RESULTS_CSV_PATH, RESULTS_FIGURES_DIR, RESULTS_PROCESSED_DIR  # noqa: E402

# AWS EC2 on-demand, m6i (general purpose, x86), Linux, Asia Pacific (Mumbai, ap-south-1).
# Every m6i size is priced at USD 0.0505 per vCPU-hour (e.g. m6i.xlarge: 4 vCPU, USD 0.202/h).
# Source and access date: Cost_Model_Assumptions.md.
PRICE_PER_VCPU_HOUR_USD = 0.0505
# Spark local[*] on the study machine runs 12 task slots (12 hardware threads); one AWS
# vCPU is one hardware thread, so the machine is priced as 12 vCPUs under P1.
STUDY_MACHINE_VCPUS = 12

STATIC_PARTITIONS = 200
N_GRID = (1, 5, 10, 50)
PRICE_FACTORS = (0.5, 1.0, 1.5)
PROXIES = ("P1", "P2")
PROXY_LABELS = {"P1": "P1 (wall-clock x vCPUs)", "P2": "P2 (executor run time)"}

CELL_OUTPUT = RESULTS_PROCESSED_DIR / "cost_per_cell.csv"
BREAKEVEN_OUTPUT = RESULTS_PROCESSED_DIR / "breakeven_table.csv"
SENSITIVITY_OUTPUT = RESULTS_PROCESSED_DIR / "cost_sensitivity.csv"
PAPER_FIGURES_DIR = PROJECT_ROOT / "paper" / "figures"


# --- Core formulas ----------------------------------------------------------------

def cost_p1(wall_clock_s: float, price_per_vcpu_hour: float, vcpus: int) -> float:
    """Time-based cost: the whole machine is paid for while the job runs."""
    return wall_clock_s * vcpus * price_per_vcpu_hour / 3600.0


def cost_p2(executor_run_s: float, price_per_vcpu_hour: float) -> float:
    """Resource-based cost: only the core time used by tasks is paid for."""
    return executor_run_s * price_per_vcpu_hour / 3600.0


def total_cost(copt: float, crun: float, n_runs: int) -> float:
    """``TC(a, N) = Copt(a) + N * Crun(a)``."""
    return copt + n_runs * crun


def break_even_runs(copt: float, crun_static: float, crun_action: float) -> float:
    """Smallest ``N >= 1`` with ``N * (Crun_static - Crun(a)) >= Copt(a)``; ``inf`` if no saving.

    The ratio is rounded before ``ceil`` so that floating-point error cannot push an
    exact integer ratio up by one.
    """
    saving = crun_static - crun_action
    if saving <= 0:
        return math.inf
    return float(max(1, math.ceil(round(copt / saving, 9))))


# --- Data preparation -------------------------------------------------------------

def load_measured_runs(path: Path = RESULTS_CSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path)
    measured = ~df["is_warmup"].astype(str).str.lower().isin(["true", "1"])
    df = df[measured & df["experiment_action"].isin(["A1", "A2"])].copy()
    df["executor_run_s"] = df["executor_run_time_ms"] / 1000.0
    return df


def add_costs(runs: pd.DataFrame, price: float, vcpus: int) -> pd.DataFrame:
    runs = runs.copy()
    runs["cost_P1"] = cost_p1(runs["wall_clock_seconds"], price, vcpus)
    runs["cost_P2"] = cost_p2(runs["executor_run_s"], price)
    return runs


def summarize_cells(runs: pd.DataFrame) -> pd.DataFrame:
    """Median and IQR of time and cost for every (action, workload, configuration) cell."""

    def iqr(s: pd.Series) -> float:
        return s.quantile(0.75) - s.quantile(0.25)

    grouped = runs.groupby(["experiment_action", "workload_id", "config_label", "shuffle_partitions"])
    cells = grouped.agg(
        n=("wall_clock_seconds", "size"),
        median_wall_s=("wall_clock_seconds", "median"),
        median_executor_run_s=("executor_run_s", "median"),
        median_cost_P1=("cost_P1", "median"),
        iqr_cost_P1=("cost_P1", iqr),
        median_cost_P2=("cost_P2", "median"),
        iqr_cost_P2=("cost_P2", iqr),
    ).reset_index()
    return cells


# --- Action options ---------------------------------------------------------------

def _option(workload, action, proxy, setting, static, crun, copt, k_search=None) -> dict:
    crun_static = static[f"median_cost_{proxy}"]
    saving = crun_static - crun
    return {
        "workload_id": workload,
        "action": action,
        "proxy": proxy,
        "chosen_setting": setting,
        "search_settings": k_search,
        "crun_static_usd": crun_static,
        "crun_action_usd": crun,
        "copt_usd": copt,
        "saving_per_run_usd": saving,
        "saving_pct": saving / crun_static,
        "static_iqr_usd": static[f"iqr_cost_{proxy}"],
        "saving_exceeds_static_iqr": saving > static[f"iqr_cost_{proxy}"],
        "n_star": break_even_runs(copt, crun_static, crun),
    }


def a1_options(cells: pd.DataFrame) -> list[dict]:
    rows = []
    a1 = cells[cells["experiment_action"] == "A1"]
    for workload, grid in a1.groupby("workload_id"):
        static = grid[grid["shuffle_partitions"] == STATIC_PARTITIONS].iloc[0]
        for proxy in PROXIES:
            col = f"median_cost_{proxy}"
            best = grid.loc[grid[col].idxmin()]
            partitions = int(best["shuffle_partitions"])
            setting = f"{partitions} partition" + ("" if partitions == 1 else "s")
            search_cost = grid[col].sum()
            rows.append(_option(workload, "A1_search", proxy, setting, static, best[col], search_cost, len(grid)))
            rows.append(_option(workload, "A1_rule", proxy, setting, static, best[col], 0.0))
    return rows


def a2_options(cells: pd.DataFrame) -> list[dict]:
    rows = []
    a2 = cells[cells["experiment_action"] == "A2"]
    for workload, stages in a2.groupby("workload_id"):
        stage = stages.set_index("config_label")
        for proxy in PROXIES:
            col = f"median_cost_{proxy}"
            rows.append(_option(workload, "A2", proxy, "compact to 1 file", stage.loc["fragmented"],
                                stage.loc["compacted", col], stage.loc["compaction", col]))
    return rows


def breakeven_table(cells: pd.DataFrame) -> pd.DataFrame:
    table = pd.DataFrame(a1_options(cells) + a2_options(cells))
    for n in N_GRID:
        table[f"tc_static_N{n}"] = n * table["crun_static_usd"]
        table[f"tc_action_N{n}"] = total_cost(table["copt_usd"], table["crun_action_usd"], n)
        table[f"worthwhile_N{n}"] = table[f"tc_action_N{n}"] < table[f"tc_static_N{n}"]
    return table


def sensitivity_table(runs: pd.DataFrame, price: float, vcpus: int) -> pd.DataFrame:
    """Recompute N* and the N=10 net saving for each price factor (and both proxies)."""
    frames = []
    for factor in PRICE_FACTORS:
        table = breakeven_table(summarize_cells(add_costs(runs, price * factor, vcpus)))
        table.insert(0, "price_factor", factor)
        table.insert(1, "price_per_vcpu_hour_usd", price * factor)
        table["net_saving_N10_usd"] = table["tc_static_N10"] - table["tc_action_N10"]
        frames.append(table[["price_factor", "price_per_vcpu_hour_usd", "workload_id", "action", "proxy",
                             "chosen_setting", "n_star", "net_saving_N10_usd", "worthwhile_N10"]])
    return pd.concat(frames, ignore_index=True)


# --- Figures ----------------------------------------------------------------------

def plot_total_cost_curves(table: pd.DataFrame, proxy: str, out_base: Path, n_max: int = 50) -> None:
    """Total cost vs number of future runs, one panel per workload."""
    data = table[table["proxy"] == proxy]
    workloads = sorted(data["workload_id"].unique(), key=lambda w: (not w.startswith("A1"), w))
    ncols = 5
    nrows = math.ceil(len(workloads) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 3.0 * nrows), squeeze=False)
    n = list(range(0, n_max + 1))
    styles = {"A1_search": ("tab:blue", "-"), "A1_rule": ("tab:blue", ":"), "A2": ("tab:green", "-")}
    for ax, workload in zip(axes.flat, workloads, strict=False):
        rows = data[data["workload_id"] == workload]
        scale = 1e3  # milli-USD keeps the axis readable
        static = rows.iloc[0]["crun_static_usd"]
        ax.plot(n, [k * static * scale for k in n], color="tab:red", label="static (none)")
        for _, row in rows.iterrows():
            color, line = styles[row["action"]]
            ax.plot(n, [total_cost(row["copt_usd"], row["crun_action_usd"], k) * scale for k in n],
                    color=color, linestyle=line, label=f"{row['action']} ({row['chosen_setting']})")
            if math.isfinite(row["n_star"]) and row["n_star"] <= n_max:
                ns = row["n_star"]
                ax.plot([ns], [total_cost(row["copt_usd"], row["crun_action_usd"], ns) * scale],
                        "o", color=color, markersize=4)
        ax.set_title(workload.replace("_", " "), fontsize=9)
        ax.set_xlabel("future runs N", fontsize=8)
        ax.set_ylabel("total cost (10$^{-3}$ USD)", fontsize=8)
        ax.tick_params(labelsize=7)
        ax.legend(fontsize=6, loc="upper left")
    for ax in list(axes.flat)[len(workloads):]:
        ax.axis("off")
    fig.suptitle(f"Estimated total cost TC(a, N), {PROXY_LABELS[proxy]}; dots mark N*", fontsize=10)
    fig.tight_layout()
    out_base.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_base.with_suffix(".pdf"))
    fig.savefig(out_base.with_suffix(".png"), dpi=150)
    plt.close(fig)


# --- Entry point ------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Estimate costs and break-even points from measured runs.")
    parser.add_argument("--price", type=float, default=PRICE_PER_VCPU_HOUR_USD, help="USD per vCPU-hour")
    parser.add_argument("--vcpus", type=int, default=STUDY_MACHINE_VCPUS, help="vCPUs charged under P1")
    args = parser.parse_args()

    runs = add_costs(load_measured_runs(), args.price, args.vcpus)
    cells = summarize_cells(runs)
    table = breakeven_table(cells)
    sensitivity = sensitivity_table(load_measured_runs(), args.price, args.vcpus)

    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cells.to_csv(CELL_OUTPUT, index=False, float_format="%.10g")
    table.to_csv(BREAKEVEN_OUTPUT, index=False, float_format="%.10g")
    sensitivity.to_csv(SENSITIVITY_OUTPUT, index=False, float_format="%.10g")

    for proxy in PROXIES:
        plot_total_cost_curves(table, proxy, RESULTS_FIGURES_DIR / f"total_cost_curves_{proxy.lower()}")
    PAPER_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(RESULTS_FIGURES_DIR / "total_cost_curves_p1.pdf", PAPER_FIGURES_DIR / "total_cost_curves_p1.pdf")

    pd.set_option("display.width", 220)
    view = table[["workload_id", "action", "proxy", "chosen_setting", "saving_pct", "n_star",
                  "saving_exceeds_static_iqr"] + [f"worthwhile_N{n}" for n in N_GRID]].copy()
    view["saving_pct"] = (100 * view["saving_pct"]).round(1)
    print(f"Price: USD {args.price}/vCPU-hour; P1 vCPUs: {args.vcpus}\n")
    print(view.to_string(index=False))

    invariant = sensitivity.groupby(["workload_id", "action", "proxy"])["n_star"].nunique().eq(1).all()
    proxy_agree = (table.pivot_table(index=["workload_id", "action"], columns="proxy", values="n_star")
                   .pipe(lambda p: (p["P1"] == p["P2"]).mean()))
    print(f"\nN* unchanged across price x{PRICE_FACTORS}: {invariant}")
    print(f"Share of (workload, action) pairs with the same N* under P1 and P2: {proxy_agree:.0%}")
    print(f"Wrote {CELL_OUTPUT.name}, {BREAKEVEN_OUTPUT.name}, {SENSITIVITY_OUTPUT.name} and figures")


if __name__ == "__main__":
    main()
