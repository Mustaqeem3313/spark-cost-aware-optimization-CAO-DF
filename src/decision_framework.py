"""Phase 9 -- the CAO-DF decision rule and its validation (RQ2, H2). No Spark needed.

Decision rule
-------------
For a workload ``w`` that will run ``N`` more times, the framework compares three
options and returns the cheapest: **Nothing** (static configuration), **Tune**
(A1, shuffle-partition tuning) or **Compact** (A2, file compaction).

Costs are expressed in units of one static run, so no absolute price is needed:

* ``TC(Nothing, N) = N``
* ``TC(a, N) = r_a + N * (1 - s_a)``, where ``s_a`` is the fraction of a static run
  that action ``a`` saves per run and ``r_a = Copt(a) / Crun_static``.

``s_a`` and ``r_a`` for an unseen workload are copied from the **nearest measured
workload** of the same action (distance over log10 order count, log10 file count
and shuffle fraction, each scaled by its range in the study workloads; ties are
averaged). An action whose estimated saving is below the noise floor ``delta``
(default 10 % of a static run) is treated as having no benefit. Ties go to
Nothing. The rule has no trained model: every input is a measured cell median.

Validation
----------
The rule only sees the study workloads (experiments E3 and E5). It is applied to
the unseen validation workloads (E6, ``src/run_validation.py``) for
``N`` in {1, 5, 10, 50}, and the *measured* total cost of its choice is compared with:

* **Static** -- never optimise;
* **Performance-only** -- always apply the action with the largest per-run saving,
  ignoring its one-time cost;
* **Oracle** -- the cheapest option in hindsight, from the measured costs.

A result within ``delta`` of the static total cost counts as a tie. A leave-one-out
check on the study workloads complements the four unseen workloads.

Outputs (``results/processed/``): ``validation_decisions.csv``,
``validation_summary.csv``, ``framework_loo.csv``; figure
``results/figures/framework_vs_static.{pdf,png}`` (PDF copied to ``paper/figures/``).

Usage::

    python src/decision_framework.py
    python src/decision_framework.py --delta 0.05
    python src/decision_framework.py --decide --orders 800000 --files 30 --shuffle 0.6 --runs 10
    python src/decision_framework.py --interactive
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from src.cost_model import (  # noqa: E402
    N_GRID,
    PRICE_PER_VCPU_HOUR_USD,
    PROXIES,
    STATIC_PARTITIONS,
    STUDY_MACHINE_VCPUS,
    add_costs,
    breakeven_table,
    load_measured_runs,
    summarize_cells,
)
from src.paths import (  # noqa: E402
    PROJECT_ROOT,
    RESULTS_CSV_PATH,
    RESULTS_FIGURES_DIR,
    RESULTS_PROCESSED_DIR,
    VALIDATION_INDEX_PATH,
    WORKLOAD_INDEX_PATH,
)
from src.workload_generator import SHUFFLE_VOLUME_LEVELS  # noqa: E402

DEFAULT_DELTA = 0.10
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 2026
ACTIONS = {"A1": "Tune", "A2": "Compact"}
OPTIONS = ("Nothing", "Tune", "Compact")
FEATURES = ("log_orders", "log_files", "shuffle_fraction")

DECISIONS_OUTPUT = RESULTS_PROCESSED_DIR / "validation_decisions.csv"
SUMMARY_OUTPUT = RESULTS_PROCESSED_DIR / "validation_summary.csv"
LOO_OUTPUT = RESULTS_PROCESSED_DIR / "framework_loo.csv"
PAPER_FIGURES_DIR = PROJECT_ROOT / "paper" / "figures"


# --- Workload features ------------------------------------------------------------

def workload_features(index_path: Path) -> pd.DataFrame:
    """``log10`` order count, ``log10`` file count and shuffle fraction of each workload."""
    index = pd.read_csv(index_path, dtype={"shuffle_volume_level": str})

    def fraction(level: str) -> float:
        return SHUFFLE_VOLUME_LEVELS[level] if level in SHUFFLE_VOLUME_LEVELS else float(level)

    return pd.DataFrame({
        "workload_id": index["workload_id"],
        "orders": index["num_orders"],
        "files": index["actual_file_count"],
        "log_orders": np.log10(index["num_orders"]),
        "log_files": np.log10(index["actual_file_count"]),
        "shuffle_fraction": index["shuffle_volume_level"].map(fraction),
    }).set_index("workload_id")


# --- Training profiles (study workloads) ------------------------------------------

def training_profiles(price: float, vcpus: int) -> pd.DataFrame:
    """Measured saving ``s`` and one-time-cost ratio ``r`` per study workload, action and proxy."""
    table = breakeven_table(summarize_cells(add_costs(load_measured_runs(), price, vcpus)))
    table = table[table["action"].isin(["A1_search", "A2"])].copy()
    table["action"] = table["action"].str.slice(0, 2)
    table["copt_ratio"] = table["copt_usd"] / table["crun_static_usd"]
    table["partitions"] = table["chosen_setting"].str.extract(r"^(\d+) partition")[0].astype(float)
    features = workload_features(WORKLOAD_INDEX_PATH)
    profiles = table[["workload_id", "action", "proxy", "saving_pct", "copt_ratio", "partitions"]]
    return profiles.join(features, on="workload_id")


@dataclass
class Estimate:
    saving_pct: float
    copt_ratio: float
    partitions: float | None
    neighbours: list[str] = field(default_factory=list)


def estimate(profiles: pd.DataFrame, action: str, proxy: str, features: pd.Series) -> Estimate:
    """Nearest-measured-workload estimate of ``s`` and ``r`` for ``action`` (ties averaged)."""
    candidates = profiles[(profiles["action"] == action) & (profiles["proxy"] == proxy)]
    scale = profiles[list(FEATURES)].max() - profiles[list(FEATURES)].min()
    diffs = (candidates[list(FEATURES)] - features[list(FEATURES)].astype(float)) / scale
    distance = np.sqrt((diffs ** 2).sum(axis=1))
    nearest = candidates[np.isclose(distance, distance.min(), rtol=0, atol=1e-9)]
    partitions = nearest["partitions"].dropna()
    return Estimate(
        saving_pct=float(nearest["saving_pct"].mean()),
        copt_ratio=float(nearest["copt_ratio"].mean()),
        partitions=float(partitions.min()) if not partitions.empty else None,
        neighbours=nearest["workload_id"].tolist(),
    )


@dataclass
class Decision:
    choice: str
    predicted_tc: dict[str, float]
    estimates: dict[str, Estimate]


def decide(profiles: pd.DataFrame, features: pd.Series, n_runs: int, proxy: str = "P1",
           delta: float = DEFAULT_DELTA) -> Decision:
    """Return the option with the lowest predicted total cost (in static-run units)."""
    estimates = {action: estimate(profiles, action, proxy, features) for action in ACTIONS}
    predicted = {"Nothing": float(n_runs)}
    for action, option in ACTIONS.items():
        est = estimates[action]
        if est.saving_pct >= delta:
            predicted[option] = est.copt_ratio + n_runs * (1 - est.saving_pct)
    choice = min(predicted, key=lambda o: (predicted[o], o != "Nothing"))
    return Decision(choice, predicted, estimates)


def performance_only(profiles: pd.DataFrame, features: pd.Series, proxy: str, delta: float) -> str:
    """The action with the largest estimated per-run saving, regardless of its one-time cost."""
    estimates = {ACTIONS[a]: estimate(profiles, a, proxy, features).saving_pct for a in ACTIONS}
    best = max(estimates, key=estimates.get)
    return best if estimates[best] >= delta else "Nothing"


# --- Measured costs on the validation workloads -----------------------------------

def load_validation_cells(price: float, vcpus: int) -> pd.DataFrame:
    df = pd.read_csv(RESULTS_CSV_PATH)
    measured = ~df["is_warmup"].astype(str).str.lower().isin(["true", "1"])
    df = df[measured & (df["experiment_action"] == "VAL")].copy()
    if df.empty:
        raise RuntimeError("No validation runs found. Run: python src/run_validation.py")
    df["executor_run_s"] = df["executor_run_time_ms"] / 1000.0
    df = add_costs(df, price, vcpus)
    return df.groupby(["workload_id", "config_label"])[["cost_P1", "cost_P2"]].median().reset_index()


def measured_options(cells: pd.DataFrame, workload: str, proxy: str) -> dict[str, tuple[float, float]]:
    """``(Copt, Crun)`` in USD of each option on a validation workload, from cell medians."""
    stage = cells[cells["workload_id"] == workload].set_index("config_label")[f"cost_{proxy}"]
    shuffle = stage[stage.index.str.startswith("shuffle")]
    return {
        "Nothing": (0.0, float(stage[f"shuffle{STATIC_PARTITIONS}"])),
        "Tune": (float(shuffle.sum()), float(shuffle.min())),
        "Compact": (float(stage["compaction"]), float(stage["compacted"])),
        **{f"shuffle{int(k.removeprefix('shuffle'))}": (0.0, float(v)) for k, v in shuffle.items()},
    }


def measured_tc(options: dict[str, tuple[float, float]], option: str, n_runs: int) -> float:
    copt, crun = options[option]
    return copt + n_runs * crun


def outcome(tc: float, tc_static: float, delta: float) -> str:
    if abs(tc - tc_static) <= delta * tc_static:
        return "tie"
    return "win" if tc < tc_static else "loss"


def validate(profiles: pd.DataFrame, cells: pd.DataFrame, delta: float) -> pd.DataFrame:
    features = workload_features(VALIDATION_INDEX_PATH)
    rows = []
    for workload in features.index:
        for proxy in PROXIES:
            options = measured_options(cells, workload, proxy)
            for n in N_GRID:
                decision = decide(profiles, features.loc[workload], n, proxy, delta)
                perf = performance_only(profiles, features.loc[workload], proxy, delta)
                tc = {o: measured_tc(options, o, n) for o in OPTIONS}
                oracle = min(tc, key=lambda o: (tc[o], o != "Nothing"))
                rule_setting = decision.estimates["A1"].partitions
                tc_rule = measured_tc(options, f"shuffle{int(rule_setting)}", n) if rule_setting else math.nan
                tc_framework = tc[decision.choice]
                rows.append({
                    "workload_id": workload,
                    "proxy": proxy,
                    "n_runs": n,
                    "framework_choice": decision.choice,
                    "performance_only_choice": perf,
                    "oracle_choice": oracle,
                    "a1_neighbours": "+".join(decision.estimates["A1"].neighbours),
                    "a2_neighbours": "+".join(decision.estimates["A2"].neighbours),
                    "est_saving_tune": decision.estimates["A1"].saving_pct,
                    "est_saving_compact": decision.estimates["A2"].saving_pct,
                    "measured_saving_tune": 1 - options["Tune"][1] / options["Nothing"][1],
                    "measured_saving_compact": 1 - options["Compact"][1] / options["Nothing"][1],
                    "tc_static_usd": tc["Nothing"],
                    "tc_framework_usd": tc_framework,
                    "tc_performance_only_usd": tc[perf],
                    "tc_oracle_usd": tc[oracle],
                    "tc_tune_rule_usd": tc_rule,
                    "framework_reduction_pct": 1 - tc_framework / tc["Nothing"],
                    "performance_only_reduction_pct": 1 - tc[perf] / tc["Nothing"],
                    "oracle_reduction_pct": 1 - tc[oracle] / tc["Nothing"],
                    "framework_regret_pct": tc_framework / tc[oracle] - 1,
                    "framework_vs_static": outcome(tc_framework, tc["Nothing"], delta),
                    "performance_only_vs_static": outcome(tc[perf], tc["Nothing"], delta),
                    "framework_matches_oracle": decision.choice == oracle,
                })
    return pd.DataFrame(rows)


def bootstrap_ci(values: np.ndarray, resamples: int = BOOTSTRAP_RESAMPLES,
                 seed: int = BOOTSTRAP_SEED) -> tuple[float, float]:
    """Percentile 95 % confidence interval of the mean."""
    rng = np.random.default_rng(seed)
    means = rng.choice(values, size=(resamples, len(values)), replace=True).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def summarize(table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for proxy, group in table.groupby("proxy"):
        for strategy in ("framework", "performance_only"):
            reductions = group[f"{strategy}_reduction_pct"].to_numpy()
            low, high = bootstrap_ci(reductions)
            counts = group[f"{strategy}_vs_static"].value_counts()
            rows.append({
                "proxy": proxy,
                "strategy": strategy,
                "cases": len(group),
                "wins": int(counts.get("win", 0)),
                "ties": int(counts.get("tie", 0)),
                "losses": int(counts.get("loss", 0)),
                "mean_reduction_pct": float(reductions.mean()),
                "ci95_low": low,
                "ci95_high": high,
                "total_cost_reduction_pct": 1 - group[f"tc_{strategy}_usd"].sum() / group["tc_static_usd"].sum(),
                "matches_oracle": int(group["framework_matches_oracle"].sum()) if strategy == "framework" else None,
                "max_regret_pct": float(group["framework_regret_pct"].max()) if strategy == "framework" else None,
            })
    return pd.DataFrame(rows)


# --- Leave-one-out check on the study workloads -----------------------------------

def leave_one_out(profiles: pd.DataFrame, delta: float) -> pd.DataFrame:
    """Predict each study workload's decision from the *other* workloads of the same action.

    Each study workload was measured with one action only, so the check compares
    "apply the action or not" for that action at every ``N``.
    """
    rows = []
    for (workload, action, proxy), measured in profiles.groupby(["workload_id", "action", "proxy"]):
        measured = measured.iloc[0]
        rest = profiles[profiles["workload_id"] != workload]
        est = estimate(rest, action, proxy, measured)
        for n in N_GRID:
            predicted = est.saving_pct >= delta and est.copt_ratio + n * (1 - est.saving_pct) < n
            actual = measured["saving_pct"] >= delta and measured["copt_ratio"] + n * (1 - measured["saving_pct"]) < n
            rows.append({"workload_id": workload, "action": action, "proxy": proxy, "n_runs": n,
                         "neighbours": "+".join(est.neighbours), "predicted_apply": bool(predicted),
                         "measured_apply": bool(actual), "agree": bool(predicted) == bool(actual)})
    return pd.DataFrame(rows)


# --- Figure -----------------------------------------------------------------------

def plot_validation(table: pd.DataFrame, proxy: str, out_base: Path) -> None:
    """Measured total cost of each strategy relative to static, per workload and N."""
    data = table[table["proxy"] == proxy]
    workloads = list(dict.fromkeys(data["workload_id"]))
    strategies = [("tc_performance_only_usd", "Performance-only", "tab:orange"),
                  ("tc_framework_usd", "CAO-DF framework", "tab:blue"),
                  ("tc_oracle_usd", "Oracle", "tab:gray")]
    fig, axes = plt.subplots(1, len(workloads), figsize=(3.2 * len(workloads), 2.8), sharey=True, squeeze=False)
    x = np.arange(len(N_GRID))
    width = 0.27
    for ax, workload in zip(axes.flat, workloads, strict=False):
        rows = data[data["workload_id"] == workload].set_index("n_runs").loc[list(N_GRID)]
        for i, (column, label, color) in enumerate(strategies):
            ax.bar(x + (i - 1) * width, rows[column] / rows["tc_static_usd"], width, label=label, color=color)
        for xi, choice in zip(x, rows["framework_choice"], strict=False):
            ax.annotate(choice[0], (xi, 0.02), ha="center", fontsize=7, color="white", weight="bold")
        ax.axhline(1.0, color="tab:red", linewidth=1, label="Static (= 1)")
        ax.set_xticks(x, [str(n) for n in N_GRID])
        ax.set_xlabel("future runs N", fontsize=8)
        ax.set_title(workload.removeprefix("V_").replace("_", " "), fontsize=9)
        ax.tick_params(labelsize=7)
    axes.flat[0].set_ylabel("total cost / static total cost", fontsize=8)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, fontsize=7, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out_base.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_base.with_suffix(".pdf"))
    fig.savefig(out_base.with_suffix(".png"), dpi=150)
    plt.close(fig)


# --- Interactive use --------------------------------------------------------------

def ask(prompt: str, cast, default=None, check=lambda v: True, error: str = "invalid value",
        reader=input, writer=print):
    """Ask until the answer casts and passes ``check``; an empty answer takes ``default``."""
    suffix = f" [{default}]" if default is not None else ""
    while True:
        text = reader(f"{prompt}{suffix}: ").strip()
        if not text and default is not None:
            return default
        try:
            value = cast(text.replace(",", "").replace("_", ""))
        except ValueError:
            writer(f"  {error}")
            continue
        if check(value):
            return value
        writer(f"  {error}")


def ask_workload(profiles: pd.DataFrame, reader=input, writer=print) -> dict:
    """Collect the four inputs of the rule (and the cost proxy) from the user."""
    writer("CAO-DF decision tool -- answer five questions about the pipeline.\n")
    orders = ask("1. How many rows does the main (orders) table have", int,
                 check=lambda v: v > 0, error="enter a whole number above 0", reader=reader, writer=writer)
    files = ask("2. How many input files is that table stored in", int,
                check=lambda v: v > 0, error="enter a whole number above 0", reader=reader, writer=writer)
    shuffle = ask("3. What share of the rows reaches the join/aggregation shuffle (0-1)", float,
                  check=lambda v: 0 < v <= 1, error="enter a number above 0 and at most 1",
                  reader=reader, writer=writer)
    runs = ask("4. How many more times will the pipeline run (N)", int,
               check=lambda v: v > 0, error="enter a whole number above 0", reader=reader, writer=writer)
    proxy = ask("5. Cost proxy: P1 = whole machine, P2 = executor time only", str.upper, default="P1",
                check=lambda v: v in PROXIES, error="enter P1 or P2", reader=reader, writer=writer)

    study = profiles.drop_duplicates("workload_id")
    outside = []
    if not study["orders"].min() <= orders <= study["orders"].max():
        outside.append(f"rows ({study['orders'].min():,}-{study['orders'].max():,})")
    if not study["files"].min() <= files <= study["files"].max():
        outside.append(f"files ({study['files'].min()}-{study['files'].max()})")
    if outside:
        writer(f"\nNote: {' and '.join(outside)} lie outside the measured study range; "
               "the estimate is extrapolated from the nearest study workload.")
    return {"orders": orders, "files": files, "shuffle": shuffle, "runs": runs, "proxy": proxy}


def print_decision(decision: Decision, n_runs: int, writer=print) -> None:
    writer(f"Recommendation for N = {n_runs}: {decision.choice}")
    for option, tc in decision.predicted_tc.items():
        writer(f"  predicted total cost of {option:<8}: {tc:6.2f} static runs")
    for action, est in decision.estimates.items():
        writer(f"  {ACTIONS[action]:<8} estimated from {', '.join(est.neighbours)}: "
               f"saving {est.saving_pct:.1%}, one-time cost {est.copt_ratio:.2f} static runs")
    skipped = [ACTIONS[a] for a in ACTIONS if ACTIONS[a] not in decision.predicted_tc]
    if skipped:
        writer(f"  ({', '.join(skipped)} left out: estimated saving below the noise floor)")


# --- Entry point ------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CAO-DF decision rule and its validation.")
    parser.add_argument("--delta", type=float, default=DEFAULT_DELTA,
                        help="noise floor as a fraction of one static run (default 0.10)")
    parser.add_argument("--price", type=float, default=PRICE_PER_VCPU_HOUR_USD, help="USD per vCPU-hour")
    parser.add_argument("--vcpus", type=int, default=STUDY_MACHINE_VCPUS, help="vCPUs charged under P1")
    parser.add_argument("--decide", action="store_true", help="only print a decision for one workload")
    parser.add_argument("--orders", type=int, help="order rows (with --decide)")
    parser.add_argument("--files", type=int, help="input file count (with --decide)")
    parser.add_argument("--shuffle", type=float, help="fraction of rows that reach the shuffle (with --decide)")
    parser.add_argument("--runs", type=int, help="expected future runs N (with --decide)")
    parser.add_argument("--proxy", choices=PROXIES, default="P1", help="cost proxy (with --decide)")
    parser.add_argument("--interactive", action="store_true",
                        help="ask for the workload details and print a decision")
    args = parser.parse_args()

    profiles = training_profiles(args.price, args.vcpus)

    if args.interactive:
        try:
            answers = ask_workload(profiles)
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled.")
            return
        args.decide = True
        args.orders, args.files, args.shuffle = answers["orders"], answers["files"], answers["shuffle"]
        args.runs, args.proxy = answers["runs"], answers["proxy"]
        print()

    if args.decide:
        if None in (args.orders, args.files, args.shuffle, args.runs):
            parser.error("--decide needs --orders, --files, --shuffle and --runs")
        if args.orders <= 0 or args.files <= 0 or not 0 < args.shuffle <= 1 or args.runs <= 0:
            parser.error("--orders, --files and --runs must be above 0; --shuffle must be in (0, 1]")
        features = pd.Series({"log_orders": math.log10(args.orders), "log_files": math.log10(args.files),
                              "shuffle_fraction": args.shuffle})
        print_decision(decide(profiles, features, args.runs, args.proxy, args.delta), args.runs)
        return

    table = validate(profiles, load_validation_cells(args.price, args.vcpus), args.delta)
    summary = summarize(table)
    loo = leave_one_out(profiles, args.delta)

    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    table.to_csv(DECISIONS_OUTPUT, index=False, float_format="%.10g")
    summary.to_csv(SUMMARY_OUTPUT, index=False, float_format="%.10g")
    loo.to_csv(LOO_OUTPUT, index=False)
    for proxy in PROXIES:
        plot_validation(table, proxy, RESULTS_FIGURES_DIR / f"framework_vs_static_{proxy.lower()}")
    PAPER_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(RESULTS_FIGURES_DIR / "framework_vs_static_p1.pdf", PAPER_FIGURES_DIR / "framework_vs_static_p1.pdf")

    pd.set_option("display.width", 220)
    view = table[["workload_id", "proxy", "n_runs", "framework_choice", "oracle_choice",
                  "performance_only_choice", "framework_reduction_pct", "performance_only_reduction_pct",
                  "framework_regret_pct", "framework_vs_static"]].copy()
    for col in ("framework_reduction_pct", "performance_only_reduction_pct", "framework_regret_pct"):
        view[col] = (100 * view[col]).round(1)
    print(f"Noise floor delta = {args.delta:.0%} of a static run\n")
    print(view.to_string(index=False))
    print("\n" + summary.round(4).to_string(index=False))
    print(f"\nLeave-one-out agreement on the study workloads: {loo['agree'].mean():.0%} "
          f"({int(loo['agree'].sum())}/{len(loo)})")
    print(f"Wrote {DECISIONS_OUTPUT.name}, {SUMMARY_OUTPUT.name}, {LOO_OUTPUT.name} and figures")


if __name__ == "__main__":
    main()
