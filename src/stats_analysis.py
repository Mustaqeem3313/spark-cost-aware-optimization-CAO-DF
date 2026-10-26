"""Phase 10 -- statistical analysis of the experiments and the H1 / H2 verdicts. No Spark needed.

Implements the data analysis plan of the master document (section 22):

1. **Data checks** -- schema, pipeline version, run counts, one run per cell and block.
2. **Descriptive statistics** -- median, IQR, mean, SD, CV, min and max per cell.
3. **Effect sizes** -- speed-up ``median(default) / median(best)``, percent saving,
   Cliff's delta and the share of blocks in which the action was faster.
4. **Uncertainty** -- 95 % *block-paired* bootstrap confidence intervals: whole
   repetition blocks are resampled (10 000 times) and every cell of the experiment
   is taken from the same resampled blocks, which keeps the block pairing.
5. **A2 trend** -- compaction benefit against file count (monotonicity, Spearman
   correlation with a permutation test).
6. **Break-even uncertainty** -- bootstrap distribution of ``N*`` per workload and action.
7. **H2 tests** -- paired comparison of CAO-DF with the static configuration on the
   unseen workloads (case and workload-cluster bootstrap, Wilcoxon signed-rank and
   sign tests) and a per-block re-evaluation.
8. **Verdicts** -- H1 and H2 judged against the criteria fixed in the master document (section 9).

Statistical notes. With 5 blocks an exact two-sided Wilcoxon signed-rank test cannot
go below ``p = 0.0625``, so effect sizes and confidence intervals carry the evidence and
p-values are reported for completeness only. Percentile bootstrap intervals from 5
blocks are coarse and tend to be too narrow; they are read as "differences larger than
the observed block-to-block variation", not as exact 95 % coverage.

Outputs (``results/processed/stats_*.csv``) are listed in :data:`OUTPUTS`. The notebook
``analysis/analysis.ipynb`` calls the same functions and draws the figures.

Usage::

    python src/stats_analysis.py
"""

from __future__ import annotations

import itertools
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

from src.cost_model import (  # noqa: E402
    PRICE_PER_VCPU_HOUR_USD,
    PROXIES,
    STATIC_PARTITIONS,
    STUDY_MACHINE_VCPUS,
    add_costs,
)
from src.decision_framework import (  # noqa: E402
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    DEFAULT_DELTA,
    DECISIONS_OUTPUT,
    bootstrap_ci,
    measured_options,
    measured_tc,
    outcome,
)
from src.paths import RESULTS_CSV_PATH, RESULTS_PROCESSED_DIR, WORKLOAD_INDEX_PATH  # noqa: E402

EXPECTED_COLUMNS = 34
EXPECTED_RUNS = {"A1": 160, "A2": 90, "VAL": 120}
EXPECTED_BLOCKS = {"A1": 5, "A2": 5, "VAL": 3}
CONFIDENCE = 0.95
CV_THRESHOLD = 0.20
H1_MIN_EFFECT = 0.15
NOISE_FLOOR = DEFAULT_DELTA
CLEAR_MAJORITY = 2 / 3
METRICS = {"wall": "wall_clock_seconds", "executor": "executor_run_s"}
A2_STAGES = ("fragmented", "compaction", "compacted")

OUTPUTS = {
    "checks": RESULTS_PROCESSED_DIR / "stats_data_checks.csv",
    "descriptive": RESULTS_PROCESSED_DIR / "stats_descriptive.csv",
    "a1_effects": RESULTS_PROCESSED_DIR / "stats_a1_effects.csv",
    "a1_pairwise": RESULTS_PROCESSED_DIR / "stats_a1_pairwise.csv",
    "a2_effects": RESULTS_PROCESSED_DIR / "stats_a2_effects.csv",
    "a2_trend": RESULTS_PROCESSED_DIR / "stats_a2_trend.csv",
    "breakeven": RESULTS_PROCESSED_DIR / "stats_breakeven_ci.csv",
    "h2_tests": RESULTS_PROCESSED_DIR / "stats_h2_tests.csv",
    "h2_by_block": RESULTS_PROCESSED_DIR / "stats_h2_by_block.csv",
    "verdicts": RESULTS_PROCESSED_DIR / "stats_hypotheses.csv",
}


# --- Loading and checks -----------------------------------------------------------

def load_runs(path: Path = RESULTS_CSV_PATH) -> pd.DataFrame:
    """Measured runs of every experiment with block number, executor seconds and P1/P2 costs."""
    df = pd.read_csv(path)
    warmup = df["is_warmup"].astype(str).str.lower().isin(["true", "1"])
    df = df[~warmup].copy()
    df["block"] = df["repetition_number"].astype(int)
    df["executor_run_s"] = df["executor_run_time_ms"] / 1000.0
    df["shuffle_mb"] = df["shuffle_bytes_written"] / 1e6
    return add_costs(df, PRICE_PER_VCPU_HOUR_USD, STUDY_MACHINE_VCPUS)


def data_checks(path: Path = RESULTS_CSV_PATH) -> pd.DataFrame:
    """Integrity checks that must all pass before any statistic is trusted."""
    raw = pd.read_csv(path)
    runs = load_runs(path)
    warmups = int(len(raw) - len(runs))
    checks = [
        ("Raw file has the 34-column schema", len(raw.columns) == EXPECTED_COLUMNS, f"{len(raw.columns)} columns"),
        ("No warm-up rows in the raw file", warmups == 0, f"{warmups} warm-up rows"),
        ("Every measured run uses pipeline v2", bool((runs["pipeline_version"] == "v2").all()),
         ", ".join(sorted(runs["pipeline_version"].unique()))),
    ]
    for action, expected in EXPECTED_RUNS.items():
        sub = runs[runs["experiment_action"] == action]
        checks.append((f"{action}: {expected} measured runs", len(sub) == expected, f"{len(sub)} runs"))
        blocks = sub["block"].nunique()
        checks.append((f"{action}: {EXPECTED_BLOCKS[action]} blocks", blocks == EXPECTED_BLOCKS[action],
                       f"{blocks} blocks"))
    duplicates = int(runs.duplicated(["experiment_action", "workload_id", "config_label", "block"]).sum())
    checks.append(("One run per cell and block", duplicates == 0, f"{duplicates} duplicates"))
    missing = int(runs[["wall_clock_seconds", "executor_run_time_ms"]].isna().sum().sum())
    checks.append(("No missing wall-clock or executor times", missing == 0, f"{missing} missing values"))
    metrics_ok = runs["sparkmeasure_available"].astype(str).str.lower().eq("true").all()
    checks.append(("sparkMeasure metrics captured for every run", bool(metrics_ok), ""))
    spilled = int((runs["disk_bytes_spilled"] > 0).sum())
    checks.append(("No disk spill in any run", spilled == 0, f"{spilled} runs with spill"))
    return pd.DataFrame(checks, columns=["check", "passed", "detail"])


# --- Descriptive statistics ---------------------------------------------------------

def _iqr(s: pd.Series) -> float:
    return float(s.quantile(0.75) - s.quantile(0.25))


def describe_cells(runs: pd.DataFrame) -> pd.DataFrame:
    """Median, IQR, mean, SD, CV, min and max of wall-clock time per cell, plus Spark metrics."""
    grouped = runs.groupby(["experiment_action", "workload_id", "config_label"], sort=False)
    cells = grouped.agg(
        shuffle_partitions=("shuffle_partitions", "first"),
        n=("wall_clock_seconds", "size"),
        median_wall_s=("wall_clock_seconds", "median"),
        iqr_wall_s=("wall_clock_seconds", _iqr),
        mean_wall_s=("wall_clock_seconds", "mean"),
        sd_wall_s=("wall_clock_seconds", "std"),
        min_wall_s=("wall_clock_seconds", "min"),
        max_wall_s=("wall_clock_seconds", "max"),
        median_executor_s=("executor_run_s", "median"),
        iqr_executor_s=("executor_run_s", _iqr),
        median_tasks=("task_count", "median"),
        median_shuffle_mb=("shuffle_mb", "median"),
        median_cost_P1=("cost_P1", "median"),
        median_cost_P2=("cost_P2", "median"),
    ).reset_index()
    cells["cv_wall"] = cells["sd_wall_s"] / cells["mean_wall_s"]
    cells["noisy"] = cells["cv_wall"] > CV_THRESHOLD
    return cells.sort_values(["experiment_action", "workload_id", "shuffle_partitions", "config_label"],
                             ignore_index=True)


def noise_overview(cells: pd.DataFrame) -> pd.DataFrame:
    """Number of noisy cells (CV above the threshold) and the CV distribution per experiment."""
    return cells.groupby("experiment_action").agg(
        cells=("cv_wall", "size"),
        noisy_cells=("noisy", "sum"),
        median_cv=("cv_wall", "median"),
        max_cv=("cv_wall", "max"),
    ).reset_index()


# --- Bootstrap and effect-size helpers ---------------------------------------------

def block_resamples(n_blocks: int, resamples: int = BOOTSTRAP_RESAMPLES,
                    seed: int = BOOTSTRAP_SEED) -> np.ndarray:
    """Indices of resampled blocks, shape ``(resamples, n_blocks)``, shared by every cell."""
    return np.random.default_rng(seed).integers(0, n_blocks, size=(resamples, n_blocks))


def percentile_ci(samples: np.ndarray, level: float = CONFIDENCE) -> tuple[float, float]:
    tail = (1 - level) / 2 * 100
    low, high = np.percentile(samples, [tail, 100 - tail])
    return float(low), float(high)


def cliffs_delta(x: np.ndarray, y: np.ndarray) -> float:
    """Cliff's delta: P(x > y) - P(x < y) over all pairs; +1 means every x exceeds every y."""
    x, y = np.asarray(x, float)[:, None], np.asarray(y, float)[None, :]
    return float((x > y).mean() - (x < y).mean())


def cliffs_label(delta: float) -> str:
    """Magnitude labels of Romano et al. (2006)."""
    d = abs(delta)
    return "negligible" if d < 0.147 else "small" if d < 0.33 else "medium" if d < 0.474 else "large"


def wilcoxon_exact_p(x: np.ndarray, y: np.ndarray, alternative: str = "two-sided") -> float:
    """Exact Wilcoxon signed-rank p-value of the paired differences ``x - y`` (zeros dropped)."""
    d = np.asarray(x, float) - np.asarray(y, float)
    d = d[d != 0]
    if len(d) == 0:
        return 1.0
    return float(stats.wilcoxon(d, alternative=alternative, method="exact").pvalue)


def _matrix(runs: pd.DataFrame, action: str, workload: str, column: str, by: str) -> pd.DataFrame:
    """Blocks x settings matrix of one measure for one workload."""
    sub = runs[(runs["experiment_action"] == action) & (runs["workload_id"] == workload)]
    return sub.pivot(index="block", columns=by, values=column).sort_index()


def _boot_medians(matrix: pd.DataFrame, idx: np.ndarray) -> np.ndarray:
    """Median of every column over each set of resampled blocks, shape ``(resamples, columns)``."""
    return np.median(matrix.to_numpy()[idx], axis=1)


def _vector_break_even(copt: np.ndarray, static: np.ndarray, action: np.ndarray) -> np.ndarray:
    """Vectorised :func:`src.cost_model.break_even_runs`."""
    saving = static - action
    with np.errstate(divide="ignore", invalid="ignore"):
        n_star = np.maximum(1, np.ceil(np.round(copt / saving, 9)))
    return np.where(saving > 0, n_star, np.inf)


# --- A1: partition tuning --------------------------------------------------------------

@dataclass
class SpeedupSamples:
    best: int
    point: float
    samples: np.ndarray


def a1_workloads(runs: pd.DataFrame) -> list[str]:
    return sorted(runs.loc[runs["experiment_action"] == "A1", "workload_id"].unique())


def a1_speedups(runs: pd.DataFrame, metric: str, idx: np.ndarray) -> dict[str, SpeedupSamples]:
    """Speed-up of the best setting over the default (200) per A1 workload, with bootstrap samples.

    The best setting is fixed from the full data, so the bootstrap measures the
    uncertainty of the speed-up, not of the choice (reported separately).
    """
    out = {}
    for workload in a1_workloads(runs):
        m = _matrix(runs, "A1", workload, METRICS[metric], "shuffle_partitions")
        med = m.median()
        best = int(med.idxmin())
        boot = _boot_medians(m, idx)
        d, b = list(m.columns).index(STATIC_PARTITIONS), list(m.columns).index(best)
        out[workload] = SpeedupSamples(best, float(med[STATIC_PARTITIONS] / med[best]), boot[:, d] / boot[:, b])
    return out


def a1_effects(runs: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    """Best setting, near-optimal plateau, speed-up with CI and effect sizes per workload and metric."""
    rows = []
    for metric in METRICS:
        speedups = a1_speedups(runs, metric, idx)
        for workload, sp in speedups.items():
            m = _matrix(runs, "A1", workload, METRICS[metric], "shuffle_partitions")
            med = m.median()
            boot = _boot_medians(m, idx)
            winners = np.array(m.columns)[boot.argmin(axis=1)]
            plateau = [int(c) for c in m.columns if med[c] <= med[sp.best] * (1 + NOISE_FLOOR)]
            low, high = percentile_ci(sp.samples)
            default, best = m[STATIC_PARTITIONS].to_numpy(), m[sp.best].to_numpy()
            delta = cliffs_delta(default, best)
            rows.append({
                "workload_id": workload,
                "metric": metric,
                "best_partitions": sp.best,
                "plateau_partitions": " ".join(map(str, plateau)),
                "best_share_of_resamples": float((winners == sp.best).mean()),
                "median_default_s": float(med[STATIC_PARTITIONS]),
                "median_best_s": float(med[sp.best]),
                "speedup": sp.point,
                "speedup_ci_low": low,
                "speedup_ci_high": high,
                "saving_pct": 1 - float(med[sp.best] / med[STATIC_PARTITIONS]),
                "blocks_best_faster": int((best < default).sum()),
                "blocks": len(default),
                "cliffs_delta": delta,
                "cliffs_magnitude": cliffs_label(delta),
                "wilcoxon_p_exact": wilcoxon_exact_p(default, best),
            })
    return pd.DataFrame(rows)


def a1_pairwise(runs: pd.DataFrame, idx: np.ndarray, effects: pd.DataFrame,
                min_effect: float = H1_MIN_EFFECT) -> pd.DataFrame:
    """H1 (A1) test for every workload pair: do the speed-ups or the best settings differ beyond noise?

    * Pre-registered criterion: the two 95 % CIs of the speed-up do not overlap and the
      larger speed-up exceeds the smaller by at least ``min_effect``.
    * Secondary: the CI of the speed-up *ratio* (joint block resampling) excludes 1, at 95 %
      and at the Bonferroni level for all pairs.
    * Best setting: the near-optimal plateaus (settings within the noise floor of the best) are disjoint.
    """
    rows = []
    workloads = a1_workloads(runs)
    pairs = list(itertools.combinations(workloads, 2))
    bonferroni = 1 - (1 - CONFIDENCE) / len(pairs)
    for metric in METRICS:
        speedups = a1_speedups(runs, metric, idx)
        plateaus = effects[effects["metric"] == metric].set_index("workload_id")["plateau_partitions"]
        for a, b in pairs:
            sa, sb = speedups[a], speedups[b]
            ci_a, ci_b = percentile_ci(sa.samples), percentile_ci(sb.samples)
            overlap = not (ci_a[1] < ci_b[0] or ci_b[1] < ci_a[0])
            ratio = sa.samples / sb.samples
            r_low, r_high = percentile_ci(ratio)
            rb_low, rb_high = percentile_ci(ratio, bonferroni)
            effect = max(sa.point, sb.point) / min(sa.point, sb.point) - 1
            disjoint = not (set(plateaus[a].split()) & set(plateaus[b].split()))
            rows.append({
                "metric": metric,
                "workload_a": a,
                "workload_b": b,
                "speedup_a": sa.point,
                "speedup_b": sb.point,
                "cis_overlap": overlap,
                "relative_difference": effect,
                "ratio": sa.point / sb.point,
                "ratio_ci_low": r_low,
                "ratio_ci_high": r_high,
                "ratio_ci_excludes_1": not (r_low <= 1 <= r_high),
                "ratio_ci_bonferroni_excludes_1": not (rb_low <= 1 <= rb_high),
                "meets_h1_speedup": (not overlap) and effect >= min_effect,
                "best_a": int(sa.best),
                "best_b": int(sb.best),
                "plateaus_disjoint": disjoint,
            })
    return pd.DataFrame(rows)


# --- A2: compaction ----------------------------------------------------------------

def a2_workloads(runs: pd.DataFrame) -> pd.DataFrame:
    """A2 workloads with volume and actual file count, ordered by volume then file count."""
    index = pd.read_csv(WORKLOAD_INDEX_PATH)
    a2 = index[index["workload_id"].str.startswith("A2_")]
    a2 = a2[a2["workload_id"].isin(runs["workload_id"].unique())]
    return a2[["workload_id", "data_volume", "actual_file_count"]].sort_values(
        ["data_volume", "actual_file_count"]).reset_index(drop=True)


def a2_benefit_samples(runs: pd.DataFrame, metric: str, idx: np.ndarray) -> dict[str, dict]:
    """Per-block paired benefit (before - after) / before and its bootstrap median per workload."""
    out = {}
    for workload in a2_workloads(runs)["workload_id"]:
        m = _matrix(runs, "A2", workload, METRICS[metric], "config_label")[list(A2_STAGES)]
        benefit = ((m["fragmented"] - m["compacted"]) / m["fragmented"]).to_numpy()
        out[workload] = {
            "matrix": m,
            "benefit": benefit,
            "point": float(np.median(benefit)),
            "samples": np.median(benefit[idx], axis=1),
        }
    return out


def a2_effects(runs: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    """Compaction benefit with CI, one-time cost and effect sizes per workload and metric."""
    meta = a2_workloads(runs).set_index("workload_id")
    rows = []
    for metric in METRICS:
        for workload, s in a2_benefit_samples(runs, metric, idx).items():
            m = s["matrix"]
            boot = _boot_medians(m, idx)
            copt_ratio = boot[:, 1] / boot[:, 0]
            low, high = percentile_ci(s["samples"])
            delta = cliffs_delta(m["fragmented"], m["compacted"])
            rows.append({
                "workload_id": workload,
                "data_volume": meta.loc[workload, "data_volume"],
                "files": int(meta.loc[workload, "actual_file_count"]),
                "metric": metric,
                "median_before_s": float(m["fragmented"].median()),
                "median_after_s": float(m["compacted"].median()),
                "median_copt_s": float(m["compaction"].median()),
                "benefit_pct": s["point"],
                "benefit_ci_low": low,
                "benefit_ci_high": high,
                "benefit_above_noise_floor": low > NOISE_FLOOR,
                "copt_ratio": float(m["compaction"].median() / m["fragmented"].median()),
                "copt_ratio_ci_low": percentile_ci(copt_ratio)[0],
                "copt_ratio_ci_high": percentile_ci(copt_ratio)[1],
                "blocks_after_faster": int((m["compacted"] < m["fragmented"]).sum()),
                "blocks": len(m),
                "cliffs_delta": delta,
                "cliffs_magnitude": cliffs_label(delta),
                "wilcoxon_p_exact": wilcoxon_exact_p(m["fragmented"], m["compacted"]),
            })
    return pd.DataFrame(rows)


def spearman_permutation(x: np.ndarray, y: np.ndarray, permutations: int = BOOTSTRAP_RESAMPLES,
                         seed: int = BOOTSTRAP_SEED) -> tuple[float, float]:
    """Spearman's rho and a one-sided (rho > 0) permutation p-value."""
    rho = float(stats.spearmanr(x, y).statistic)
    rng = np.random.default_rng(seed)
    null = np.array([stats.spearmanr(x, rng.permutation(y)).statistic for _ in range(permutations)])
    return rho, float((np.sum(null >= rho - 1e-12) + 1) / (permutations + 1))


def a2_trend(runs: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    """H1 (A2): is the benefit monotonic in file count (1 -> 10 -> 50) for each data volume?"""
    meta = a2_workloads(runs)
    rows = []
    for metric in METRICS:
        samples = a2_benefit_samples(runs, metric, idx)
        for volume, group in meta.groupby("data_volume"):
            low_w, mid_w, high_w = group.sort_values("actual_file_count")["workload_id"]
            b = {k: samples[w] for k, w in zip(("low", "medium", "high"), (low_w, mid_w, high_w))}
            monotone_boot = (b["low"]["samples"] < b["medium"]["samples"]) & (
                b["medium"]["samples"] < b["high"]["samples"])
            files = np.repeat(group.sort_values("actual_file_count")["actual_file_count"].to_numpy(),
                              len(b["low"]["benefit"]))
            values = np.concatenate([b[k]["benefit"] for k in ("low", "medium", "high")])
            rho, p_perm = spearman_permutation(files, values)
            mid_low = b["medium"]["samples"] - b["low"]["samples"]
            high_mid = b["high"]["samples"] - b["medium"]["samples"]
            rows.append({
                "metric": metric,
                "data_volume": volume,
                "benefit_1_file": b["low"]["point"],
                "benefit_10_files": b["medium"]["point"],
                "benefit_50_files": b["high"]["point"],
                "monotonic": b["low"]["point"] < b["medium"]["point"] < b["high"]["point"],
                "monotonic_share_of_resamples": float(monotone_boot.mean()),
                "gain_1_to_10": b["medium"]["point"] - b["low"]["point"],
                "gain_1_to_10_ci_low": percentile_ci(mid_low)[0],
                "gain_1_to_10_ci_high": percentile_ci(mid_low)[1],
                "gain_10_to_50": b["high"]["point"] - b["medium"]["point"],
                "gain_10_to_50_ci_low": percentile_ci(high_mid)[0],
                "gain_10_to_50_ci_high": percentile_ci(high_mid)[1],
                "high_ci_low": percentile_ci(b["high"]["samples"])[0],
                "high_above_noise_floor": percentile_ci(b["high"]["samples"])[0] > NOISE_FLOOR,
                "spearman_rho": rho,
                "spearman_p_permutation": p_perm,
            })
    return pd.DataFrame(rows)


# --- Break-even uncertainty -----------------------------------------------------------

def _n_star_summary(samples: np.ndarray, point: float) -> dict:
    finite = samples[np.isfinite(samples)]
    values, counts = np.unique(samples, return_counts=True)
    return {
        "n_star": point,
        "n_star_share_same": float(np.mean(samples == point)),
        "n_star_mode": float(values[counts.argmax()]),
        "n_star_min": float(samples.min()),
        "n_star_max": float(samples.max()),
        "n_star_share_finite": float(len(finite) / len(samples)),
    }


def breakeven_uncertainty(runs: pd.DataFrame, idx: np.ndarray) -> pd.DataFrame:
    """Saving, one-time cost ratio and ``N*`` with bootstrap intervals for each workload, action and proxy.

    Definitions match ``src/cost_model.py``: A1 search ``Copt`` is the sum of the median
    cost of every setting and the action runs at the setting with the lowest median cost;
    A2 ``Copt`` is the compaction job.
    """
    rows = []
    for proxy in PROXIES:
        column = f"cost_{proxy}"
        for workload in a1_workloads(runs):
            m = _matrix(runs, "A1", workload, column, "shuffle_partitions")
            med, boot = m.median(), _boot_medians(m, idx)
            best = med.idxmin()
            d, b = list(m.columns).index(STATIC_PARTITIONS), list(m.columns).index(best)
            rows.append(_breakeven_row(workload, "A1_search", proxy, med.sum(), med[STATIC_PARTITIONS],
                                       med[best], boot.sum(axis=1), boot[:, d], boot[:, b]))
        for workload in a2_workloads(runs)["workload_id"]:
            m = _matrix(runs, "A2", workload, column, "config_label")[list(A2_STAGES)]
            med, boot = m.median(), _boot_medians(m, idx)
            rows.append(_breakeven_row(workload, "A2", proxy, med["compaction"], med["fragmented"],
                                       med["compacted"], boot[:, 1], boot[:, 0], boot[:, 2]))
    return pd.DataFrame(rows)


def _breakeven_row(workload, action, proxy, copt, static, crun, copt_s, static_s, crun_s) -> dict:
    saving = 1 - crun_s / static_s
    ratio = copt_s / static_s
    point = _vector_break_even(np.array([copt]), np.array([static]), np.array([crun]))[0]
    return {
        "workload_id": workload,
        "action": action,
        "proxy": proxy,
        "saving_pct": 1 - crun / static,
        "saving_ci_low": percentile_ci(saving)[0],
        "saving_ci_high": percentile_ci(saving)[1],
        "copt_ratio": copt / static,
        "copt_ratio_ci_low": percentile_ci(ratio)[0],
        "copt_ratio_ci_high": percentile_ci(ratio)[1],
        **_n_star_summary(_vector_break_even(copt_s, static_s, crun_s), float(point)),
    }


# --- H2: framework validation --------------------------------------------------------

def load_decisions(path: Path = DECISIONS_OUTPUT) -> pd.DataFrame:
    return pd.read_csv(path)


def _cluster_bootstrap_mean(values: pd.Series, clusters: pd.Series, resamples: int = BOOTSTRAP_RESAMPLES,
                            seed: int = BOOTSTRAP_SEED) -> tuple[float, float]:
    """CI of the mean when whole clusters (workloads) are resampled -- the conservative interval."""
    groups = [values[clusters == c].to_numpy() for c in clusters.unique()]
    rng = np.random.default_rng(seed)
    means = np.empty(resamples)
    for i in range(resamples):
        chosen = rng.integers(0, len(groups), len(groups))
        means[i] = np.concatenate([groups[j] for j in chosen]).mean()
    return percentile_ci(means)


def h2_tests(decisions: pd.DataFrame) -> pd.DataFrame:
    """Paired tests of CAO-DF against static (and against performance-only) per cost proxy."""
    rows = []
    for proxy, g in decisions.groupby("proxy"):
        red = g["framework_reduction_pct"]
        perf = g["performance_only_reduction_pct"]
        counts = g["framework_vs_static"].value_counts()
        wins, ties, losses = (int(counts.get(k, 0)) for k in ("win", "tie", "loss"))
        case_ci = bootstrap_ci(red.to_numpy())
        cluster_ci = _cluster_bootstrap_mean(red, g["workload_id"])
        diff = red - perf
        rows.append({
            "proxy": proxy,
            "cases": len(g),
            "wins": wins,
            "ties": ties,
            "losses": losses,
            "no_worse_share": (wins + ties) / len(g),
            "mean_reduction": float(red.mean()),
            "mean_ci_case_low": case_ci[0],
            "mean_ci_case_high": case_ci[1],
            "mean_ci_workload_low": cluster_ci[0],
            "mean_ci_workload_high": cluster_ci[1],
            "median_reduction": float(red.median()),
            "total_cost_reduction": 1 - g["tc_framework_usd"].sum() / g["tc_static_usd"].sum(),
            "wilcoxon_p_one_sided": wilcoxon_exact_p(red.to_numpy(), np.zeros(len(red)), "greater"),
            "sign_test_p_one_sided": float(stats.binomtest(wins, wins + losses, 0.5, "greater").pvalue)
            if wins + losses else 1.0,
            "mean_gain_vs_performance_only": float(diff.mean()),
            "gain_vs_performance_only_ci_low": bootstrap_ci(diff.to_numpy())[0],
            "gain_vs_performance_only_ci_high": bootstrap_ci(diff.to_numpy())[1],
            "wilcoxon_p_vs_performance_only": wilcoxon_exact_p(red.to_numpy(), perf.to_numpy(), "greater"),
            "mean_gap_to_oracle": float((g["oracle_reduction_pct"] - red).mean()),
            "matches_oracle": int(g["framework_matches_oracle"].sum()),
        })
    return pd.DataFrame(rows)


def h2_by_block(runs: pd.DataFrame, decisions: pd.DataFrame, delta: float = NOISE_FLOOR) -> pd.DataFrame:
    """Re-score CAO-DF's (fixed) choices with the costs of each validation block on its own."""
    val = runs[runs["experiment_action"] == "VAL"]
    rows = []
    for block, runs_b in val.groupby("block"):
        cells = runs_b.groupby(["workload_id", "config_label"])[["cost_P1", "cost_P2"]].median().reset_index()
        for proxy, g in decisions.groupby("proxy"):
            reductions, outcomes, tc_s, tc_f = [], [], 0.0, 0.0
            for _, case in g.iterrows():
                options = measured_options(cells, case["workload_id"], proxy)
                static = measured_tc(options, "Nothing", case["n_runs"])
                chosen = measured_tc(options, case["framework_choice"], case["n_runs"])
                reductions.append(1 - chosen / static)
                outcomes.append(outcome(chosen, static, delta))
                tc_s, tc_f = tc_s + static, tc_f + chosen
            rows.append({
                "block": int(block),
                "proxy": proxy,
                "wins": outcomes.count("win"),
                "ties": outcomes.count("tie"),
                "losses": outcomes.count("loss"),
                "mean_reduction": float(np.mean(reductions)),
                "total_cost_reduction": 1 - tc_f / tc_s,
            })
    return pd.DataFrame(rows)


# --- Verdicts -----------------------------------------------------------------------

def hypothesis_verdicts(a1_pairs: pd.DataFrame, a1_eff: pd.DataFrame, trend: pd.DataFrame,
                        a2_eff: pd.DataFrame, h2: pd.DataFrame) -> pd.DataFrame:
    """Judge H1 (A1, A2) and H2 against the criteria fixed before the experiments."""
    rows = []
    for metric in METRICS:
        pairs = a1_pairs[a1_pairs["metric"] == metric]
        eff = a1_eff[a1_eff["metric"] == metric]
        n_speed = int(pairs["meets_h1_speedup"].sum())
        n_best = int(pairs["plateaus_disjoint"].sum())
        supported = n_speed > 0 or n_best > 0
        rows.append({
            "hypothesis": "H1 (A1)",
            "metric": metric,
            "criterion": "Best setting or speed-up differs between >= 1 workload pair beyond noise "
                         "(non-overlapping 95 % CIs, effect >= 15 %)",
            "evidence": f"{n_speed}/{len(pairs)} pairs meet the speed-up criterion; {n_best}/{len(pairs)} pairs "
                        f"have disjoint near-optimal settings; speed-ups {eff['speedup'].min():.2f}-"
                        f"{eff['speedup'].max():.2f}x, all {eff['blocks_best_faster'].min()}/"
                        f"{eff['blocks'].max()} blocks faster",
            "verdict": "Supported" if supported else "Not supported",
        })
    for metric in METRICS:
        t = trend[trend["metric"] == metric]
        monotone = bool(t["monotonic"].all())
        above = bool(t["high_above_noise_floor"].all())
        eff = a2_eff[a2_eff["metric"] == metric]
        verdict = "Supported" if monotone and above else "Partly supported" if above else "Not supported"
        rows.append({
            "hypothesis": "H1 (A2)",
            "metric": metric,
            "criterion": "Benefit rises monotonically with file count (1 -> 10 -> 50); "
                         "50-file benefit outside the noise band",
            "evidence": "; ".join(
                f"{r.data_volume}: {r.benefit_1_file:.1%} -> {r.benefit_10_files:.1%} -> {r.benefit_50_files:.1%} "
                f"(10->50 gain CI {r.gain_10_to_50_ci_low:+.1%} to {r.gain_10_to_50_ci_high:+.1%})"
                for r in t.itertuples()) + f"; benefit range {eff['benefit_pct'].min():.1%} to {eff['benefit_pct'].max():.1%}",
            "verdict": verdict,
        })
    for r in h2.itertuples():
        majority = r.no_worse_share >= CLEAR_MAJORITY
        lower = r.mean_reduction > 0
        ci_excludes = r.mean_ci_case_low > 0
        rows.append({
            "hypothesis": "H2",
            "metric": r.proxy,
            "criterion": "Framework <= static in a clear majority (>= 2/3) of unseen cases, lower on average, "
                         "paired-bootstrap CI of the mean excludes 0",
            "evidence": f"{r.wins}/{r.ties}/{r.losses} win/tie/loss; mean reduction {r.mean_reduction:.1%} "
                        f"(case CI {r.mean_ci_case_low:.1%} to {r.mean_ci_case_high:.1%}; workload CI "
                        f"{r.mean_ci_workload_low:.1%} to {r.mean_ci_workload_high:.1%})",
            "verdict": "Supported" if majority and lower and ci_excludes else "Not supported",
        })
    return pd.DataFrame(rows)


# --- Entry point ----------------------------------------------------------------------

def run_all(write: bool = True) -> dict[str, pd.DataFrame]:
    """Compute every Phase 10 table; optionally write them to ``results/processed/``."""
    runs = load_runs()
    idx = block_resamples(EXPECTED_BLOCKS["A1"])
    decisions = load_decisions()
    tables = {
        "checks": data_checks(),
        "descriptive": describe_cells(runs),
        "a1_effects": a1_effects(runs, idx),
        "a2_effects": a2_effects(runs, idx),
        "a2_trend": a2_trend(runs, idx),
        "breakeven": breakeven_uncertainty(runs, idx),
        "h2_tests": h2_tests(decisions),
        "h2_by_block": h2_by_block(runs, decisions),
    }
    tables["a1_pairwise"] = a1_pairwise(runs, idx, tables["a1_effects"])
    tables["verdicts"] = hypothesis_verdicts(tables["a1_pairwise"], tables["a1_effects"], tables["a2_trend"],
                                             tables["a2_effects"], tables["h2_tests"])
    if write:
        for name, table in tables.items():
            table.to_csv(OUTPUTS[name], index=False)
    return tables


def main() -> None:
    tables = run_all()
    failed = tables["checks"][~tables["checks"]["passed"]]
    print(f"Data checks: {len(tables['checks']) - len(failed)}/{len(tables['checks'])} passed")
    for row in failed.itertuples():
        print(f"  FAILED: {row.check} ({row.detail})")
    with pd.option_context("display.max_colwidth", 200, "display.width", 250):
        print(tables["verdicts"][["hypothesis", "metric", "verdict", "evidence"]].to_string(index=False))
    print("\nWrote:", ", ".join(p.name for p in OUTPUTS.values()))


if __name__ == "__main__":
    main()
