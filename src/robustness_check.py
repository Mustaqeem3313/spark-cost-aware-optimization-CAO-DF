"""Outlier-robustness check for the formal A1 results (no Spark needed).

The protocol keeps every measured run: noisy cells are disclosed, never dropped.
This script asks whether the conclusions *would* change if they were dropped. For
every cell whose CV exceeds the threshold it removes the single slowest run,
recomputes the cell statistics and compares, per workload:

* the best partition setting (wall-clock median) and its speed-up over 200;
* the full ranking of partition settings (Spearman rank correlation).

Writes ``results/processed/a1_outlier_robustness.csv`` and prints a verdict.

Usage::

    python src/robustness_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.experiment_runner import summarize_cells
from src.paths import RESULTS_CSV_PATH, RESULTS_PROCESSED_DIR
from src.run_a1_experiments import best_config_per_workload

CV_THRESHOLD = 0.20
OUTPUT_PATH = RESULTS_PROCESSED_DIR / "a1_outlier_robustness.csv"


def load_measured_a1(path: Path = RESULTS_CSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path)
    measured = ~df["is_warmup"].astype(str).str.lower().isin(["true", "1"])
    return df[(df["experiment_action"] == "A1") & measured].copy()


def drop_slowest_in_noisy_cells(runs: pd.DataFrame, cell_summary: pd.DataFrame) -> pd.DataFrame:
    """Remove the slowest run from each cell whose CV exceeds the threshold."""
    noisy = cell_summary.loc[cell_summary["cv"] > CV_THRESHOLD, ["workload_id", "shuffle_partitions"]]
    drop_index = []
    for wid, partitions in noisy.itertuples(index=False):
        cell = runs[(runs["workload_id"] == wid) & (runs["shuffle_partitions"] == partitions)]
        drop_index.append(cell["wall_clock_seconds"].idxmax())
    return runs.drop(index=drop_index)


def compare(full: pd.DataFrame, trimmed: pd.DataFrame) -> pd.DataFrame:
    best_full = best_config_per_workload(full).set_index("workload_id")
    best_trim = best_config_per_workload(trimmed).set_index("workload_id")
    rows = []
    for wid in best_full.index:
        ranks = pd.DataFrame({
            "full": full[full["workload_id"] == wid].set_index("shuffle_partitions")["median_wall_s"],
            "trimmed": trimmed[trimmed["workload_id"] == wid].set_index("shuffle_partitions")["median_wall_s"],
        })
        rows.append({
            "workload_id": wid,
            "noisy_cells": int((full[full["workload_id"] == wid]["cv"] > CV_THRESHOLD).sum()),
            "best_partitions_full": int(best_full.at[wid, "best_partitions_by_wall_clock"]),
            "best_partitions_trimmed": int(best_trim.at[wid, "best_partitions_by_wall_clock"]),
            "speedup_full": best_full.at[wid, "speedup_vs_default200"],
            "speedup_trimmed": best_trim.at[wid, "speedup_vs_default200"],
            "spearman_rank_corr": round(ranks["full"].rank().corr(ranks["trimmed"].rank()), 4),
            "max_cv_full": round(full[full["workload_id"] == wid]["cv"].max(), 3),
            "max_cv_trimmed": round(trimmed[trimmed["workload_id"] == wid]["cv"].max(), 3),
        })
    return pd.DataFrame(rows)


def main() -> None:
    runs = load_measured_a1()
    full = summarize_cells(runs)
    trimmed = summarize_cells(drop_slowest_in_noisy_cells(runs, full))
    report = compare(full, trimmed)

    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    report.to_csv(OUTPUT_PATH, index=False)

    pd.set_option("display.width", 200)
    print(report.to_string(index=False))
    stable = (report["best_partitions_full"] == report["best_partitions_trimmed"]).all()
    print(f"\nNoisy cells (CV > {CV_THRESHOLD}): {int(report['noisy_cells'].sum())} of {len(full)}")
    print("Verdict: best settings " + ("UNCHANGED" if stable else "CHANGE")
          + f"; minimum rank correlation {report['spearman_rank_corr'].min():.3f}")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
