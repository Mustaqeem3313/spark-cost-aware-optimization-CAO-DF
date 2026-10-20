"""Phase 6 -- formal A1 experiment matrix (shuffle-partition tuning).

Runs ``4 workloads x len(partitions) settings x blocks`` measured executions
under the randomized-block protocol in :mod:`src.experiment_runner`, and writes:

* ``results/raw/experiment_results.csv`` -- one row per measured run (append-only);
* ``results/processed/a1_cell_summary.csv`` -- per-cell median / IQR / CV;
* ``results/processed/a1_best_config_per_workload.csv`` -- best setting vs the static baseline.

``spark.sql.shuffle.partitions = 200`` (Spark's default) is the static baseline
and is always included in the grid.

Usage::

    python src/run_a1_experiments.py                          # formal grid, 5 blocks
    python src/run_a1_experiments.py --partitions 4 16 200    # custom grid
    python src/run_a1_experiments.py --blocks 3 --force       # debugging only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.experiment_runner import run_randomized_blocks, summarize_cells
from src.instrumentation import load_and_cache_workloads, preflight_resource_check
from src.paths import RESULTS_CSV_PATH, RESULTS_PROCESSED_DIR
from src.spark_config import SHUFFLE_PARTITIONS_DEFAULT, get_spark_session

A1_WORKLOADS = ["A1_small_low", "A1_small_high", "A1_large_low", "A1_large_high"]
FORMAL_PARTITIONS = [1, 2, 4, 8, 16, 50, 200, 400]
DEFAULT_BLOCKS = 5
RUN_ORDER_SEED = 123

CELL_SUMMARY_PATH = RESULTS_PROCESSED_DIR / "a1_cell_summary.csv"
BEST_CONFIG_PATH = RESULTS_PROCESSED_DIR / "a1_best_config_per_workload.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the formal A1 shuffle-partition matrix.")
    parser.add_argument("--partitions", type=int, nargs="+", default=FORMAL_PARTITIONS,
                        help=f"shuffle-partition settings to test (default: {FORMAL_PARTITIONS})")
    parser.add_argument("--blocks", type=int, default=DEFAULT_BLOCKS,
                        help=f"repetition blocks (default: {DEFAULT_BLOCKS})")
    parser.add_argument("--force", action="store_true",
                        help="ignore the RAM / AC-power pre-flight check (debugging only)")
    return parser.parse_args()


def best_config_per_workload(cell_summary: pd.DataFrame) -> pd.DataFrame:
    """Best partition setting per workload by wall-clock and executor time, vs the 200 baseline."""
    rows = []
    for wid, group in cell_summary.groupby("workload_id", sort=False):
        by_wall = group.loc[group["median_wall_s"].idxmin()]
        by_exec = (group.loc[group["median_executor_run_s"].idxmin()]
                   if group["median_executor_run_s"].notna().any() else by_wall)
        baseline = group[group["shuffle_partitions"] == SHUFFLE_PARTITIONS_DEFAULT]["median_wall_s"].iloc[0]
        rows.append({
            "workload_id": wid,
            "best_partitions_by_wall_clock": int(by_wall["shuffle_partitions"]),
            "best_median_wall_s": by_wall["median_wall_s"],
            "best_partitions_by_executor_time": int(by_exec["shuffle_partitions"]),
            "default200_median_wall_s": baseline,
            "speedup_vs_default200": round(baseline / by_wall["median_wall_s"], 3),
        })
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    grid = sorted(set(args.partitions) | {SHUFFLE_PARTITIONS_DEFAULT})

    preflight_resource_check(force=args.force)
    spark = get_spark_session("A1_experiment_matrix", shuffle_partitions=SHUFFLE_PARTITIONS_DEFAULT,
                              enable_sparkmeasure=True)
    print("Loading and caching A1 workloads (untimed)...")
    workloads = load_and_cache_workloads(spark, A1_WORKLOADS)

    results = run_randomized_blocks(spark, workloads, grid, args.blocks, RUN_ORDER_SEED,
                                    experiment_action="A1", results_path=RESULTS_CSV_PATH)
    spark.stop()

    cell_summary = summarize_cells(results)
    best = best_config_per_workload(cell_summary)
    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cell_summary.to_csv(CELL_SUMMARY_PATH, index=False)
    best.to_csv(BEST_CONFIG_PATH, index=False)

    pd.set_option("display.width", 200)
    print("\n" + "=" * 72 + "\nA1 MATRIX COMPLETE\n" + "=" * 72)
    print(cell_summary.to_string(index=False))
    print("\n" + best.to_string(index=False))
    noisy = cell_summary[cell_summary["cv_ok"].eq(False)]
    if not noisy.empty:
        print(f"\nWARNING: {len(noisy)} cell(s) exceed the CV threshold -- see the cv column.")
    print(f"\nWrote {CELL_SUMMARY_PATH.name} and {BEST_CONFIG_PATH.name} to {RESULTS_PROCESSED_DIR}")


if __name__ == "__main__":
    main()
