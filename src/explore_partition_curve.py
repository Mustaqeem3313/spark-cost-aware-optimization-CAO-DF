"""Exploratory partition sweep used to choose the formal A1 grid (not part of the formal dataset).

Runs the two most contrasting workloads across a wide range of shuffle-partition
settings to locate where the runtime curve bends on the study machine. The
grid includes 12, the logical core count of the study machine, because
``local[*]`` runs one task per core.

Output: ``results/raw/pilot_a1_v2_sweep.csv`` (``experiment_action = "A1_sweep"``).

Usage::

    python src/explore_partition_curve.py
    python src/explore_partition_curve.py --force    # debugging only
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
from src.paths import RESULTS_RAW_DIR
from src.spark_config import SHUFFLE_PARTITIONS_DEFAULT, get_spark_session

SWEEP_WORKLOADS = ["A1_small_high", "A1_large_high"]
SWEEP_PARTITIONS = [1, 2, 4, 8, 12, 16, 50, 200, 400]
SWEEP_BLOCKS = 3
RUN_ORDER_SEED = 7
SWEEP_CSV_PATH = RESULTS_RAW_DIR / "pilot_a1_v2_sweep.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Exploratory A1 partition sweep.")
    parser.add_argument("--force", action="store_true",
                        help="ignore the RAM / AC-power pre-flight check (debugging only)")
    args = parser.parse_args()

    preflight_resource_check(force=args.force)
    spark = get_spark_session("A1_sweep", shuffle_partitions=SHUFFLE_PARTITIONS_DEFAULT, enable_sparkmeasure=True)
    print("Loading and caching sweep workloads (untimed)...")
    workloads = load_and_cache_workloads(spark, SWEEP_WORKLOADS)

    results = run_randomized_blocks(spark, workloads, SWEEP_PARTITIONS, SWEEP_BLOCKS, RUN_ORDER_SEED,
                                    experiment_action="A1_sweep", results_path=SWEEP_CSV_PATH)
    spark.stop()

    pd.set_option("display.width", 200)
    print("\n" + "=" * 72 + "\nSWEEP RESULT\n" + "=" * 72)
    print(summarize_cells(results).to_string(index=False))
    print(f"\nRaw rows: {SWEEP_CSV_PATH}")


if __name__ == "__main__":
    main()
