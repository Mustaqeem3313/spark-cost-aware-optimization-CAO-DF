"""
explore_partition_curve.py   (pilot v2 -- choose the formal partition grid)
=============================================================================
A short exploratory sweep on the two most contrasting workloads, run BEFORE
the formal A1 matrix, to see where the runtime-vs-partitions curve bends on
this machine. Results go to results/raw/pilot_a1_v2_sweep.csv (experiment_action
'A1_sweep') and are NOT part of the formal dataset -- they only inform which
partition values the formal matrix tests.

Usage:
    python src/explore_partition_curve.py
    python src/explore_partition_curve.py --force      # skip RAM/AC guard (debug only)
"""

import sys
import random
import argparse

sys.path.insert(0, ".")
import pandas as pd
from src.spark_config import get_spark_session
from src.baseline_pipeline import execute_pipeline, PIPELINE_VERSION
from src.instrumentation import (preflight_resource_check, load_and_cache_workloads,
                                  run_and_measure, append_results)

SWEEP_WORKLOADS = ["A1_small_high", "A1_large_high"]
SWEEP_PARTITIONS = [1, 2, 4, 8, 16, 50, 200, 400]
BLOCKS = 3
SWEEP_CSV = "results/raw/pilot_a1_v2_sweep.csv"


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    preflight_resource_check(force=args.force)
    random.seed(7)

    spark = get_spark_session("A1_sweep", shuffle_partitions=200, adaptive_enabled=False, enable_sparkmeasure=True)
    print("Loading and caching sweep workloads (untimed)...")
    wl = load_and_cache_workloads(spark, SWEEP_WORKLOADS)
    cells = [(w, p) for w in SWEEP_WORKLOADS for p in SWEEP_PARTITIONS]

    warm = cells.copy(); random.shuffle(warm)
    print(f"\nSession warm-up ({len(warm)} untimed executions)...")
    for w, p in warm:
        spark.conf.set("spark.sql.shuffle.partitions", p)
        execute_pipeline(spark, wl[w][0], wl[w][1])

    rows = []
    for block in range(1, BLOCKS + 1):
        order = cells.copy(); random.shuffle(order)
        print(f"\n=== Sweep block {block}/{BLOCKS} ===")
        for w, p in order:
            o, c, meta = wl[w]
            spark.conf.set("spark.sql.shuffle.partitions", p)
            md = {"pipeline_version": PIPELINE_VERSION, "workload_id": w, "experiment_action": "A1_sweep",
                  "config_label": f"shuffle{p}", "shuffle_partitions": p, "adaptive_enabled": False,
                  "repetition_block": block, **meta}
            res = run_and_measure(spark, lambda o=o, c=c: execute_pipeline(spark, o, c), md,
                                   warmup_runs=1, measured_runs=1, use_sparkmeasure=True)
            append_results(res, path=SWEEP_CSV)
            rows.extend(r for r in res if not r["is_warmup"])
    spark.stop()

    df = pd.DataFrame(rows)
    g = df.groupby(["workload_id", "shuffle_partitions"]).agg(
        median_wall_s=("wall_clock_seconds", "median"),
        min_wall_s=("wall_clock_seconds", "min"),
        max_wall_s=("wall_clock_seconds", "max"),
        median_exec_run_s=("executor_run_time_ms", lambda x: x.median() / 1000),
        median_spill_mb=("disk_bytes_spilled", lambda x: x.median() / 1e6),
        median_shuffle_mb=("shuffle_bytes_written", lambda x: x.median() / 1e6),
    ).round(2)
    pd.set_option("display.width", 200)
    print("\n" + "=" * 72 + "\nSWEEP RESULT (paste this back)\n" + "=" * 72)
    print(g.to_string())
    print(f"\nRaw rows: {SWEEP_CSV}")


if __name__ == "__main__":
    main()
