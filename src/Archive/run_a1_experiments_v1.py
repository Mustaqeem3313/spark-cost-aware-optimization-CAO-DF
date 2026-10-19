"""
run_a1_experiments.py   (Phase 6 -- v2 protocol)
==================================================
Runs the formal A1 (shuffle/partition tuning) matrix unattended:

    4 workloads x N partition configs x 5 repetition blocks
    (N = len(--partitions); default 5 -> 100 measured runs)

Usage:
    python src/run_a1_experiments.py                       # default grid
    python src/run_a1_experiments.py --partitions 4 8 16 50 200 400
    python src/run_a1_experiments.py --blocks 3            # fewer repetitions
    python src/run_a1_experiments.py --force               # skip RAM/AC guard (debug only)

Protocol (Methodology section):
  1. Pre-flight: refuses to run if free RAM is low or on battery.
  2. ONE shared SparkSession; shuffle partitions changed with spark.conf.set().
  3. Each workload cached once, untimed, so runs measure compute/shuffle cost
     rather than Parquet I/O.
  4. SESSION WARM-UP: every (workload, config) cell is executed once, untimed,
     before block 1. The v1 pilot showed block 1 inflated by cold-start effects
     (e.g. 4.9 s vs ~1.2 s for the same cell later) that a per-run warm-up did
     not remove.
  5. Randomized-block design: each repetition block shuffles the order of all
     cells afresh; each measured run is preceded by its own discarded warm-up.
  6. spark.sql.shuffle.partitions = 200 (Spark's default, the static baseline)
     is always part of the grid.
  7. Every row records pipeline_version, repetition_block and sparkMeasure
     metrics; results are append-only.
"""

import os
import sys
import random
import argparse

sys.path.insert(0, ".")
import pandas as pd
from src.spark_config import get_spark_session
from src.baseline_pipeline import execute_pipeline, PIPELINE_VERSION
from src.instrumentation import (preflight_resource_check, load_and_cache_workloads,
                                  run_and_measure, append_results, check_measurement_quality)

A1_WORKLOADS = ["A1_small_low", "A1_small_high", "A1_large_low", "A1_large_high"]
DEFAULT_PARTITIONS = [4, 16, 50, 200, 400]
RUN_ORDER_SEED = 123        # controls run ORDER only (data seed 42 is separate)
PER_RUN_WARMUPS = 1
CELL_SUMMARY_PATH = "results/processed/a1_cell_summary.csv"
BEST_CONFIG_PATH = "results/processed/a1_best_config_per_workload.csv"


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partitions", type=int, nargs="+", default=DEFAULT_PARTITIONS)
    ap.add_argument("--blocks", type=int, default=5)
    ap.add_argument("--force", action="store_true")
    return ap.parse_args()


def main():
    args = parse_args()
    grid = sorted(set(args.partitions) | {200})
    if 200 not in args.partitions:
        print("[a1] Added 200 (Spark's default) to the grid -- it is the static baseline.")

    preflight_resource_check(force=args.force)
    random.seed(RUN_ORDER_SEED)

    spark = get_spark_session("A1_experiment_matrix", shuffle_partitions=200,
                               adaptive_enabled=False, enable_sparkmeasure=True)

    print("Loading and caching A1 workloads (untimed)...")
    workloads = load_and_cache_workloads(spark, A1_WORKLOADS)

    cells = [(wid, p) for wid in A1_WORKLOADS for p in grid]
    print(f"\nGrid: {grid}  ->  {len(cells)} cells x {args.blocks} blocks = {len(cells) * args.blocks} measured runs")

    # ---- session warm-up: every cell once, untimed, randomized order ----
    warm = cells.copy(); random.shuffle(warm)
    print(f"\n=== Session warm-up ({len(warm)} untimed executions) ===")
    for wid, p in warm:
        o, c, _ = workloads[wid]
        spark.conf.set("spark.sql.shuffle.partitions", p)
        execute_pipeline(spark, o, c)
    print("    done.")

    all_rows = []
    for block in range(1, args.blocks + 1):
        order = cells.copy(); random.shuffle(order)
        print(f"\n=== Repetition block {block}/{args.blocks} ===")
        for wid, p in order:
            o, c, meta = workloads[wid]
            spark.conf.set("spark.sql.shuffle.partitions", p)
            run_metadata = {
                "pipeline_version": PIPELINE_VERSION,
                "workload_id": wid,
                "experiment_action": "A1",
                "config_label": f"shuffle{p}",
                "shuffle_partitions": p,
                "adaptive_enabled": False,
                "repetition_block": block,
                **meta,
            }
            results = run_and_measure(spark, lambda o=o, c=c: execute_pipeline(spark, o, c),
                                       run_metadata, warmup_runs=PER_RUN_WARMUPS,
                                       measured_runs=1, use_sparkmeasure=True)
            append_results(results)
            all_rows.extend(r for r in results if not r["is_warmup"])

    spark.stop()

    # ---- summaries ----
    df = pd.DataFrame(all_rows)
    cell_rows, best_rows = [], []
    print("\n" + "=" * 72 + "\nA1 MATRIX COMPLETE\n" + "=" * 72)
    for wid in A1_WORKLOADS:
        print(f"\n{wid}")
        sub = df[df.workload_id == wid]
        for p in grid:
            cell = sub[sub.shuffle_partitions == p]
            t = cell.wall_clock_seconds
            q = check_measurement_quality([{"wall_clock_seconds": x, "is_warmup": False} for x in t])
            rt = cell.executor_run_time_ms.median() / 1000 if cell.executor_run_time_ms.notna().any() else None
            cell_rows.append({"workload_id": wid, "shuffle_partitions": p, "n": len(cell),
                              "median_wall_s": q["median"], "iqr_wall_s": q["iqr"], "cv": q["cv"],
                              "median_executor_run_s": rt,
                              "median_shuffle_mb": cell.shuffle_bytes_written.median() / 1e6})
            print(f"  partitions={p:>4}  median={q['median']:>7}s  IQR={q['iqr']:>6}  CV={q['cv']}"
                  f"{'  [NOISY]' if q['cv_ok'] is False else ''}")
    cs = pd.DataFrame(cell_rows)
    for wid, g in cs.groupby("workload_id"):
        w = g.loc[g.median_wall_s.idxmin()]
        r = g.loc[g.median_executor_run_s.idxmin()] if g.median_executor_run_s.notna().any() else w
        best_rows.append({"workload_id": wid,
                          "best_partitions_by_wall_clock": int(w.shuffle_partitions),
                          "best_median_wall_s": w.median_wall_s,
                          "best_partitions_by_executor_time": int(r.shuffle_partitions),
                          "default200_median_wall_s": g[g.shuffle_partitions == 200].median_wall_s.iloc[0]})
        print(f"\n  best for {wid}: {int(w.shuffle_partitions)} partitions by wall-clock, "
              f"{int(r.shuffle_partitions)} by executor time")
    os.makedirs("results/processed", exist_ok=True)
    cs.to_csv(CELL_SUMMARY_PATH, index=False)
    pd.DataFrame(best_rows).to_csv(BEST_CONFIG_PATH, index=False)
    print(f"\nWrote {CELL_SUMMARY_PATH} and {BEST_CONFIG_PATH}")


if __name__ == "__main__":
    main()