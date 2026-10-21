"""Phase 7 -- A2 experiment matrix (file compaction).

For each ``A2_{small,large}_{low,medium,high}`` workload (1 / 10 / 50 order
files, shuffle volume fixed *high*), every repetition block measures three
stages in a fixed sequence:

1. ``fragmented`` -- the pipeline on the original fragmented files (**before**);
2. ``compaction`` -- the compaction job itself, rewriting the fragmented files
   to a new path (**``Copt``**);
3. ``compacted``  -- the pipeline on the compacted files (**after**).

Unlike A1, inputs are **not cached**: A2's effect lives on the read side
(listing and opening files, one scan task per split), so every run reads
Parquet from disk via a fresh ``spark.read``. ``spark.sql.shuffle.partitions``
is held at Spark's default (200) -- the static baseline -- so compaction is
evaluated on its own, not jointly with A1 (design decision D3).

Protocol: one untimed session warm-up of all three stages per workload; then,
in every block, workloads in a freshly shuffled order, each stage preceded by
one discarded warm-up run. Measured rows are appended as they are produced.

Outputs (formal run):

* ``results/raw/experiment_results.csv`` -- rows with ``experiment_action = A2``;
* ``results/processed/a2_cell_summary.csv`` -- per (workload, stage) median / IQR / CV;
* ``results/processed/a2_compaction_summary.csv`` -- per-workload benefit, ``Copt``
  and break-even executions ``N* = Copt / (before - after)``.

``--pilot`` (experiment E4) writes to ``pilot_a2*`` files instead and evaluates
the pre-registered pilot gate.

Usage::

    python src/run_a2_experiments.py --pilot          # E4 pilot, 3 blocks
    python src/run_a2_experiments.py                  # E5 formal, 5 blocks
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import SparkSession

from src.baseline_pipeline import PIPELINE_VERSION, execute_pipeline
from src.compaction import COMPACTED_DATA_DIR, compact, parquet_files
from src.experiment_runner import summarize_cells
from src.instrumentation import append_results, preflight_resource_check, run_and_measure
from src.paths import (
    RESULTS_CSV_PATH,
    RESULTS_PROCESSED_DIR,
    RESULTS_RAW_DIR,
    WORKLOAD_INDEX_PATH,
    from_project_relative,
)
from src.spark_config import SHUFFLE_PARTITIONS_DEFAULT, get_spark_session

A2_WORKLOADS = ["A2_small_low", "A2_small_medium", "A2_small_high",
                "A2_large_low", "A2_large_medium", "A2_large_high"]
STAGES = ("fragmented", "compaction", "compacted")
DEFAULT_BLOCKS = 5
PILOT_BLOCKS = 3
RUN_ORDER_SEED = 456
PER_RUN_WARMUPS = 1
PILOT_GATE_MIN_BENEFIT = 0.15


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the A2 file-compaction matrix.")
    parser.add_argument("--pilot", action="store_true",
                        help="E4 pilot: write pilot_a2* outputs and evaluate the pilot gate")
    parser.add_argument("--workloads", nargs="+", default=A2_WORKLOADS,
                        help="workload ids to run (default: all six A2 workloads)")
    parser.add_argument("--blocks", type=int, default=None,
                        help=f"repetition blocks (default: {DEFAULT_BLOCKS}; {PILOT_BLOCKS} with --pilot)")
    parser.add_argument("--force", action="store_true",
                        help="ignore the RAM / AC-power pre-flight check (debugging only)")
    return parser.parse_args()


def load_workload_paths(workload_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Orders/customers paths and factor levels of each workload, from the workload index."""
    index = pd.read_csv(WORKLOAD_INDEX_PATH).set_index("workload_id")
    missing = [w for w in workload_ids if w not in index.index]
    if missing:
        raise ValueError(f"Not in {WORKLOAD_INDEX_PATH}: {missing}. Run src/workload_generator.py.")
    workloads = {}
    for wid in workload_ids:
        row = index.loc[wid]
        workloads[wid] = {
            "orders_path": from_project_relative(row["orders_path"]),
            "customers_path": from_project_relative(row["customers_path"]),
            "compacted_path": COMPACTED_DATA_DIR / wid,
            "metadata": {
                "data_volume": row["data_volume"],
                "file_count_level": row["file_count_level"],
                "shuffle_volume_level": row["shuffle_volume_level"],
                "num_orders": int(row["num_orders"]),
            },
        }
    return workloads


def stage_action(spark: SparkSession, workload: dict[str, Any], stage: str) -> Callable[[], Any]:
    """Zero-argument callable executing one stage; reads Parquet afresh on every call."""
    customers = str(workload["customers_path"])
    if stage == "fragmented":
        source = str(workload["orders_path"])
        return lambda: execute_pipeline(spark, spark.read.parquet(source), spark.read.parquet(customers))
    if stage == "compaction":
        return lambda: compact(spark, workload["orders_path"], workload["compacted_path"])
    if stage == "compacted":
        target = str(workload["compacted_path"])
        return lambda: execute_pipeline(spark, spark.read.parquet(target), spark.read.parquet(customers))
    raise ValueError(f"Unknown stage '{stage}'")


def run_a2_blocks(
    spark: SparkSession,
    workloads: dict[str, dict[str, Any]],
    blocks: int,
    experiment_action: str,
    results_path: Path,
) -> pd.DataFrame:
    """Session warm-up, then ``blocks`` randomized blocks of fragmented -> compaction -> compacted."""
    rng = random.Random(RUN_ORDER_SEED)
    ids = list(workloads)
    print(f"\n{len(ids)} workloads x {len(STAGES)} stages x {blocks} blocks = "
          f"{len(ids) * len(STAGES) * blocks} measured executions")

    warmup_order = ids.copy()
    rng.shuffle(warmup_order)
    print(f"\n=== Session warm-up ({len(warmup_order) * len(STAGES)} untimed executions) ===")
    for wid in warmup_order:
        for stage in STAGES:
            stage_action(spark, workloads[wid], stage)()

    measured: list[dict[str, Any]] = []
    for block in range(1, blocks + 1):
        order = ids.copy()
        rng.shuffle(order)
        print(f"\n=== Repetition block {block}/{blocks} ===")
        for wid in order:
            workload = workloads[wid]
            for stage in STAGES:
                run_metadata = {
                    "pipeline_version": PIPELINE_VERSION,
                    "workload_id": wid,
                    "experiment_action": experiment_action,
                    "config_label": stage,
                    "shuffle_partitions": SHUFFLE_PARTITIONS_DEFAULT,
                    "adaptive_enabled": False,
                    "repetition_block": block,
                    **workload["metadata"],
                }
                rows = run_and_measure(spark, stage_action(spark, workload, stage), run_metadata,
                                       warmup_runs=PER_RUN_WARMUPS, measured_runs=1)
                input_path = workload["compacted_path"] if stage == "compacted" else workload["orders_path"]
                note = f"input_files={len(parquet_files(input_path))}"
                if stage == "compaction":
                    note += f";output_files={len(parquet_files(workload['compacted_path']))}"
                for row in rows:
                    row["notes"] = note
                append_results(rows, path=results_path)
                measured.extend(r for r in rows if not r["is_warmup"])
    return pd.DataFrame(measured)


def compaction_summary(results: pd.DataFrame) -> pd.DataFrame:
    """Per-workload compaction benefit, ``Copt`` and break-even executions.

    Benefit is computed per block (paired: before and after measured back to back)
    and summarised by its median, so block-level drift cancels out.
    """
    rows = []
    for wid, group in results.groupby("workload_id", sort=False):
        wide = group.pivot(index="repetition_number", columns="config_label", values="wall_clock_seconds")
        exec_wide = group.pivot(index="repetition_number", columns="config_label",
                                values="executor_run_time_ms") / 1e3
        benefit = wide["fragmented"] - wide["compacted"]
        benefit_pct = benefit / wide["fragmented"]
        median_benefit = float(benefit.median())
        median_copt = float(wide["compaction"].median())
        exec_benefit = float((exec_wide["fragmented"] - exec_wide["compacted"]).median())
        exec_copt = float(exec_wide["compaction"].median())
        notes = group[group["config_label"] == "fragmented"]["notes"].iloc[0]
        rows.append({
            "workload_id": wid,
            "data_volume": group["data_volume"].iloc[0],
            "file_count_level": group["file_count_level"].iloc[0],
            "input_files": int(notes.split("=")[1]),
            "n_blocks": len(wide),
            "median_before_s": round(float(wide["fragmented"].median()), 4),
            "median_after_s": round(float(wide["compacted"].median()), 4),
            "median_copt_s": round(median_copt, 4),
            "median_benefit_s": round(median_benefit, 4),
            "median_benefit_pct": round(float(benefit_pct.median()), 4),
            "blocks_after_faster": int((benefit > 0).sum()),
            "break_even_runs_wall": _break_even(median_copt, median_benefit),
            "median_exec_benefit_s": round(exec_benefit, 4),
            "median_exec_copt_s": round(exec_copt, 4),
            "break_even_runs_exec": _break_even(exec_copt, exec_benefit),
        })
    return pd.DataFrame(rows)


def _break_even(copt: float, benefit: float) -> float:
    """``N* = Copt / benefit`` rounded up; infinite when compaction gives no benefit."""
    return float(math.ceil(copt / benefit)) if benefit > 0 else math.inf


def evaluate_pilot_gate(summary: pd.DataFrame) -> bool:
    """Pre-registered gate (master document §13.4), A2 half.

    Passes if, for every data volume, the highest-fragmentation workload shows a
    compaction benefit > 15 % in the same direction in every block.
    """
    high = summary[summary["file_count_level"] == "high"]
    if high.empty:
        print("[pilot gate] No high-fragmentation workload in this run -- gate not evaluated.")
        return False
    passed = True
    for _, row in high.iterrows():
        ok = row["median_benefit_pct"] > PILOT_GATE_MIN_BENEFIT and row["blocks_after_faster"] == row["n_blocks"]
        passed &= ok
        print(f"[pilot gate] {row['workload_id']}: benefit {row['median_benefit_pct']:+.1%}, "
              f"after faster in {row['blocks_after_faster']}/{row['n_blocks']} blocks -> {'PASS' if ok else 'FAIL'}")
    print(f"[pilot gate] A2 half: {'PASSED' if passed else 'FAILED'}")
    return passed


def main() -> None:
    args = parse_args()
    blocks = args.blocks or (PILOT_BLOCKS if args.pilot else DEFAULT_BLOCKS)
    if args.pilot:
        action, results_path, prefix = "A2_pilot", RESULTS_RAW_DIR / "pilot_a2.csv", "pilot_a2"
    else:
        action, results_path, prefix = "A2", RESULTS_CSV_PATH, "a2"

    preflight_resource_check(force=args.force)
    workloads = load_workload_paths(args.workloads)
    spark = get_spark_session("A2_experiment_matrix", shuffle_partitions=SHUFFLE_PARTITIONS_DEFAULT,
                              enable_sparkmeasure=True)
    results = run_a2_blocks(spark, workloads, blocks, action, results_path)
    spark.stop()

    cells = summarize_cells(results, by=("workload_id", "config_label"))
    summary = compaction_summary(results)
    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cells.to_csv(RESULTS_PROCESSED_DIR / f"{prefix}_cell_summary.csv", index=False)
    summary.to_csv(RESULTS_PROCESSED_DIR / f"{prefix}_compaction_summary.csv", index=False)

    pd.set_option("display.width", 220)
    print("\n" + "=" * 72 + f"\nA2 {'PILOT' if args.pilot else 'MATRIX'} COMPLETE\n" + "=" * 72)
    print(cells.to_string(index=False))
    print("\n" + summary.to_string(index=False))
    noisy = cells[cells["cv_ok"].eq(False)]
    if not noisy.empty:
        print(f"\nWARNING: {len(noisy)} cell(s) exceed the CV threshold -- see the cv column.")
    if args.pilot:
        evaluate_pilot_gate(summary)
    print(f"\nWrote {prefix}_cell_summary.csv and {prefix}_compaction_summary.csv to {RESULTS_PROCESSED_DIR}")


if __name__ == "__main__":
    main()
