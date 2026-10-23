"""Phase 9 -- experiment E6: measure the unseen validation workloads.

The decision framework (``src/decision_framework.py``) predicts costs for these
workloads from the study workloads only. This script measures what each option
*actually* costs on them, so the framework's choices can be checked:

* ``shuffle{p}`` for every setting in the A1 grid -- ``shuffle200`` is the static
  baseline, the others give the A1 search cost and the tuned run;
* ``compaction`` -- the compaction job (A2 ``Copt``);
* ``compacted`` -- the pipeline on the compacted files at 200 partitions (A2 run).

Every run reads Parquet from disk with a fresh ``spark.read`` (no caching), as a
scheduled production job would. Per block, workloads run in a shuffled order;
within a workload, the eight shuffle settings run in a shuffled order, followed
by compaction and then the compacted run. One untimed session warm-up of every
stage precedes the blocks, and each measured run follows one discarded warm-up.

Outputs: rows with ``experiment_action = VAL`` appended to
``results/raw/experiment_results.csv``, and
``results/processed/validation_cell_summary.csv``.

Usage::

    python src/workload_generator.py --validation   # once, to create the workloads
    python src/run_validation.py                    # 3 blocks
"""

from __future__ import annotations

import argparse
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
from src.compaction import COMPACTED_DATA_DIR, compact
from src.experiment_runner import summarize_cells
from src.instrumentation import append_results, preflight_resource_check, run_and_measure
from src.paths import RESULTS_CSV_PATH, RESULTS_PROCESSED_DIR, VALIDATION_INDEX_PATH, from_project_relative
from src.run_a1_experiments import FORMAL_PARTITIONS
from src.spark_config import SHUFFLE_PARTITIONS_DEFAULT, get_spark_session

EXPERIMENT_ACTION = "VAL"
DEFAULT_BLOCKS = 3
RUN_ORDER_SEED = 789
PER_RUN_WARMUPS = 1
CELL_SUMMARY_PATH = RESULTS_PROCESSED_DIR / "validation_cell_summary.csv"


def load_validation_workloads() -> dict[str, dict[str, Any]]:
    """Paths and factor values of every workload in the validation index."""
    if not VALIDATION_INDEX_PATH.exists():
        raise FileNotFoundError(f"{VALIDATION_INDEX_PATH} not found. "
                                "Run: python src/workload_generator.py --validation")
    index = pd.read_csv(VALIDATION_INDEX_PATH, dtype={"file_count_level": str, "shuffle_volume_level": str})
    workloads = {}
    for row in index.itertuples(index=False):
        workloads[row.workload_id] = {
            "orders_path": from_project_relative(row.orders_path),
            "customers_path": from_project_relative(row.customers_path),
            "compacted_path": COMPACTED_DATA_DIR / row.workload_id,
            "metadata": {
                "data_volume": row.data_volume,
                "file_count_level": row.file_count_level,
                "shuffle_volume_level": row.shuffle_volume_level,
                "num_orders": int(row.num_orders),
            },
        }
    return workloads


def stage_labels() -> list[str]:
    return [f"shuffle{p}" for p in FORMAL_PARTITIONS] + ["compaction", "compacted"]


def stage_partitions(label: str) -> int:
    return int(label.removeprefix("shuffle")) if label.startswith("shuffle") else SHUFFLE_PARTITIONS_DEFAULT


def stage_action(spark: SparkSession, workload: dict[str, Any], label: str) -> Callable[[], Any]:
    """Zero-argument callable for one stage; sets the partition count and reads Parquet afresh."""
    customers = str(workload["customers_path"])
    partitions = stage_partitions(label)

    def run() -> Any:
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        if label == "compaction":
            return compact(spark, workload["orders_path"], workload["compacted_path"])
        source = str(workload["compacted_path"] if label == "compacted" else workload["orders_path"])
        return execute_pipeline(spark, spark.read.parquet(source), spark.read.parquet(customers))

    return run


def block_sequence(rng: random.Random, workload_ids: list[str]) -> list[tuple[str, str]]:
    """Shuffled workloads; per workload, shuffled shuffle settings, then compaction, then compacted."""
    order = workload_ids.copy()
    rng.shuffle(order)
    sequence = []
    for wid in order:
        settings = [f"shuffle{p}" for p in FORMAL_PARTITIONS]
        rng.shuffle(settings)
        sequence += [(wid, label) for label in settings + ["compaction", "compacted"]]
    return sequence


def run_validation(spark: SparkSession, workloads: dict[str, dict[str, Any]], blocks: int) -> pd.DataFrame:
    rng = random.Random(RUN_ORDER_SEED)
    ids = list(workloads)
    per_block = len(ids) * len(stage_labels())
    print(f"\n{len(ids)} workloads x {len(stage_labels())} stages x {blocks} blocks = "
          f"{per_block * blocks} measured executions")

    print(f"\n=== Session warm-up ({per_block} untimed executions) ===")
    for wid, label in block_sequence(rng, ids):
        stage_action(spark, workloads[wid], label)()

    measured: list[dict[str, Any]] = []
    for block in range(1, blocks + 1):
        print(f"\n=== Repetition block {block}/{blocks} ===")
        for wid, label in block_sequence(rng, ids):
            workload = workloads[wid]
            run_metadata = {
                "pipeline_version": PIPELINE_VERSION,
                "workload_id": wid,
                "experiment_action": EXPERIMENT_ACTION,
                "config_label": label,
                "shuffle_partitions": stage_partitions(label),
                "adaptive_enabled": False,
                "repetition_block": block,
                **workload["metadata"],
            }
            rows = run_and_measure(spark, stage_action(spark, workload, label), run_metadata,
                                   warmup_runs=PER_RUN_WARMUPS, measured_runs=1)
            append_results(rows, path=RESULTS_CSV_PATH)
            measured.extend(r for r in rows if not r["is_warmup"])
    return pd.DataFrame(measured)


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure the unseen validation workloads (E6).")
    parser.add_argument("--blocks", type=int, default=DEFAULT_BLOCKS,
                        help=f"repetition blocks (default: {DEFAULT_BLOCKS})")
    parser.add_argument("--force", action="store_true",
                        help="ignore the RAM / AC-power pre-flight check (debugging only)")
    args = parser.parse_args()

    preflight_resource_check(force=args.force)
    workloads = load_validation_workloads()
    spark = get_spark_session("E6_validation", shuffle_partitions=SHUFFLE_PARTITIONS_DEFAULT,
                              enable_sparkmeasure=True)
    results = run_validation(spark, workloads, args.blocks)
    spark.stop()

    cells = summarize_cells(results, by=("workload_id", "config_label"))
    RESULTS_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cells.to_csv(CELL_SUMMARY_PATH, index=False)

    pd.set_option("display.width", 200)
    print("\n" + "=" * 72 + "\nE6 VALIDATION RUNS COMPLETE\n" + "=" * 72)
    print(cells.to_string(index=False))
    noisy = cells[cells["cv_ok"].eq(False)]
    if not noisy.empty:
        print(f"\nWARNING: {len(noisy)} cell(s) exceed the CV threshold -- see the cv column.")
    print(f"\nWrote {CELL_SUMMARY_PATH.name} to {RESULTS_PROCESSED_DIR}")


if __name__ == "__main__":
    main()
