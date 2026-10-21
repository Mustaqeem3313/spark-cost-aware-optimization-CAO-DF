"""Randomized complete-block executor shared by the A1 sweep and the formal A1 matrix,
plus the per-cell summary used by both A1 and A2.

Protocol (Methodology section):

1. Every ``(workload, shuffle_partitions)`` cell is executed once, untimed, in
   random order (session warm-up). The v1 pilot showed the first block inflated
   by cold-start effects that a per-run warm-up alone did not remove.
2. Each repetition block runs every cell once in a freshly shuffled order.
3. Each measured run is preceded by one discarded warm-up run.
4. Measured rows are appended to the results CSV as soon as they are produced,
   so an interrupted run loses at most one cell.
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession

from src.baseline_pipeline import PIPELINE_VERSION, execute_pipeline
from src.instrumentation import WorkloadData, append_results, check_measurement_quality, run_and_measure

PER_RUN_WARMUPS = 1

Cell = tuple[str, int]


def _set_partitions(spark: SparkSession, partitions: int) -> None:
    spark.conf.set("spark.sql.shuffle.partitions", partitions)


def run_randomized_blocks(
    spark: SparkSession,
    workloads: dict[str, WorkloadData],
    partitions_grid: list[int],
    blocks: int,
    run_order_seed: int,
    experiment_action: str,
    results_path: Path,
) -> pd.DataFrame:
    """Run every (workload, partitions) cell ``blocks`` times in randomized order.

    Args:
        spark: Active session built with ``enable_sparkmeasure=True``.
        workloads: Cached workloads from ``load_and_cache_workloads``.
        partitions_grid: ``spark.sql.shuffle.partitions`` values to test.
        blocks: Number of repetition blocks.
        run_order_seed: Seed controlling run order only (independent of the data seed).
        experiment_action: Value written to the ``experiment_action`` column.
        results_path: CSV that measured rows are appended to.

    Returns:
        DataFrame of the measured (non-warm-up) rows produced by this call.
    """
    rng = random.Random(run_order_seed)
    cells: list[Cell] = [(wid, p) for wid in workloads for p in partitions_grid]
    print(f"\nGrid: {partitions_grid} -> {len(cells)} cells x {blocks} blocks = {len(cells) * blocks} measured runs")

    warmup_order = cells.copy()
    rng.shuffle(warmup_order)
    print(f"\n=== Session warm-up ({len(warmup_order)} untimed executions) ===")
    for wid, partitions in warmup_order:
        orders_df, customers_df, _ = workloads[wid]
        _set_partitions(spark, partitions)
        execute_pipeline(spark, orders_df, customers_df)

    measured_rows: list[dict[str, Any]] = []
    for block in range(1, blocks + 1):
        order = cells.copy()
        rng.shuffle(order)
        print(f"\n=== Repetition block {block}/{blocks} ===")
        for wid, partitions in order:
            orders_df, customers_df, metadata = workloads[wid]
            _set_partitions(spark, partitions)
            run_metadata = {
                "pipeline_version": PIPELINE_VERSION,
                "workload_id": wid,
                "experiment_action": experiment_action,
                "config_label": f"shuffle{partitions}",
                "shuffle_partitions": partitions,
                "adaptive_enabled": False,
                "repetition_block": block,
                **metadata,
            }
            rows = run_and_measure(
                spark,
                lambda o=orders_df, c=customers_df: execute_pipeline(spark, o, c),
                run_metadata,
                warmup_runs=PER_RUN_WARMUPS,
                measured_runs=1,
            )
            append_results(rows, path=results_path)
            measured_rows.extend(r for r in rows if not r["is_warmup"])

    return pd.DataFrame(measured_rows)


def summarize_cells(results: pd.DataFrame, by: tuple[str, str] = ("workload_id", "shuffle_partitions")) -> pd.DataFrame:
    """Per-cell medians, IQR and CV of wall-clock time plus median Spark metrics.

    Args:
        results: Measured rows.
        by: The two columns that identify a cell -- ``(workload_id, shuffle_partitions)``
            for A1, ``(workload_id, config_label)`` for A2.
    """
    summary_rows = []
    for (wid, level), cell in results.groupby(list(by), sort=True):
        quality = check_measurement_quality(
            [{"wall_clock_seconds": t, "is_warmup": False} for t in cell["wall_clock_seconds"]],
            verbose=False,
        )
        summary_rows.append({
            by[0]: wid,
            by[1]: int(level) if by[1] == "shuffle_partitions" else level,
            "n": len(cell),
            "median_wall_s": quality["median"],
            "iqr_wall_s": quality["iqr"],
            "cv": quality["cv"],
            "cv_ok": quality["cv_ok"],
            "median_executor_run_s": _median_scaled(cell["executor_run_time_ms"], 1e3),
            "median_executor_cpu_s": _median_scaled(cell["executor_cpu_time_ms"], 1e3),
            "median_shuffle_mb": _median_scaled(cell["shuffle_bytes_written"], 1e6),
            "median_spill_mb": _median_scaled(cell["disk_bytes_spilled"], 1e6),
        })
    return pd.DataFrame(summary_rows)


def _median_scaled(series: pd.Series, divisor: float) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return round(float(values.median()) / divisor, 4) if not values.empty else None
