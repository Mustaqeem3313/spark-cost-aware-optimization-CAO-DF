"""Deterministic synthetic workload generator for the A1 and A2 experiments.

Schema (matches ``baseline_pipeline.py``):

* ``orders``:    ``order_id, customer_id, product_category, amount, order_date``
* ``customers``: ``customer_id, region, segment``

Design
------
1. **Reproducibility.** Data is drawn from a seeded NumPy ``Generator`` on the
   driver and converted to Spark, so a seed produces identical data on any
   machine. Spark's ``F.rand(seed)`` is deterministic only under fixed
   partitioning, which is a weaker guarantee.
2. **Data volume** is the order row count (small / medium / large).
3. **File count** is set by ``repartition(N)`` before the Parquet write; it is
   the A2 (compaction) factor.
4. **Shuffle volume** is set by filter selectivity: the fraction of orders with
   ``amount > 0`` that survive the pipeline's filter and enter the shuffle.

Each workload gets a ``manifest.json`` recording both the requested levels and
what was actually written (rows, files, bytes), so every factor level is
measured rather than assumed.

Validation workloads (Phase 9, experiment E6) are generated separately with new
seeds and factor values that the study workloads never use (500 000 orders, 5 or
25 files, 50 % shuffle fraction), so the decision framework is tested on data it
has not seen.

Usage::

    python src/workload_generator.py                # 10 study workloads + index, then an integration check
    python src/workload_generator.py --validation   # 4 unseen validation workloads + their index
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from src.paths import (
    DATA_GENERATED_DIR,
    VALIDATION_INDEX_PATH,
    WORKLOAD_INDEX_PATH,
    from_project_relative,
    to_project_relative,
)

DATA_SEED = 42

DATA_VOLUME_LEVELS = {"small": 50_000, "medium": 500_000, "large": 2_000_000}
FILE_COUNT_LEVELS = {"low": 1, "medium": 10, "high": 50}
SHUFFLE_VOLUME_LEVELS = {"low": 0.15, "high": 0.85}  # fraction of orders with amount > 0

MIN_CUSTOMERS = 500
ORDERS_PER_CUSTOMER = 100
CUSTOMER_FILE_COUNT = 2

# Unseen validation workloads: (workload_id, orders, order files, shuffle fraction, seed).
VALIDATION_WORKLOADS = [
    ("V_medium_1f_s85", 500_000, 1, 0.85, 701),
    ("V_medium_25f_s50", 500_000, 25, 0.50, 702),
    ("V_small_25f_s15", 50_000, 25, 0.15, 703),
    ("V_large_5f_s50", 2_000_000, 5, 0.50, 704),
]

REGIONS = ["north", "south", "east", "west"]
SEGMENTS = ["retail", "enterprise", "smb"]
PRODUCT_CATEGORIES = ["electronics", "grocery", "apparel", "home", "sports"]

INDEX_COLUMNS = [
    "workload_id", "data_volume", "file_count_level", "shuffle_volume_level", "seed",
    "num_orders", "actual_file_count", "actual_size_mb", "orders_path", "customers_path",
]


def _generate_customers(rng: np.random.Generator, num_customers: int) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": np.arange(1, num_customers + 1),
        "region": rng.choice(REGIONS, size=num_customers),
        "segment": rng.choice(SEGMENTS, size=num_customers),
    })


def _generate_orders(
    rng: np.random.Generator, num_orders: int, num_customers: int, shuffle_fraction: float
) -> pd.DataFrame:
    customer_ids = rng.integers(1, num_customers + 1, size=num_orders)
    categories = rng.choice(PRODUCT_CATEGORIES, size=num_orders)

    # Rows with a positive amount survive the pipeline's `amount > 0` filter.
    survives_filter = rng.random(num_orders) < shuffle_fraction
    positive_amounts = rng.uniform(5, 1000, size=num_orders).round(2)
    negative_amounts = -rng.uniform(1, 50, size=num_orders).round(2)
    amounts = np.where(survives_filter, positive_amounts, negative_amounts)

    start = np.datetime64("2026-01-01")
    offsets = rng.integers(0, 180, size=num_orders)
    order_dates = (start + offsets.astype("timedelta64[D]")).astype(str)

    return pd.DataFrame({
        "order_id": np.arange(1, num_orders + 1),
        "customer_id": customer_ids,
        "product_category": categories,
        "amount": amounts,
        "order_date": order_dates,
    })


def generate_workload(
    spark: SparkSession,
    workload_id: str,
    data_volume: str,
    file_count_level: str,
    shuffle_volume_level: str,
    seed: int = DATA_SEED,
    output_root: Path = DATA_GENERATED_DIR,
) -> dict[str, Any]:
    """Generate one study workload from named factor levels and return its manifest.

    Raises:
        ValueError: If a level name is not defined.
    """
    for name, value, levels in (("data_volume", data_volume, DATA_VOLUME_LEVELS),
                                ("file_count_level", file_count_level, FILE_COUNT_LEVELS),
                                ("shuffle_volume_level", shuffle_volume_level, SHUFFLE_VOLUME_LEVELS)):
        if value not in levels:
            raise ValueError(f"{name} must be one of {list(levels)}, got '{value}'")
    return write_workload(
        spark, workload_id,
        num_orders=DATA_VOLUME_LEVELS[data_volume],
        num_files=FILE_COUNT_LEVELS[file_count_level],
        shuffle_fraction=SHUFFLE_VOLUME_LEVELS[shuffle_volume_level],
        seed=seed,
        labels={"data_volume": data_volume, "file_count_level": file_count_level,
                "shuffle_volume_level": shuffle_volume_level},
        output_root=output_root,
    )


def write_workload(
    spark: SparkSession,
    workload_id: str,
    num_orders: int,
    num_files: int,
    shuffle_fraction: float,
    seed: int,
    labels: dict[str, str],
    output_root: Path = DATA_GENERATED_DIR,
) -> dict[str, Any]:
    """Generate one workload from explicit sizes, write it as Parquet, and return its manifest."""
    num_customers = max(MIN_CUSTOMERS, num_orders // ORDERS_PER_CUSTOMER)

    rng = np.random.default_rng(seed)
    customers_pdf = _generate_customers(rng, num_customers)
    orders_pdf = _generate_orders(rng, num_orders, num_customers, shuffle_fraction)

    workload_dir = Path(output_root) / workload_id
    orders_path = workload_dir / "orders"
    customers_path = workload_dir / "customers"

    # Arrow conversion needs extra JVM flags on Java 17; generation is untimed,
    # so the portable non-Arrow path is used.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    orders_df = spark.createDataFrame(orders_pdf)
    customers_df = spark.createDataFrame(customers_pdf)

    orders_df.repartition(num_files).write.mode("overwrite").parquet(str(orders_path))
    customers_df.repartition(CUSTOMER_FILE_COUNT).write.mode("overwrite").parquet(str(customers_path))

    order_files = list(orders_path.glob("*.parquet"))
    size_bytes = sum(f.stat().st_size for f in order_files)

    manifest = {
        "workload_id": workload_id,
        "requested": dict(labels),
        "seed": seed,
        "measured": {
            "num_orders_generated": int(num_orders),
            "num_customers_generated": int(num_customers),
            "actual_order_file_count": len(order_files),
            "actual_order_size_bytes": int(size_bytes),
            "actual_order_size_mb": round(size_bytes / (1024 * 1024), 2),
            "requested_shuffle_fraction": shuffle_fraction,
            "actual_positive_amount_fraction": round(float((orders_pdf["amount"] > 0).mean()), 4),
        },
        "paths": {
            "orders": to_project_relative(orders_path),
            "customers": to_project_relative(customers_path),
        },
    }
    (workload_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    measured = manifest["measured"]
    print(f"[workload_generator] {workload_id}: {measured['num_orders_generated']} orders, "
          f"{measured['actual_order_file_count']} files, {measured['actual_order_size_mb']} MB, "
          f"{measured['actual_positive_amount_fraction']:.2%} positive-amount rows")
    return manifest


def _write_index(manifests: list[dict[str, Any]], path: Path) -> None:
    index = pd.DataFrame([{
        "workload_id": m["workload_id"],
        "data_volume": m["requested"]["data_volume"],
        "file_count_level": m["requested"]["file_count_level"],
        "shuffle_volume_level": m["requested"]["shuffle_volume_level"],
        "seed": m["seed"],
        "num_orders": m["measured"]["num_orders_generated"],
        "actual_file_count": m["measured"]["actual_order_file_count"],
        "actual_size_mb": m["measured"]["actual_order_size_mb"],
        "orders_path": m["paths"]["orders"],
        "customers_path": m["paths"]["customers"],
    } for m in manifests], columns=INDEX_COLUMNS)
    path.parent.mkdir(parents=True, exist_ok=True)
    index.to_csv(path, index=False)
    print(f"[workload_generator] Wrote index of {len(index)} workloads to {to_project_relative(path)}")


def generate_all_workloads(spark: SparkSession, seed: int = DATA_SEED) -> list[dict[str, Any]]:
    """Generate the 10 study workloads and write the workload index.

    * A1 (shuffle tuning): {small, large} x {low, high} shuffle volume, file count fixed at medium.
    * A2 (compaction): {small, large} x {low, medium, high} file count, shuffle volume fixed at high.
    """
    specs = [(f"A1_{vol}_{shuf}", vol, "medium", shuf)
             for vol in ("small", "large") for shuf in ("low", "high")]
    specs += [(f"A2_{vol}_{files}", vol, files, "high")
              for vol in ("small", "large") for files in ("low", "medium", "high")]

    manifests = [generate_workload(spark, wid, vol, files, shuf, seed=seed)
                 for wid, vol, files, shuf in specs]
    _write_index(manifests, WORKLOAD_INDEX_PATH)
    return manifests


def generate_validation_workloads(spark: SparkSession) -> list[dict[str, Any]]:
    """Generate the unseen validation workloads (new seeds and factor values) and their index."""
    manifests = []
    for wid, orders, files, fraction, seed in VALIDATION_WORKLOADS:
        labels = {"data_volume": str(orders), "file_count_level": str(files),
                  "shuffle_volume_level": f"{fraction:.2f}"}
        manifests.append(write_workload(spark, wid, orders, files, fraction, seed, labels))
    _write_index(manifests, VALIDATION_INDEX_PATH)
    return manifests


def main() -> None:
    """Generate the workloads, then check the pipeline consumes the output correctly."""
    from src.baseline_pipeline import run_pipeline, verify_correctness, verify_shuffle_plan
    from src.spark_config import get_spark_session

    parser = argparse.ArgumentParser(description="Generate the synthetic workloads.")
    parser.add_argument("--validation", action="store_true",
                        help="generate the unseen validation workloads instead of the study workloads")
    args = parser.parse_args()

    spark = get_spark_session("workload_generation", shuffle_partitions=50)
    manifests = generate_validation_workloads(spark) if args.validation else generate_all_workloads(spark)

    print("\n[workload_generator] Integration check against the baseline pipeline:")
    sample = manifests[0]
    orders_df = spark.read.parquet(str(from_project_relative(sample["paths"]["orders"])))
    customers_df = spark.read.parquet(str(from_project_relative(sample["paths"]["customers"])))
    result = run_pipeline(spark, orders_df, customers_df)
    verify_correctness(orders_df, result)
    shuffle_ok = verify_shuffle_plan(result)
    spark.stop()

    if not shuffle_ok:
        print("[workload_generator] Integration check FAILED.")
        sys.exit(1)
    print(f"[workload_generator] All checks PASSED -- {len(manifests)} workloads ready.")


if __name__ == "__main__":
    main()
