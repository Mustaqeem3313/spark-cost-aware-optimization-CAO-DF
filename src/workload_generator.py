"""
workload_generator.py
======================
Deterministic synthetic workload generator for A1/A2 experiments.

Schema matches baseline_pipeline.py exactly -- nothing in the pipeline
needs to change to consume this generator's output.
    orders:    order_id, customer_id, product_category, amount, order_date
    customers: customer_id, region, segment

DESIGN DECISIONS (documented, not hidden -- these belong in your paper's
Methodology section almost verbatim):

1. TRUE reproducibility via numpy, not Spark's F.rand(seed=...).
   Data is generated with a numpy-seeded RNG on the driver, then converted
   to a Spark DataFrame via pandas + Arrow. Spark's own F.rand(seed=...) is
   only deterministic under fixed partition count / parallelism, which is a
   weaker guarantee than this study needs. Numpy's seeded RNG gives
   identical bytes for a given seed on any machine, independent of Spark
   internals.

2. Data volume = total row count (Small / Medium / Large).

3. File count / fragmentation is controlled by .repartition(N) before
   write -- this is what determines how many physical Parquet files land
   on disk, which is exactly what A2 (compaction) experiments vary.

4. Shuffle volume is controlled via FILTER SELECTIVITY: the fraction of
   orders with amount > 0 (the baseline pipeline's own filter condition).
   "High shuffle volume" = most rows survive the filter and flow into the
   Join+GroupBy shuffle stages. "Low shuffle volume" = most rows are
   filtered out before any shuffle happens. This ties the workload
   variable directly to pipeline behavior instead of an arbitrary knob.

Every generated workload gets a manifest.json recording BOTH the requested
parameters AND the actually-measured output (row count, file count, size
on disk) -- so "did fragmentation actually vary the way I intended?" is
answered by measurement, never assumed.
"""

import os
import json
import glob
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# ----------------------------------------------------------------------
# Fixed level definitions -- change these constants, not the logic below,
# if you need different sizes. Document any change with a date, the same
# way you would for environment.md.
# ----------------------------------------------------------------------
DATA_VOLUME_LEVELS = {
    "small": 50_000,
    "medium": 500_000,
    "large": 2_000_000,
}

FILE_COUNT_LEVELS = {   # number of output Parquet files for the orders table
    "low": 1,
    "medium": 10,
    "high": 50,
}

SHUFFLE_VOLUME_LEVELS = {   # fraction of orders with amount > 0
    "low": 0.15,
    "high": 0.85,
}

REGIONS = ["north", "south", "east", "west"]
SEGMENTS = ["retail", "enterprise", "smb"]
PRODUCT_CATEGORIES = ["electronics", "grocery", "apparel", "home", "sports"]

OUTPUT_ROOT = "data/generated"
INDEX_PATH = "experiments/configs/workload_index.csv"


def _generate_customers(rng: np.random.Generator, num_customers: int) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": np.arange(1, num_customers + 1),
        "region": rng.choice(REGIONS, size=num_customers),
        "segment": rng.choice(SEGMENTS, size=num_customers),
    })


def _generate_orders(rng: np.random.Generator, num_orders: int, num_customers: int,
                      shuffle_fraction: float) -> pd.DataFrame:
    customer_ids = rng.integers(1, num_customers + 1, size=num_orders)
    categories = rng.choice(PRODUCT_CATEGORIES, size=num_orders)

    # shuffle_fraction of rows get a positive amount (survive the pipeline's
    # `amount > 0` filter and flow into the shuffle stages); the rest get a
    # non-positive amount (filtered out before any shuffle).
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


def generate_workload(spark: SparkSession, workload_id: str, data_volume: str,
                       file_count_level: str, shuffle_volume_level: str,
                       seed: int = 42, output_root: str = OUTPUT_ROOT) -> dict:
    """
    Generates one workload, writes it to disk, and returns a manifest dict
    (also written to manifest.json alongside the data).
    """
    if data_volume not in DATA_VOLUME_LEVELS:
        raise ValueError(f"data_volume must be one of {list(DATA_VOLUME_LEVELS)}")
    if file_count_level not in FILE_COUNT_LEVELS:
        raise ValueError(f"file_count_level must be one of {list(FILE_COUNT_LEVELS)}")
    if shuffle_volume_level not in SHUFFLE_VOLUME_LEVELS:
        raise ValueError(f"shuffle_volume_level must be one of {list(SHUFFLE_VOLUME_LEVELS)}")

    num_orders = DATA_VOLUME_LEVELS[data_volume]
    num_files = FILE_COUNT_LEVELS[file_count_level]
    shuffle_fraction = SHUFFLE_VOLUME_LEVELS[shuffle_volume_level]
    num_customers = max(500, num_orders // 100)

    rng = np.random.default_rng(seed)  # numpy Generator -- true, portable determinism

    customers_pdf = _generate_customers(rng, num_customers)
    orders_pdf = _generate_orders(rng, num_orders, num_customers, shuffle_fraction)

    workload_dir = os.path.join(output_root, workload_id)
    orders_path = os.path.join(workload_dir, "orders")
    customers_path = os.path.join(workload_dir, "customers")

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    # Arrow-accelerated pandas->Spark conversion requires JVM module-access
    # flags on Java 9+ that aren't set by default (and would need extra
    # spark.driver.extraJavaOptions wiring to enable reliably across
    # platforms). Since workload generation is never part of a timed
    # experiment, correctness/portability matters far more here than speed
    # -- so Arrow stays off and Spark's standard conversion path is used.

    orders_df = spark.createDataFrame(orders_pdf)
    customers_df = spark.createDataFrame(customers_pdf)

    # File-count control lives here -- repartition BEFORE write determines
    # how many physical Parquet files land on disk.
    orders_df.repartition(num_files).write.mode("overwrite").parquet(orders_path)
    # Customers is a small reference/dimension table -- fragmentation is not
    # a variable of interest for it, so its file count stays fixed and small.
    customers_df.repartition(2).write.mode("overwrite").parquet(customers_path)

    # Measure what actually landed on disk -- never assume the requested
    # level produced what you expected.
    actual_order_files = [f for f in glob.glob(os.path.join(orders_path, "*.parquet"))]
    actual_size_bytes = sum(os.path.getsize(f) for f in actual_order_files)

    manifest = {
        "workload_id": workload_id,
        "requested": {
            "data_volume": data_volume,
            "file_count_level": file_count_level,
            "shuffle_volume_level": shuffle_volume_level,
        },
        "seed": seed,
        "measured": {
            "num_orders_generated": int(num_orders),
            "num_customers_generated": int(num_customers),
            "actual_order_file_count": len(actual_order_files),
            "actual_order_size_bytes": int(actual_size_bytes),
            "actual_order_size_mb": round(actual_size_bytes / (1024 * 1024), 2),
            "requested_shuffle_fraction": shuffle_fraction,
            "actual_positive_amount_fraction": round(float((orders_pdf["amount"] > 0).mean()), 4),
        },
        "paths": {
            "orders": orders_path,
            "customers": customers_path,
        },
    }

    with open(os.path.join(workload_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[workload_generator] Generated '{workload_id}': "
          f"{manifest['measured']['num_orders_generated']} orders, "
          f"{manifest['measured']['actual_order_file_count']} files "
          f"(requested level: {file_count_level}), "
          f"{manifest['measured']['actual_order_size_mb']} MB, "
          f"{manifest['measured']['actual_positive_amount_fraction']:.2%} positive-amount rows "
          f"(requested shuffle level: {shuffle_volume_level})")

    return manifest


def generate_all_workloads(spark: SparkSession, seed: int = 42) -> list:
    """
    Generates exactly the workloads your compressed experiment matrix
    (Phase 6 / Phase 7) needs -- no more, no less:

    A1 (shuffle/partition tuning) needs: 2 data volumes x 2 shuffle volumes
      -> 4 workloads, file_count held at "medium" (irrelevant to A1)

    A2 (compaction) needs: 2 data volumes x 3 file-count/fragmentation levels
      -> 6 workloads, shuffle_volume held at "high" (so compaction's benefit
         on subsequent reads is actually observable)

    Total: 10 pre-generated workloads. Writes an index CSV so the Phase 6/7
    experiment runner can look up which dataset to load for each config row
    without regenerating anything.
    """
    manifests = []

    # --- A1 workloads: vary data_volume x shuffle_volume ---
    for data_volume in ["small", "large"]:
        for shuffle_volume in ["low", "high"]:
            wid = f"A1_{data_volume}_{shuffle_volume}"
            manifests.append(generate_workload(
                spark, wid, data_volume=data_volume,
                file_count_level="medium", shuffle_volume_level=shuffle_volume, seed=seed
            ))

    # --- A2 workloads: vary data_volume x file_count/fragmentation ---
    for data_volume in ["small", "large"]:
        for file_count in ["low", "medium", "high"]:
            wid = f"A2_{data_volume}_{file_count}"
            manifests.append(generate_workload(
                spark, wid, data_volume=data_volume,
                file_count_level=file_count, shuffle_volume_level="high", seed=seed
            ))

    os.makedirs(os.path.dirname(INDEX_PATH), exist_ok=True)
    index_rows = []
    for m in manifests:
        index_rows.append({
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
        })
    pd.DataFrame(index_rows).to_csv(INDEX_PATH, index=False)
    print(f"\n[workload_generator] Wrote index of {len(index_rows)} workloads to {INDEX_PATH}")

    return manifests


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from src.spark_config_v1 import get_spark_session

    spark = get_spark_session("phase4_workload_generation", shuffle_partitions=50, adaptive_enabled=False)

    manifests = generate_all_workloads(spark)

    print("\n" + "=" * 60)
    print("INTEGRATION CHECK: does baseline_pipeline.py consume this output correctly?")
    print("=" * 60)
    from src.baseline_pipeline_v1 import run_pipeline, verify_correctness, verify_shuffle_plan

    sample = manifests[0]
    orders_df = spark.read.parquet(sample["paths"]["orders"])
    customers_df = spark.read.parquet(sample["paths"]["customers"])
    result = run_pipeline(spark, orders_df, customers_df)
    verify_correctness(orders_df, result)
    shuffle_ok = verify_shuffle_plan(result)

    spark.stop()

    if shuffle_ok:
        print(f"\n[workload_generator] ALL CHECKS PASSED. Phase 4 complete -- "
              f"10 workloads generated and verified compatible with the pipeline.")
    else:
        print("\n[workload_generator] INTEGRATION CHECK FAILED.")
