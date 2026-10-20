"""Reference pipeline used by every A1 and A2 experiment (pipeline version v2).

Shape: filter -> sort-merge join -> high-cardinality aggregation -> projection,
executed into Spark's ``noop`` sink.

Version history (results from different versions are not comparable):

* **v1** -- ``groupBy(region, product_category)`` (20 groups) with a broadcast
  join. The only shuffle was ~10 KB for every workload, so runtime reflected
  task-scheduling overhead only. Archived as pilot evidence.
* **v2** -- ``groupBy(customer_id, region, product_category)`` with broadcast
  joins disabled. Both join sides are shuffled and partial aggregation cannot
  collapse the data, so shuffle bytes scale with the workload (verified by
  ``verify_workloads.py``).

The pipeline logic is frozen for formal experiments. Any change requires a new
``PIPELINE_VERSION``.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PIPELINE_VERSION = "v2"
MIN_EXPECTED_EXCHANGES = 2


def run_pipeline(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Build the (lazy) pipeline plan.

    Args:
        spark: Active session (kept for a uniform signature across pipeline versions).
        orders_df: ``order_id, customer_id, product_category, amount, order_date``.
        customers_df: ``customer_id, region, segment``.

    Returns:
        Aggregated DataFrame with one row per (customer, region, category).
    """
    # Filter selectivity is the "shuffle volume" workload factor.
    filtered = orders_df.filter(F.col("amount") > 0)

    # Sort-merge join: both sides are hash-partitioned on customer_id (2 Exchanges).
    joined = filtered.join(customers_df, on="customer_id", how="inner")

    # The key contains customer_id, so Spark reuses the join's partitioning and
    # adds no third Exchange.
    aggregated = joined.groupBy("customer_id", "region", "product_category").agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("avg_amount"),
    )

    return aggregated.withColumn("avg_amount_rounded", F.round(F.col("avg_amount"), 2)).drop("avg_amount")


def execute_pipeline(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> None:
    """Execute the full pipeline into the ``noop`` sink (the action every timed run uses).

    The ``noop`` sink runs the complete physical plan without writing any output,
    and -- unlike ``count()`` -- prevents Spark from pruning aggregate columns.
    """
    run_pipeline(spark, orders_df, customers_df).write.format("noop").mode("overwrite").save()


def verify_shuffle_plan(result_df: DataFrame) -> bool:
    """Check that the physical plan contains at least two ``Exchange`` operators.

    This proves shuffles exist, not that they are large; ``verify_workloads.py``
    checks that shuffle bytes actually scale with the workload factors.
    """
    plan = result_df._jdf.queryExecution().toString()
    exchange_count = plan.count("Exchange")
    passed = exchange_count >= MIN_EXPECTED_EXCHANGES
    print(f"[baseline_pipeline] Exchange operators in plan: {exchange_count} "
          f"({'PASS' if passed else 'FAIL'}, need >= {MIN_EXPECTED_EXCHANGES})")
    if not passed:
        print(plan)
    return passed


def verify_correctness(orders_df: DataFrame, result_df: DataFrame) -> None:
    """Sanity-check pipeline output against its input (run once, untimed).

    Raises:
        AssertionError: If the join fans out or the output is empty.
    """
    filtered_count = orders_df.filter(F.col("amount") > 0).count()
    output_order_count = result_df.agg(F.sum("order_count")).collect()[0][0] or 0

    print(f"[baseline_pipeline] Filtered input rows: {filtered_count}; "
          f"summed output order_count: {output_order_count}")
    assert output_order_count <= filtered_count, "Join fans out: more output orders than filtered input."
    assert output_order_count > 0, "Output is empty: join key mismatch or filter too aggressive."
    print("[baseline_pipeline] Correctness check PASSED.")


def main() -> None:
    """Self-test on a five-row dataset."""
    from src.spark_config import get_spark_session

    spark = get_spark_session("baseline_pipeline_self_test", shuffle_partitions=10)
    orders = spark.createDataFrame(
        [(1, 101, "electronics", 250.0, "2026-01-01"),
         (2, 102, "grocery", 40.0, "2026-01-01"),
         (3, 101, "electronics", -10.0, "2026-01-02"),
         (4, 103, "electronics", 500.0, "2026-01-02"),
         (5, 102, "grocery", 15.0, "2026-01-03")],
        ["order_id", "customer_id", "product_category", "amount", "order_date"],
    )
    customers = spark.createDataFrame(
        [(101, "north", "retail"), (102, "south", "retail"), (103, "north", "enterprise")],
        ["customer_id", "region", "segment"],
    )

    result = run_pipeline(spark, orders, customers)
    result.show()
    verify_correctness(orders, result)
    shuffle_ok = verify_shuffle_plan(result)
    execute_pipeline(spark, orders, customers)
    spark.stop()

    print(f"[baseline_pipeline] Self-test {'PASSED' if shuffle_ok else 'FAILED'} ({PIPELINE_VERSION}).")
    if not shuffle_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
