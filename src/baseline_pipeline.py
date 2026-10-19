"""
baseline_pipeline.py  (PIPELINE VERSION v2)
=============================================
The single reference pipeline used across every A1 and A2 experiment.

Shape: Filter -> Join -> Aggregation -> Transformation -> (no-op sink)

VERSION HISTORY (results from different versions are NOT comparable):
  v1  groupBy(region, product_category)  -> only 20 groups. Combined with a
      broadcast join, the only shuffle was ~10 KB regardless of workload, so
      runtime reflected pure task-scheduling overhead. The v1 pilot (60 runs)
      is archived as pilot evidence; it is not used for conclusions.
  v2  groupBy(customer_id, region, product_category) -> ~5 x num_customers
      groups (high cardinality), so partial aggregation cannot collapse the
      shuffle. Used together with spark.sql.autoBroadcastJoinThreshold = -1
      (set in spark_config.py) so the join is a sort-merge join that really
      shuffles the orders table. Action is a no-op write (see execute_pipeline)
      so Spark cannot prune aggregate columns out of the plan.

Do not modify this pipeline's logic once formal experiments begin.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import sys
sys.path.insert(0, ".")

PIPELINE_VERSION = "v2"


def run_pipeline(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    orders_df:    order_id, customer_id, product_category, amount, order_date
    customers_df: customer_id, region, segment
    """
    # 1. FILTER -- cheap, no shuffle. Its selectivity is the "shuffle volume"
    #    workload knob: rows that survive flow into the shuffles below.
    filtered = orders_df.filter(F.col("amount") > 0)

    # 2. JOIN -- sort-merge join (broadcast disabled in spark_config.py):
    #    shuffles BOTH sides on customer_id (these are the 2 Exchanges).
    joined = filtered.join(customers_df, on="customer_id", how="inner")

    # 3. AGGREGATION -- high-cardinality key. Because the key includes
    #    customer_id (already hash-partitioned by the join), Spark reuses
    #    that partitioning and adds NO third exchange: the plan has exactly
    #    two Exchange operators (one per join side), and the shuffle volume
    #    is therefore (surviving orders + customers).
    aggregated = (
        joined.groupBy("customer_id", "region", "product_category")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("order_id").alias("order_count"),
            F.avg("amount").alias("avg_amount"),
        )
    )

    # 4. TRANSFORMATION -- cheap, no extra shuffle
    return aggregated.withColumn(
        "avg_amount_rounded", F.round(F.col("avg_amount"), 2)
    ).drop("avg_amount")


def execute_pipeline(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> None:
    """
    The action every timed run uses. Executes the FULL plan and discards the
    output via Spark's built-in 'noop' sink: nothing is written to disk (no
    file I/O noise, no winutils involvement), and -- unlike .count() -- Spark
    cannot prune unused aggregate columns, so the measured work is the whole
    pipeline.
    """
    run_pipeline(spark, orders_df, customers_df).write.format("noop").mode("overwrite").save()


def verify_shuffle_plan(result_df: DataFrame) -> bool:
    """
    Prints the physical plan check. Requires >= 2 'Exchange' operators.
    NOTE: this proves shuffles EXIST, not that they are LARGE -- use
    verify_workloads.py to confirm shuffle bytes actually scale with the
    workload (the check v1 lacked).
    """
    plan = result_df._jdf.queryExecution().toString()
    exchange_count = plan.count("Exchange")
    print(f"[verify_shuffle_plan] 'Exchange' operator found {exchange_count} time(s) in physical plan.")
    if exchange_count >= 2:
        print("[verify_shuffle_plan] PASS -- both sides of the sort-merge join are shuffling.")
        return True
    print("[verify_shuffle_plan] FAIL -- fewer shuffles than expected. Do not proceed to experiments.")
    print(plan)
    return False


def verify_correctness(orders_df: DataFrame, result_df: DataFrame):
    """Sanity checks run once, unmeasured, before any timed experiment."""
    input_positive_amount_count = orders_df.filter(F.col("amount") > 0).count()
    output_total_orders = result_df.agg(F.sum("order_count")).collect()[0][0]

    print(f"[verify_correctness] Input rows with amount > 0: {input_positive_amount_count}")
    print(f"[verify_correctness] Output order_count summed:   {output_total_orders}")

    assert output_total_orders <= input_positive_amount_count, \
        "BUG: output has more orders than filtered input -- join is fanning out incorrectly."
    assert output_total_orders > 0, \
        "BUG: output is empty -- join key mismatch or filter too aggressive."
    print("[verify_correctness] PASS -- output is within a sane range of input.")


if __name__ == "__main__":
    from src.spark_config import get_spark_session
    spark = get_spark_session("baseline_pipeline_smoke_test", shuffle_partitions=10, adaptive_enabled=False)

    orders = spark.createDataFrame(
        [(1, 101, "electronics", 250.0, "2026-01-01"),
         (2, 102, "grocery", 40.0, "2026-01-01"),
         (3, 101, "electronics", -10.0, "2026-01-02"),   # filtered out
         (4, 103, "electronics", 500.0, "2026-01-02"),
         (5, 102, "grocery", 15.0, "2026-01-03")],
        ["order_id", "customer_id", "product_category", "amount", "order_date"]
    )
    customers = spark.createDataFrame(
        [(101, "north", "retail"), (102, "south", "retail"), (103, "north", "enterprise")],
        ["customer_id", "region", "segment"]
    )

    result = run_pipeline(spark, orders, customers)
    result.show()
    verify_correctness(orders, result)
    shuffle_ok = verify_shuffle_plan(result)
    execute_pipeline(spark, orders, customers)   # proves the noop-sink action runs
    spark.stop()
    print("\n[baseline_pipeline] ALL CHECKS PASSED (v2)." if shuffle_ok else "\n[baseline_pipeline] CHECKS FAILED.")
