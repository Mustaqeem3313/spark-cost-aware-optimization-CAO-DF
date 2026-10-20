"""
baseline_pipeline.py
=====================
The single reference pipeline used across every A1 and A2 experiment.

Shape: Filter -> Join -> Aggregation -> Transformation -> Output
Chosen deliberately so that BOTH a Join and a GroupBy force genuine shuffle
(Exchange) stages -- verified by verify_shuffle_plan(), not assumed.

Do not modify this pipeline's logic once experiments begin (Phase 6/7).
If it needs to change, that is a new pipeline version and old results are
not comparable to new ones -- document any such change explicitly.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import sys
sys.path.insert(0, ".")
from src.spark_config_v1 import get_spark_session


def run_pipeline(spark: SparkSession, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    orders_df:    order_id, customer_id, product_category, amount, order_date
    customers_df: customer_id, region, segment

    Filter -> Join -> GroupBy Aggregation -> Transformation
    """
    # 1. FILTER -- cheap, no shuffle
    filtered = orders_df.filter(F.col("amount") > 0)

    # 2. JOIN -- forces a shuffle (Exchange) on customer_id, real cardinality
    #    on both sides, not a trivial self-join.
    joined = filtered.join(customers_df, on="customer_id", how="inner")

    # 3. AGGREGATION -- forces a second shuffle (Exchange) on a meaningful-
    #    cardinality grouping key (region x product_category, not a
    #    near-constant column Spark could trivially optimize around).
    aggregated = (
        joined.groupBy("region", "product_category")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("order_id").alias("order_count"),
            F.avg("amount").alias("avg_amount"),
        )
    )

    # 4. TRANSFORMATION -- cheap, no additional shuffle
    result = aggregated.withColumn(
        "avg_amount_rounded", F.round(F.col("avg_amount"), 2)
    ).drop("avg_amount")

    return result


def verify_shuffle_plan(result_df: DataFrame) -> bool:
    """
    Prints the physical plan and checks that 'Exchange' (Spark's shuffle
    operator) actually appears. This MUST be run and MUST pass before any
    experiment is trusted -- a pipeline that silently doesn't shuffle
    invalidates the entire A1 experiment (there would be nothing for
    shuffle-partition tuning to act on).
    """
    plan = result_df._jdf.queryExecution().toString()
    exchange_count = plan.count("Exchange")
    print(f"[verify_shuffle_plan] 'Exchange' operator found {exchange_count} time(s) in physical plan.")
    if exchange_count >= 2:
        print("[verify_shuffle_plan] PASS -- both Join and GroupBy are shuffling as intended.")
        return True
    else:
        print("[verify_shuffle_plan] FAIL -- fewer shuffles than expected. Do not proceed to experiments.")
        print(plan)
        return False


def verify_correctness(orders_df: DataFrame, result_df: DataFrame):
    """
    Sanity checks run ONCE, unmeasured, before any timed experiment.
    Catches silent bugs (dropped rows from a bad join, wrong filter) that
    would otherwise poison every downstream measurement.
    """
    input_positive_amount_count = orders_df.filter(F.col("amount") > 0).count()
    output_total_orders = result_df.agg(F.sum("order_count")).collect()[0][0]

    print(f"[verify_correctness] Input rows with amount > 0: {input_positive_amount_count}")
    print(f"[verify_correctness] Output order_count summed:   {output_total_orders}")

    # These won't be exactly equal (inner join drops unmatched customer_ids
    # by design) but output should never exceed input, and should be in a
    # sane range -- catches gross join/filter bugs, not a full unit test.
    assert output_total_orders <= input_positive_amount_count, \
        "BUG: output has more orders than filtered input -- join is fanning out incorrectly."
    assert output_total_orders > 0, \
        "BUG: output is empty -- join key mismatch or filter too aggressive."
    print("[verify_correctness] PASS -- output is within a sane range of input.")


if __name__ == "__main__":
    # Minimal smoke test with tiny synthetic data -- NOT a real experiment,
    # just proves the pipeline logic + verification functions work end to end
    # before Phase 4's real workload generator is wired in.
    spark = get_spark_session("baseline_pipeline_smoke_test", shuffle_partitions=10, adaptive_enabled=False)

    orders = spark.createDataFrame(
        [(1, 101, "electronics", 250.0, "2026-01-01"),
         (2, 102, "grocery", 40.0, "2026-01-01"),
         (3, 101, "electronics", -10.0, "2026-01-02"),   # should be filtered out
         (4, 103, "electronics", 500.0, "2026-01-02"),
         (5, 102, "grocery", 15.0, "2026-01-03")],
        ["order_id", "customer_id", "product_category", "amount", "order_date"]
    )
    customers = spark.createDataFrame(
        [(101, "north", "retail"),
         (102, "south", "retail"),
         (103, "north", "enterprise")],
        ["customer_id", "region", "segment"]
    )

    result = run_pipeline(spark, orders, customers)
    result.show()

    verify_correctness(orders, result)
    shuffle_ok = verify_shuffle_plan(result)

    spark.stop()

    if shuffle_ok:
        print("\n[baseline_pipeline] ALL CHECKS PASSED. Pipeline is ready for Phase 4 (real workload generator).")
    else:
        print("\n[baseline_pipeline] CHECKS FAILED. Fix the pipeline before proceeding.")
