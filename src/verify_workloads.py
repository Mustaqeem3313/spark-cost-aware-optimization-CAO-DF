"""Shuffle-scaling gate: proves the workload factors really change shuffle cost.

``verify_shuffle_plan`` only proves Exchange operators exist. The v1 pilot had
Exchanges that moved ~10 KB for every workload, so its factors were inert. This
gate runs the pipeline once per A1 workload, reads shuffle bytes from Spark's
REST API (no sparkMeasure dependency), and requires:

* large vs small data volume  -> shuffle bytes >= 5x (at each shuffle level);
* high vs low shuffle level   -> shuffle bytes >= 2x (at each data volume);
* largest workload            -> >= 1 MB shuffled.

Usage::

    python src/verify_workloads.py           # exit code 0 = PASSED, 1 = FAILED
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from src.baseline_pipeline import execute_pipeline
from src.instrumentation import load_workload, preflight_resource_check
from src.spark_config import get_spark_session

WORKLOADS = ["A1_small_low", "A1_small_high", "A1_large_low", "A1_large_high"]
CHECK_PARTITIONS = 50
MIN_VOLUME_RATIO = 5.0
MIN_SHUFFLE_LEVEL_RATIO = 2.0
MIN_LARGEST_SHUFFLE_BYTES = 1_000_000
SETTLE_TIMEOUT_S = 15.0

StageTotals = tuple[int, int, int]  # (shuffle write bytes, shuffle read bytes, completed stages)


def _stage_totals(spark: SparkSession) -> StageTotals:
    """Cumulative shuffle bytes over all completed stages, from the Spark REST API."""
    sc = spark.sparkContext
    port = sc.uiWebUrl.rsplit(":", 1)[1].strip("/")
    url = f"http://localhost:{port}/api/v1/applications/{sc.applicationId}/stages?status=complete"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # bypass system proxies
    with opener.open(url, timeout=5) as response:
        stages = json.load(response)
    return (sum(s.get("shuffleWriteBytes", 0) for s in stages),
            sum(s.get("shuffleReadBytes", 0) for s in stages),
            len(stages))


def _settled_totals(spark: SparkSession, min_stages: int) -> StageTotals:
    """Poll until more than ``min_stages`` stages are complete and the totals stop changing.

    The Spark UI's status store updates asynchronously after a job finishes.
    """
    last = None
    deadline = time.time() + SETTLE_TIMEOUT_S
    while time.time() < deadline:
        current = _stage_totals(spark)
        if current[2] > min_stages and current == last:
            return current
        last = current
        time.sleep(0.5)
    print("  WARNING: stage metrics did not settle within the timeout; using the latest values.")
    return last


def measure_shuffle_bytes(spark: SparkSession) -> dict[str, int]:
    """Execute the pipeline once per workload and return shuffle bytes written."""
    measured = {}
    for wid in WORKLOADS:
        orders_df, customers_df, meta = load_workload(spark, wid)
        orders_df, customers_df = orders_df.cache(), customers_df.cache()
        orders_df.count()
        customers_df.count()

        before = _settled_totals(spark, min_stages=-1)
        execute_pipeline(spark, orders_df, customers_df)
        after = _settled_totals(spark, min_stages=before[2])

        written, read = after[0] - before[0], after[1] - before[1]
        measured[wid] = written
        print(f"  {wid:15s} orders={meta['num_orders']:>8}  "
              f"shuffle written={written / 1e6:9.2f} MB  read={read / 1e6:9.2f} MB")
    return measured


def evaluate_gate(measured: dict[str, int]) -> bool:
    """Print each gate check and return ``True`` only if all pass."""
    checks = []
    for level in ("low", "high"):
        ratio = measured[f"A1_large_{level}"] / max(measured[f"A1_small_{level}"], 1)
        checks.append((f"data volume moves shuffle bytes ({level} shuffle level): "
                       f"large/small = {ratio:.1f}x (need >= {MIN_VOLUME_RATIO:.0f}x)",
                       ratio >= MIN_VOLUME_RATIO))
    for volume in ("small", "large"):
        ratio = measured[f"A1_{volume}_high"] / max(measured[f"A1_{volume}_low"], 1)
        checks.append((f"shuffle level moves shuffle bytes ({volume} data): "
                       f"high/low = {ratio:.1f}x (need >= {MIN_SHUFFLE_LEVEL_RATIO:.0f}x)",
                       ratio >= MIN_SHUFFLE_LEVEL_RATIO))
    largest = measured["A1_large_high"]
    checks.append((f"largest workload shuffles {largest / 1e6:.1f} MB "
                   f"(need >= {MIN_LARGEST_SHUFFLE_BYTES / 1e6:.0f} MB)",
                   largest >= MIN_LARGEST_SHUFFLE_BYTES))

    print("\nGATE CHECKS")
    for label, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
    return all(passed for _, passed in checks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Shuffle-scaling gate for the A1 workloads.")
    parser.add_argument("--force", action="store_true",
                        help="ignore the RAM / AC-power pre-flight check (debugging only)")
    args = parser.parse_args()

    preflight_resource_check(force=args.force)
    spark = get_spark_session("verify_workloads", shuffle_partitions=CHECK_PARTITIONS)
    measured = measure_shuffle_bytes(spark)
    spark.stop()

    passed = evaluate_gate(measured)
    print("\nGATE PASSED -- workload factors change shuffle cost." if passed
          else "\nGATE FAILED -- do not run formal experiments.")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
