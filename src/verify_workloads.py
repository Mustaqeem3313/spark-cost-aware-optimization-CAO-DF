"""
verify_workloads.py
=====================
The shuffle-scaling gate that the v1 pilot was missing.

Phase 3's verify_shuffle_plan() only proves Exchange operators EXIST. The v1
pilot showed that is not enough: shuffles existed but moved ~10 KB for every
workload, so the "data volume" and "shuffle volume" factors changed nothing.
This script runs the pipeline once per workload and measures how many bytes
were actually shuffled, then checks that the workload factors really move it:

    large-vs-small data volume   -> shuffle bytes grow   (>= 5x)
    high-vs-low shuffle level    -> shuffle bytes grow   (>= 2x)
    largest workload             -> shuffles >= 1 MB     (not trivial)

Shuffle bytes are read from Spark's own REST API (localhost UI), so this has
no dependency on the sparkMeasure JAR.

Usage:   python src/verify_workloads.py
Exit code 0 = gate passed, 1 = failed (do not run formal experiments).
"""

import sys
import json
import time
import urllib.request

sys.path.insert(0, ".")
from src.spark_config import get_spark_session
from src.baseline_pipeline import execute_pipeline
from src.instrumentation import load_workload, preflight_resource_check

WORKLOADS = ["A1_small_low", "A1_small_high", "A1_large_low", "A1_large_high"]
CHECK_PARTITIONS = 50


def _stage_totals(spark):
    """Cumulative (shuffle_write_bytes, shuffle_read_bytes, n_completed_stages)."""
    sc = spark.sparkContext
    port = sc.uiWebUrl.rsplit(":", 1)[1].strip("/")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # bypass any system proxy
    url = f"http://localhost:{port}/api/v1/applications/{sc.applicationId}/stages?status=complete"
    with opener.open(url, timeout=5) as r:
        stages = json.load(r)
    return (sum(s.get("shuffleWriteBytes", 0) for s in stages),
            sum(s.get("shuffleReadBytes", 0) for s in stages),
            len(stages))


def _settled_totals(spark, min_stages):
    """Spark's UI updates asynchronously -- wait until the totals stop changing."""
    last = None
    deadline = time.time() + 15
    while time.time() < deadline:
        cur = _stage_totals(spark)
        if cur[2] > min_stages and cur == last:
            return cur
        last = cur
        time.sleep(0.5)
    return last


def main():
    force = "--force" in sys.argv
    preflight_resource_check(force=force)
    spark = get_spark_session("verify_workloads", shuffle_partitions=CHECK_PARTITIONS, adaptive_enabled=False)
    spark.conf.set("spark.sql.shuffle.partitions", CHECK_PARTITIONS)

    measured = {}
    for wid in WORKLOADS:
        orders_df, customers_df, meta = load_workload(spark, wid)
        orders_df, customers_df = orders_df.cache(), customers_df.cache()
        orders_df.count(); customers_df.count()          # materialize cache (not measured)
        before = _settled_totals(spark, -1)
        execute_pipeline(spark, orders_df, customers_df)  # the measured execution
        after = _settled_totals(spark, before[2])
        wb, rb = after[0] - before[0], after[1] - before[1]
        measured[wid] = wb
        print(f"  {wid:15s} orders={meta['num_orders']:>8}  shuffle written={wb/1e6:9.2f} MB   read={rb/1e6:9.2f} MB")

    spark.stop()

    print("\nGATE CHECKS")
    ok = True

    def check(label, passed):
        nonlocal ok
        ok &= passed
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")

    for lvl in ("low", "high"):
        r = measured[f"A1_large_{lvl}"] / max(measured[f"A1_small_{lvl}"], 1)
        check(f"data volume moves shuffle bytes ({lvl} shuffle level): large/small = {r:.1f}x (need >= 5x)", r >= 5)
    for vol in ("small", "large"):
        r = measured[f"A1_{vol}_high"] / max(measured[f"A1_{vol}_low"], 1)
        check(f"shuffle level moves shuffle bytes ({vol} data): high/low = {r:.1f}x (need >= 2x)", r >= 2)
    check(f"largest workload shuffles a non-trivial amount: {measured['A1_large_high']/1e6:.1f} MB (need >= 1 MB)",
          measured["A1_large_high"] >= 1e6)

    print("\nGATE PASSED -- workload factors genuinely change shuffle cost." if ok
          else "\nGATE FAILED -- do NOT run formal experiments; the workload factors are not doing their job.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()


#python src/explore_partition_curve.py