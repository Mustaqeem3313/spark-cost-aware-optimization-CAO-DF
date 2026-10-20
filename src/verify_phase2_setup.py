"""Environment check: run after setup to confirm the machine can run the study.

Checks pinned packages, Java, the project SparkSession settings,
``environment.md``, and the baseline pipeline (correctness and shuffle plan).

Usage::

    python src/verify_phase2_setup.py        # exit code 0 = ready, 1 = fix the FAIL items
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.paths import ENVIRONMENT_MD_PATH

EXPECTED_PYSPARK_VERSION = "3.5.1"


def _check(label: str, fn: Callable[[], None]) -> bool:
    try:
        fn()
        print(f"  [PASS] {label}")
        return True
    except Exception as exc:
        print(f"  [FAIL] {label} -- {exc}")
        return False


def check_imports() -> None:
    import importlib

    for module in ("pandas", "psutil", "pyarrow", "sparkmeasure"):
        importlib.import_module(module)
    import pyspark

    assert pyspark.__version__ == EXPECTED_PYSPARK_VERSION, \
        f"expected pyspark {EXPECTED_PYSPARK_VERSION}, got {pyspark.__version__}"


def check_java() -> None:
    result = subprocess.run(["java", "-version"], capture_output=True, text=True)
    assert result.returncode == 0, "`java -version` failed -- install JDK 17 and set JAVA_HOME"


def check_spark_session() -> None:
    from src.spark_config import get_spark_session

    spark = get_spark_session("setup_check", shuffle_partitions=50)
    try:
        assert spark.conf.get("spark.sql.adaptive.enabled") == "false", "AQE is not disabled"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "50", "shuffle.partitions not applied"
    finally:
        spark.stop()


def check_environment_md() -> None:
    assert ENVIRONMENT_MD_PATH.exists(), "environment.md missing -- run src/environment_report.py"
    assert "CPU model" in ENVIRONMENT_MD_PATH.read_text(encoding="utf-8"), "environment.md looks malformed"


def check_pipeline() -> None:
    from src.baseline_pipeline import run_pipeline, verify_correctness, verify_shuffle_plan
    from src.spark_config import get_spark_session

    spark = get_spark_session("setup_check_pipeline", shuffle_partitions=10)
    try:
        orders = spark.createDataFrame(
            [(1, 101, "electronics", 250.0, "2026-01-01"), (2, 102, "grocery", 40.0, "2026-01-01")],
            ["order_id", "customer_id", "product_category", "amount", "order_date"],
        )
        customers = spark.createDataFrame(
            [(101, "north", "retail"), (102, "south", "retail")],
            ["customer_id", "region", "segment"],
        )
        result = run_pipeline(spark, orders, customers)
        verify_correctness(orders, result)
        assert verify_shuffle_plan(result), "pipeline plan does not shuffle both join sides"
    finally:
        spark.stop()


def main() -> None:
    print("=" * 60 + "\nENVIRONMENT VERIFICATION\n" + "=" * 60)
    checks = [
        ("Pinned packages import (pyspark, psutil, pandas, pyarrow, sparkmeasure)", check_imports),
        ("Java is installed and on PATH", check_java),
        ("SparkSession builds with AQE off and requested shuffle.partitions", check_spark_session),
        ("environment.md exists and looks valid", check_environment_md),
        ("Baseline pipeline runs, is correct, and shuffles", check_pipeline),
    ]
    results = []
    for number, (label, fn) in enumerate(checks, start=1):
        print(f"\n{number}. {label}")
        results.append(_check(label, fn))

    print("\n" + "=" * 60)
    if all(results):
        print("ENVIRONMENT READY.")
        print("=" * 60)
    else:
        print("ENVIRONMENT INCOMPLETE -- fix the FAIL items above.")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
