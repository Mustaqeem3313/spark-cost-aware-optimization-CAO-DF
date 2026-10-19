"""
verify_phase2_setup.py
========================
Run this ONE script after setting up your environment. If it prints
"PHASE 2 COMPLETE" at the end, your environment is correctly configured
and you're ready for Phase 3/4.

Usage:
    python3 src/verify_phase2_setup.py
"""

import sys
import os
import subprocess

# Ensure this works whether run as `python3 src/verify_phase2_setup.py`
# from the project root, or invoked from elsewhere.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def check(label, fn):
    try:
        fn()
        print(f"  [PASS] {label}")
        return True
    except Exception as e:
        print(f"  [FAIL] {label} -- {e}")
        return False


def main():
    print("=" * 60)
    print("PHASE 2 SETUP VERIFICATION")
    print("=" * 60)

    results = []

    print("\n1. Checking pinned package imports...")
    def imports():
        import pyspark, psutil, pandas, pyarrow
        assert pyspark.__version__ == "3.5.1", f"Expected pyspark 3.5.1, got {pyspark.__version__}"
    results.append(check("pyspark/psutil/pandas/pyarrow import correctly", imports))

    print("\n2. Checking Java is available...")
    def java_check():
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        assert result.returncode == 0, "java -version failed"
    results.append(check("Java is installed and on PATH", java_check))

    print("\n3. Checking SparkSession builds with project config...")
    def spark_check():
        from src.spark_config_v1 import get_spark_session
        spark = get_spark_session("phase2_verify", shuffle_partitions=50, adaptive_enabled=False)
        assert spark.conf.get("spark.sql.adaptive.enabled") == "false", "AQE was not disabled!"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "50", "shuffle.partitions not applied!"
        spark.stop()
    results.append(check("SparkSession builds with AQE off + correct shuffle.partitions", spark_check))

    print("\n4. Checking environment.md exists...")
    def env_md_check():
        with open("environment.md") as f:
            content = f.read()
        assert "CPU model" in content, "environment.md looks malformed"
    results.append(check("environment.md exists and looks valid", env_md_check))

    print("\n5. Checking baseline pipeline runs and shuffles correctly...")
    def pipeline_check():
        from src.spark_config_v1 import get_spark_session
        from src.baseline_pipeline_v1 import run_pipeline, verify_shuffle_plan, verify_correctness

        spark = get_spark_session("phase2_verify_pipeline", shuffle_partitions=10, adaptive_enabled=False)
        orders = spark.createDataFrame(
            [(1, 101, "electronics", 250.0, "2026-01-01"),
             (2, 102, "grocery", 40.0, "2026-01-01")],
            ["order_id", "customer_id", "product_category", "amount", "order_date"]
        )
        customers = spark.createDataFrame(
            [(101, "north", "retail"), (102, "south", "retail")],
            ["customer_id", "region", "segment"]
        )
        result = run_pipeline(spark, orders, customers)
        verify_correctness(orders, result)
        shuffle_ok = verify_shuffle_plan(result)
        spark.stop()
        assert shuffle_ok, "Pipeline did not shuffle as expected"
    results.append(check("Baseline pipeline runs, is correct, and shuffles", pipeline_check))

    print("\n" + "=" * 60)
    if all(results):
        print("PHASE 2 COMPLETE -- environment is correctly configured.")
        print("Next: Phase 3 (finalize baseline pipeline) / Phase 4 (workload generator)")
    else:
        print("PHASE 2 INCOMPLETE -- fix the FAIL items above before proceeding.")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()
