"""Single source of truth for building the project's SparkSession.

Every script obtains its session through :func:`get_spark_session`; no script
calls ``SparkSession.builder`` directly. This guarantees that every row in the
results dataset was produced under the same, documented configuration.

Fixed study settings
--------------------
* ``spark.sql.adaptive.enabled = false`` -- Adaptive Query Execution would
  coalesce shuffle partitions at runtime and override the A1 setting under test.
* ``spark.sql.autoBroadcastJoinThreshold = -1`` -- forces a sort-merge join so
  the orders table is genuinely shuffled (the v1 pilot's broadcast join left a
  ~10 KB shuffle that no workload factor could move).
* ``spark.driver.memory = 3g`` -- in ``local[*]`` mode executors run inside the
  driver JVM; 4g pushed the original 7.9 GB study machine to 87-96 % RAM.

Windows notes
-------------
* ``PYSPARK_PYTHON`` is pinned to ``sys.executable`` so Spark's Python workers
  start the venv interpreter rather than the Microsoft Store ``python`` alias.
* Event logging is opt-in: it needs ``winutils.exe`` and no experiment uses it
  (metrics come from sparkMeasure).

Effective settings are read back from Spark after the session starts; any
mismatch with the request raises, because a silently ignored setting would
invalidate every downstream result.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Must be set before pyspark launches the JVM / Python workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

from src.paths import SPARK_EVENT_LOG_DIR

DRIVER_MEMORY = "3g"
EXECUTOR_MEMORY = "3g"  # documentation only: no effect in local mode
LOCAL_CORES = "*"
SHUFFLE_PARTITIONS_DEFAULT = 200  # Spark's default -- the static A1 baseline
BROADCAST_JOIN_THRESHOLD = -1
SPARKMEASURE_PACKAGE = "ch.cern.sparkmeasure:spark-measure_2.12:0.28"


def get_spark_session(
    app_name: str,
    shuffle_partitions: int = SHUFFLE_PARTITIONS_DEFAULT,
    adaptive_enabled: bool = False,
    enable_sparkmeasure: bool = False,
    enable_event_log: bool = False,
) -> SparkSession:
    """Build (or reuse) the project SparkSession and verify its effective settings.

    Args:
        app_name: Name shown in the Spark UI; make it traceable to the experiment.
        shuffle_partitions: Initial ``spark.sql.shuffle.partitions`` (the A1 variable).
        adaptive_enabled: AQE flag; keep ``False`` for every study run.
        enable_sparkmeasure: Attach the sparkMeasure JAR (downloaded from Maven
            Central on first use, cached afterwards).
        enable_event_log: Write a Spark event log (requires ``winutils.exe`` on Windows).

    Returns:
        The active SparkSession.

    Raises:
        RuntimeError: If Spark's effective configuration differs from the request,
            typically because a session with other settings already exists.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{LOCAL_CORES}]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", BROADCAST_JOIN_THRESHOLD)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )

    if enable_event_log:
        SPARK_EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", SPARK_EVENT_LOG_DIR.as_uri())
        )

    if enable_sparkmeasure:
        builder = builder.config("spark.jars.packages", SPARKMEASURE_PACKAGE)

    spark = builder.getOrCreate()

    effective = {
        "app_name": app_name,
        "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled") == "true",
        "broadcast_join_threshold": int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")),
        "event_log_enabled": enable_event_log,
        "driver_memory": DRIVER_MEMORY,
        "spark_version": spark.version,
    }
    expected = {
        "shuffle_partitions": shuffle_partitions,
        "adaptive_enabled": adaptive_enabled,
        "broadcast_join_threshold": BROADCAST_JOIN_THRESHOLD,
    }
    mismatches = {k: (v, effective[k]) for k, v in expected.items() if effective[k] != v}
    if mismatches:
        raise RuntimeError(
            f"SparkSession ignored requested settings (expected, actual): {mismatches}. "
            "A session with different settings may already exist in this process."
        )

    print(f"[spark_config] Session started: {effective}")
    return spark


def main() -> None:
    """Smoke test: start and stop a session with non-default settings."""
    spark = get_spark_session("spark_config_smoke_test", shuffle_partitions=50)
    spark.stop()
    print("[spark_config] Smoke test PASSED.")


if __name__ == "__main__":
    main()
