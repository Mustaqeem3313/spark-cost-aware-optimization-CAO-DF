"""
spark_config.py
================
Single source of truth for creating a SparkSession across this entire project.

Every experiment script MUST build its SparkSession through get_spark_session()
below -- never call SparkSession.builder directly in an experiment script.
This is what guarantees every run in experiment_results.csv was produced under
identical, documented conditions (the single biggest threat to credibility in
a study like this is silent configuration drift between runs).

Key methodological safeguard baked in here:
  - spark.sql.adaptive.enabled is FORCED OFF for A1 (shuffle/partition tuning)
    experiments. Spark 3.x's AQE auto-coalesces shuffle partitions at runtime,
    which would silently override whatever spark.sql.shuffle.partitions value
    you set, and you'd end up measuring AQE's decisions instead of yours.
    For A2 (compaction) experiments, AQE may stay at its default -- compaction
    doesn't interact with shuffle-partition count the same way, but this is
    still passed explicitly so it's never ambiguous which mode a run used.

Why event logging is OFF by default (Windows compatibility note):
  Spark's event log writes through Hadoop's local-filesystem permission-setting
  code, which requires a Windows-only binary (winutils.exe) that isn't part of
  a normal PySpark install. This project's actual metrics come from
  sparkmeasure (a listener attached directly to the live session, Phase 5),
  which needs no event log file at all -- so event logging was never
  load-bearing here, and forcing it on by default just adds a platform-specific
  failure mode for no benefit. It stays available as an opt-in
  (enable_event_log=True) for anyone who wants the Spark History Server UI,
  with setup instructions in README.md.
Why the Python interpreter is set explicitly via sys.executable:
  On Windows, Spark's local[*] mode spawns Python worker subprocesses by
  invoking "python" as a bare command. Windows often intercepts that with
  the Microsoft Store's "App Execution Alias" stub instead of the venv's
  real interpreter, so the worker process never actually starts and the
  JVM times out waiting for it ("Python worker failed to connect back").
  Setting spark.pyspark.python / spark.pyspark.driver.python explicitly to
  sys.executable sidesteps this entirely and portably -- it always points
  to whichever interpreter is actually running this script, on any machine,
  regardless of PATH or Windows alias settings. This is a code-level fix,
  not a machine-specific Windows settings change, so it can't silently
  break again on a different machine or after a reinstall.
"""

from pyspark.sql import SparkSession
import os
import sys

# Fixed for the entire study -- do not change between runs.
# If you must change these, you have started a new experiment series and
# must re-run environment_report.py and note the change in environment.md.
DRIVER_MEMORY = "4g"
EXECUTOR_MEMORY = "4g"
LOCAL_CORES = "*"          # local[*] -- uses all available cores, fixed for the study
EVENT_LOG_DIR = os.path.abspath("./spark-event-logs")
SHUFFLE_PARTITIONS_DEFAULT = 200  # Spark's own default -- your A1 baseline


def get_spark_session(app_name: str, shuffle_partitions: int = SHUFFLE_PARTITIONS_DEFAULT,
                       adaptive_enabled: bool = False, enable_sparkmeasure: bool = False,
                       enable_event_log: bool = False):
    """
    Build a SparkSession with every setting explicit and logged.

    Parameters
    ----------
    app_name : str
        Name this run appears under in the event log / Spark UI. Use something
        traceable to your experiment matrix, e.g. "A1_run_shuffle400_rep3".
    shuffle_partitions : int
        Value for spark.sql.shuffle.partitions. This is your A1 independent
        variable -- pass it explicitly on every call, never rely on a default.
    adaptive_enabled : bool
        Must be False for every A1 experiment run (see module docstring).
        Leave as False by default project-wide unless you have a specific,
        documented reason to test with AQE on.
    enable_sparkmeasure : bool
        If True, attaches the sparkMeasure JAR via Maven coordinates (Phase 5).
        Requires internet access to Maven Central on first run (it caches
        locally after that).
    enable_event_log : bool
        Off by default -- see module docstring for why (Windows winutils.exe
        requirement). Only turn on if you've installed winutils and want the
        Spark History Server UI; not needed for any experiment in this study.
    """
    # Belt-and-suspenders alongside the spark.pyspark.python config below --
    # some Spark code paths read these env vars directly rather than the
    # Spark conf when deciding how to spawn worker subprocesses.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{LOCAL_CORES}]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
        # Explicit interpreter path -- fixes the Windows "Python worker failed
        # to connect back" error caused by the Microsoft Store App Execution
        # Alias intercepting a bare "python" command. See module docstring.
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )

    if enable_event_log:
        os.makedirs(EVENT_LOG_DIR, exist_ok=True)
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", EVENT_LOG_DIR)
        )

    if enable_sparkmeasure:
        # Version 0.28 is the officially recommended sparkMeasure release for
        # Spark 3.x with Scala 2.12 (confirmed against the project's own
        # compatibility table -- an earlier pin at version 0.24 turned out
        # to not exist on Maven Central at all, which was the real root
        # cause of every "jar not loaded" failure so far, unrelated to
        # internet access).
        builder = builder.config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.28")

    spark = builder.getOrCreate()

    # Log the effective config immediately so every run is self-documenting.
    effective = {
        "app_name": app_name,
        "shuffle_partitions": shuffle_partitions,
        "adaptive_enabled": adaptive_enabled,
        "event_log_enabled": enable_event_log,
        "driver_memory": DRIVER_MEMORY,
        "executor_memory": EXECUTOR_MEMORY,
        "spark_version": spark.version,
    }
    print(f"[spark_config] Session started with: {effective}")

    return spark


if __name__ == "__main__":
    # Smoke test -- confirms the builder itself works before anything else
    # in the project depends on it.
    spark = get_spark_session("setup_smoke_test", shuffle_partitions=50, adaptive_enabled=False)
    print("Spark version:", spark.version)
    print("Effective shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
    print("Effective AQE setting:", spark.conf.get("spark.sql.adaptive.enabled"))
    spark.stop()
    print("[spark_config] Smoke test PASSED -- session started and stopped cleanly.")
