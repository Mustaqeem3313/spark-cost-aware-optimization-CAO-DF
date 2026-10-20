"""Measurement harness: runs a pipeline action and records its cost.

Every experiment row in the study is produced by :func:`run_and_measure` and
persisted by :func:`append_results`; no script writes the results CSV directly.

Each measured execution records:

* wall-clock time (``time.perf_counter``);
* JVM-level stage metrics from sparkMeasure (executor CPU/run time, shuffle
  bytes, spill, GC) -- the inputs to the cost model;
* system-wide CPU % (mean) and memory % (peak) sampled by a background thread.

If the sparkMeasure JAR is unavailable the harness falls back to wall-clock and
system metrics, and marks the row with ``sparkmeasure_available = False``.
"""

from __future__ import annotations

import csv
import statistics
import sys
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import psutil
from pyspark.sql import DataFrame, SparkSession

from src.paths import RESULTS_CSV_PATH, RESULTS_RAW_DIR, WORKLOAD_INDEX_PATH, from_project_relative

MIN_AVAILABLE_RAM_GB = 3.5
DEFAULT_CV_THRESHOLD = 0.20
RESOURCE_SAMPLE_INTERVAL_S = 0.2

RESULTS_CSV_COLUMNS = [
    "run_id", "timestamp", "pipeline_version", "workload_id", "experiment_action", "config_label",
    "shuffle_partitions", "adaptive_enabled", "repetition_number", "is_warmup",
    "data_volume", "file_count_level", "shuffle_volume_level", "num_orders",
    "wall_clock_seconds",
    "sparkmeasure_available",
    "stage_count", "task_count", "elapsed_time_ms", "stage_duration_ms",
    "executor_run_time_ms", "executor_cpu_time_ms", "jvm_gc_time_ms",
    "shuffle_total_bytes_read", "shuffle_bytes_written", "shuffle_records_written",
    "memory_bytes_spilled", "disk_bytes_spilled", "peak_execution_memory",
    "bytes_read", "records_read",
    "peak_memory_percent", "avg_cpu_percent",
    "notes",
]

# Results-CSV column -> sparkMeasure aggregate metric name.
_SPARKMEASURE_FIELDS = {
    "stage_count": "numStages",
    "task_count": "numTasks",
    "elapsed_time_ms": "elapsedTime",
    "stage_duration_ms": "stageDuration",
    "executor_run_time_ms": "executorRunTime",
    "executor_cpu_time_ms": "executorCpuTime",
    "jvm_gc_time_ms": "jvmGCTime",
    "shuffle_total_bytes_read": "shuffleTotalBytesRead",
    "shuffle_bytes_written": "shuffleBytesWritten",
    "shuffle_records_written": "shuffleRecordsWritten",
    "memory_bytes_spilled": "memoryBytesSpilled",
    "disk_bytes_spilled": "diskBytesSpilled",
    "peak_execution_memory": "peakExecutionMemory",
    "bytes_read": "bytesRead",
    "records_read": "recordsRead",
}

WorkloadData = tuple[DataFrame, DataFrame, dict[str, Any]]

_sparkmeasure_warned = False


class _ResourceMonitor:
    """Background thread sampling system-wide CPU % and memory % during one run."""

    def __init__(self, interval: float = RESOURCE_SAMPLE_INTERVAL_S) -> None:
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.cpu_samples: list[float] = []
        self.mem_samples: list[float] = []

    def _poll(self) -> None:
        while not self._stop_event.is_set():
            self.cpu_samples.append(psutil.cpu_percent(interval=None))
            self.mem_samples.append(psutil.virtual_memory().percent)
            time.sleep(self.interval)

    def start(self) -> None:
        self._stop_event.clear()
        self.cpu_samples, self.mem_samples = [], []
        # The first non-blocking cpu_percent() call always returns 0.0; prime it
        # so every recorded sample covers a real interval.
        psutil.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def stop(self) -> tuple[float | None, float | None]:
        """Stop sampling and return ``(mean_cpu_percent, peak_memory_percent)``."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else None
        peak_mem = max(self.mem_samples) if self.mem_samples else None
        return avg_cpu, peak_mem


def preflight_resource_check(min_available_gb: float = MIN_AVAILABLE_RAM_GB, force: bool = False) -> None:
    """Enforce the measurement-isolation protocol before any timed work.

    Aborts if free RAM is below ``min_available_gb`` or the machine is on
    battery. Call once, before creating the SparkSession.

    Args:
        min_available_gb: Minimum free RAM required.
        force: Downgrade failures to warnings (debugging only; never for formal runs).

    Raises:
        SystemExit: If a condition fails and ``force`` is ``False``.
    """
    avail_gb = psutil.virtual_memory().available / (1024 ** 3)
    battery = psutil.sensors_battery()
    on_battery = battery is not None and not battery.power_plugged
    print(f"[preflight] Available RAM: {avail_gb:.1f} GB (need >= {min_available_gb} GB); "
          f"power: {'BATTERY' if on_battery else 'AC'}")

    problems = []
    if avail_gb < min_available_gb:
        problems.append(f"only {avail_gb:.1f} GB RAM available -- close browsers, IDEs and other apps")
    if on_battery:
        problems.append("running on battery -- connect AC power and use the best-performance power mode")

    if problems and not force:
        raise SystemExit("[preflight] ABORTING -- measurement isolation not met: "
                         + "; ".join(problems) + ". (Override with --force for debugging only.)")
    if problems:
        print("[preflight] WARNING (forced): " + "; ".join(problems))


def load_workload(spark: SparkSession, workload_id: str) -> WorkloadData:
    """Read a generated workload via the workload index.

    Returns:
        ``(orders_df, customers_df, metadata)`` where ``metadata`` holds the
        workload factor levels and order count recorded in every results row.

    Raises:
        FileNotFoundError: If the workload index does not exist.
        ValueError: If ``workload_id`` is not in the index.
    """
    if not WORKLOAD_INDEX_PATH.exists():
        raise FileNotFoundError(f"{WORKLOAD_INDEX_PATH} not found. Run src/workload_generator.py first.")

    index = pd.read_csv(WORKLOAD_INDEX_PATH)
    match = index[index["workload_id"] == workload_id]
    if match.empty:
        raise ValueError(f"'{workload_id}' not found in {WORKLOAD_INDEX_PATH}. Run src/workload_generator.py.")
    row = match.iloc[0]

    orders_df = spark.read.parquet(str(from_project_relative(row["orders_path"])))
    customers_df = spark.read.parquet(str(from_project_relative(row["customers_path"])))
    metadata = {
        "data_volume": row["data_volume"],
        "file_count_level": row["file_count_level"],
        "shuffle_volume_level": row["shuffle_volume_level"],
        "num_orders": int(row["num_orders"]),
    }
    return orders_df, customers_df, metadata


def load_and_cache_workloads(spark: SparkSession, workload_ids: list[str]) -> dict[str, WorkloadData]:
    """Load each workload once and cache it in memory (materialized, untimed).

    Caching makes timed runs measure compute and shuffle cost rather than
    repeated Parquet I/O.
    """
    workloads = {}
    for workload_id in workload_ids:
        orders_df, customers_df, metadata = load_workload(spark, workload_id)
        orders_df, customers_df = orders_df.cache(), customers_df.cache()
        orders_df.count()
        customers_df.count()
        workloads[workload_id] = (orders_df, customers_df, metadata)
        print(f"  Cached '{workload_id}': {metadata['num_orders']} orders")
    return workloads


def _init_stage_metrics(spark: SparkSession) -> Any | None:
    """Return a ready sparkMeasure ``StageMetrics`` object, or ``None`` if unavailable."""
    global _sparkmeasure_warned
    try:
        from sparkmeasure import StageMetrics

        stage_metrics = StageMetrics(spark)
        # A missing JAR only surfaces on begin(), so probe it before timing anything.
        stage_metrics.begin()
        stage_metrics.end()
        return stage_metrics
    except Exception as exc:
        if not _sparkmeasure_warned:
            print(f"[instrumentation] WARNING: sparkMeasure unavailable ({exc}).\n"
                  "  Likely cause: the session was built without enable_sparkmeasure=True, or\n"
                  "  Maven Central was unreachable on first use. Recording wall-clock and\n"
                  "  system metrics only; stage/shuffle metrics will be empty.")
            _sparkmeasure_warned = True
        return None


def _build_run_id(run_metadata: dict[str, Any], is_warmup: bool, block: int | None, rep_number: int) -> str:
    suffix = f"warmup{block or ''}" if is_warmup else f"rep{rep_number}"
    return (f"{run_metadata.get('experiment_action', 'run')}_{run_metadata.get('workload_id', '')}_"
            f"{run_metadata.get('config_label', '')}_{suffix}")


def run_and_measure(
    spark: SparkSession,
    action_fn: Callable[[], Any],
    run_metadata: dict[str, Any],
    warmup_runs: int = 1,
    measured_runs: int = 5,
    use_sparkmeasure: bool = True,
) -> list[dict[str, Any]]:
    """Execute ``action_fn`` ``warmup_runs + measured_runs`` times and measure each run.

    Warm-up rows are returned (flagged ``is_warmup=True``) for transparency;
    :func:`append_results` drops them by default. Nothing is written to disk here.

    Args:
        spark: Active session.
        action_fn: Zero-argument callable that triggers a Spark action; must be
            safe to call repeatedly.
        run_metadata: Static fields for every row -- ``pipeline_version``,
            ``workload_id``, ``experiment_action``, ``config_label``,
            ``shuffle_partitions``, ``adaptive_enabled``, the workload metadata
            from :func:`load_workload`, and optionally ``repetition_block``.
            Runners that call this once per block pass ``repetition_block`` so
            each row carries its true repetition number.
        warmup_runs: Discarded executions before the measured ones.
        measured_runs: Measured executions.
        use_sparkmeasure: Collect sparkMeasure stage metrics when available.

    Returns:
        One dict per execution, keyed by :data:`RESULTS_CSV_COLUMNS`.
    """
    stage_metrics = _init_stage_metrics(spark) if use_sparkmeasure else None
    block = run_metadata.get("repetition_block")
    results = []

    for i in range(warmup_runs + measured_runs):
        is_warmup = i < warmup_runs
        if is_warmup:
            rep_number = 0
        else:
            rep_number = block if block is not None else i - warmup_runs + 1

        monitor = _ResourceMonitor()
        monitor.start()
        if stage_metrics is not None:
            stage_metrics.begin()

        start = time.perf_counter()
        action_fn()
        wall_clock = time.perf_counter() - start

        sm_metrics: dict[str, Any] = {}
        if stage_metrics is not None:
            stage_metrics.end()
            try:
                sm_metrics = stage_metrics.aggregate_stagemetrics()
            except Exception as exc:
                print(f"[instrumentation] WARNING: could not aggregate sparkMeasure metrics ({exc}).")

        avg_cpu, peak_mem = monitor.stop()

        row: dict[str, Any] = {
            "run_id": _build_run_id(run_metadata, is_warmup, block, rep_number),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "pipeline_version": run_metadata.get("pipeline_version"),
            "workload_id": run_metadata.get("workload_id"),
            "experiment_action": run_metadata.get("experiment_action"),
            "config_label": run_metadata.get("config_label"),
            "shuffle_partitions": run_metadata.get("shuffle_partitions"),
            "adaptive_enabled": run_metadata.get("adaptive_enabled"),
            "repetition_number": rep_number,
            "is_warmup": is_warmup,
            "data_volume": run_metadata.get("data_volume"),
            "file_count_level": run_metadata.get("file_count_level"),
            "shuffle_volume_level": run_metadata.get("shuffle_volume_level"),
            "num_orders": run_metadata.get("num_orders"),
            "wall_clock_seconds": round(wall_clock, 4),
            "sparkmeasure_available": stage_metrics is not None,
            **{column: sm_metrics.get(metric) for column, metric in _SPARKMEASURE_FIELDS.items()},
            "peak_memory_percent": round(peak_mem, 2) if peak_mem is not None else None,
            "avg_cpu_percent": round(avg_cpu, 2) if avg_cpu is not None else None,
            "notes": "",
        }
        results.append(row)

        if is_warmup:
            tag = "WARMUP (discarded)"
        elif block is not None:
            tag = f"block {block}"
        else:
            tag = f"rep {rep_number}/{measured_runs}"
        print(f"[instrumentation] {run_metadata.get('workload_id', '')} "
              f"{run_metadata.get('config_label', '')} [{tag}]: wall_clock={wall_clock:.3f}s")

    if stage_metrics is not None:
        # Detach the listener; otherwise repeated calls accumulate listeners on the session.
        try:
            stage_metrics.remove_listener()
        except Exception:
            pass

    return results


def append_results(
    rows: list[dict[str, Any]],
    path: Path | str = RESULTS_CSV_PATH,
    include_warmups: bool = False,
) -> None:
    """Append rows to a results CSV, writing the header when the file is new.

    Raises:
        RuntimeError: If an existing file has a different header, since appending
            would silently misalign columns. Archive the old file first.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_write = rows if include_warmups else [r for r in rows if not r["is_warmup"]]
    if not rows_to_write:
        return

    file_exists = path.exists()
    if file_exists:
        with path.open(newline="") as f:
            existing_header = next(csv.reader(f), [])
        if existing_header != RESULTS_CSV_COLUMNS:
            raise RuntimeError(f"{path} has a different column schema from RESULTS_CSV_COLUMNS. "
                               "Archive or rename it before appending.")

    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)

    print(f"[instrumentation] Appended {len(rows_to_write)} row(s) to {path.name}")


def check_measurement_quality(
    results: list[dict[str, Any]],
    cv_threshold: float = DEFAULT_CV_THRESHOLD,
    verbose: bool = True,
) -> dict[str, Any]:
    """Summarise measured wall-clock times for one configuration.

    Returns:
        ``{"median", "iqr", "cv", "cv_ok"}``. ``cv`` is the coefficient of
        variation (stdev / mean); ``cv_ok`` is ``False`` when it exceeds
        ``cv_threshold``, meaning the configuration is too noisy to trust.
    """
    times = [r["wall_clock_seconds"] for r in results if not r["is_warmup"]]
    if len(times) < 2:
        return {"median": times[0] if times else None, "iqr": None, "cv": None, "cv_ok": None}

    mean = statistics.mean(times)
    q1, _, q3 = statistics.quantiles(times, n=4, method="inclusive")
    cv = statistics.stdev(times) / mean if mean > 0 else None
    cv_ok = cv is not None and cv <= cv_threshold

    quality = {
        "median": round(statistics.median(times), 4),
        "iqr": round(q3 - q1, 4),
        "cv": round(cv, 4) if cv is not None else None,
        "cv_ok": cv_ok,
    }
    if verbose and not cv_ok and cv is not None:
        print(f"[instrumentation] WARNING: CV {cv:.1%} exceeds the {cv_threshold:.0%} threshold.")
    return quality


def main() -> None:
    """Smoke test: measure A1_small_low at 50 partitions (results kept out of the study dataset)."""
    from src.baseline_pipeline import PIPELINE_VERSION, execute_pipeline
    from src.spark_config import get_spark_session

    workload_id = "A1_small_low"
    smoke_csv = RESULTS_RAW_DIR / "smoke_test_results.csv"

    spark = get_spark_session("instrumentation_smoke_test", shuffle_partitions=50, enable_sparkmeasure=True)
    orders_df, customers_df, workload_meta = load_workload(spark, workload_id)
    run_metadata = {
        "pipeline_version": PIPELINE_VERSION,
        "workload_id": workload_id,
        "experiment_action": "A1_smoke_test",
        "config_label": "shuffle50",
        "shuffle_partitions": 50,
        "adaptive_enabled": False,
        **workload_meta,
    }
    results = run_and_measure(spark, lambda: execute_pipeline(spark, orders_df, customers_df),
                              run_metadata, warmup_runs=1, measured_runs=3)
    spark.stop()

    print(f"[instrumentation] Quality: {check_measurement_quality(results)}")
    append_results(results, path=smoke_csv)
    print("[instrumentation] Smoke test complete.")


if __name__ == "__main__":
    main()
