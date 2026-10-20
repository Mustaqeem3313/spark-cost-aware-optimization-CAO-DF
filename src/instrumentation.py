"""
instrumentation.py
====================
Wraps any pipeline action with the full measurement protocol from the
execution plan: warm-up runs (discarded), repeated measured runs, real
Spark stage/shuffle metrics (via sparkmeasure) alongside wall-clock time
and system resource sampling, and an append-only results CSV.

This is the ONLY place experiment measurements get written -- Phase 6/7's
experiment runner calls run_and_measure(), never writes results.csv by hand.

Design decisions (belongs in the Methodology section):
  - sparkmeasure gives real JVM-level metrics (executorCpuTime, shuffle
    bytes, GC time) -- far more informative than wall-clock alone, and
    exactly what Phase 8's cost model needs. It requires the sparkMeasure
    JAR (fetched from Maven Central on first use, then cached) -- if that
    fetch fails for any reason (no internet, firewall), this module
    degrades gracefully to wall-clock + system resource sampling only,
    rather than crashing an entire experiment run.
  - CPU/memory are sampled via a lightweight background polling thread
    (psutil, system-wide) rather than trying to isolate the Spark JVM's
    child-process PID -- simpler, portable, and reasonable given the
    measurement-isolation protocol (background apps closed) already
    established in Phase 2.
  - Results are appended via Python's built-in csv module, not pandas --
    deliberately avoiding another dependency-version landmine for
    something this simple. Header is written once, on first creation.
"""

import os
import csv
import time
import threading
import statistics
from datetime import datetime

import psutil
import pandas as pd

RESULTS_CSV_PATH = "results/raw/experiment_results.csv"
WORKLOAD_INDEX_PATH = "experiments/configs/workload_index.csv"

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

_sparkmeasure_warned = False  # only print the fallback warning once per process


class _ResourceMonitor:
    """Background thread sampling system-wide CPU% / memory% during a run."""

    def __init__(self, interval=0.2):
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread = None
        self.cpu_samples = []
        self.mem_samples = []

    def _poll(self):
        while not self._stop_event.is_set():
            self.cpu_samples.append(psutil.cpu_percent(interval=None))
            self.mem_samples.append(psutil.virtual_memory().percent)
            time.sleep(self.interval)

    def start(self):
        self._stop_event.clear()
        self.cpu_samples, self.mem_samples = [], []
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else None
        peak_mem = max(self.mem_samples) if self.mem_samples else None
        return avg_cpu, peak_mem


def preflight_resource_check(min_available_gb: float = 3.5, force: bool = False) -> None:
    """
    Enforces the measurement-isolation protocol in code instead of trusting
    memory: the v1 pilot ran with system RAM at 87-96% used, which risks
    swapping noise. Aborts (unless force=True) if free RAM is too low, and
    refuses to run on battery. Call once, BEFORE creating the SparkSession.
    """
    avail_gb = psutil.virtual_memory().available / (1024 ** 3)
    battery = psutil.sensors_battery()
    on_battery = battery is not None and not battery.power_plugged
    print(f"[preflight] Available RAM: {avail_gb:.1f} GB (need >= {min_available_gb} GB); "
          f"power: {'BATTERY' if on_battery else 'AC/none'}")
    problems = []
    if avail_gb < min_available_gb:
        problems.append(f"only {avail_gb:.1f} GB RAM available -- close browsers/IDEs/other apps")
    if on_battery:
        problems.append("running on battery -- plug in AC power and use the High-performance power plan")
    if problems and not force:
        raise SystemExit("[preflight] ABORTING -- measurement isolation not met: "
                         + "; ".join(problems) + ".  (Override with --force only for debugging.)")
    if problems:
        print("[preflight] WARNING (forced): " + "; ".join(problems))


def load_workload(spark, workload_id: str):
    """
    Reads a workload's orders/customers DataFrames plus its recorded
    metadata, using experiments/configs/workload_index.csv as the lookup
    (written by Phase 4's generate_all_workloads()). This is how Phase 6/7
    go from "workload_id" to actual data without ever hand-typing a path.
    """
    index = pd.read_csv(WORKLOAD_INDEX_PATH)
    row = index[index["workload_id"] == workload_id]
    if row.empty:
        raise ValueError(f"'{workload_id}' not found in {WORKLOAD_INDEX_PATH}. "
                          f"Did you run workload_generator.py?")
    row = row.iloc[0]

    orders_df = spark.read.parquet(row["orders_path"])
    customers_df = spark.read.parquet(row["customers_path"])

    metadata = {
        "data_volume": row["data_volume"],
        "file_count_level": row["file_count_level"],
        "shuffle_volume_level": row["shuffle_volume_level"],
        "num_orders": int(row["num_orders"]),
    }
    return orders_df, customers_df, metadata


def load_and_cache_workloads(spark, workload_ids: list) -> dict:
    """
    Reads each workload once and caches it in memory (materialized with an
    untimed count) so timed runs measure compute/shuffle cost, not repeated
    Parquet I/O. Returns {workload_id: (orders_df, customers_df, metadata)}.
    """
    out = {}
    for wid in workload_ids:
        orders_df, customers_df, meta = load_workload(spark, wid)
        orders_df, customers_df = orders_df.cache(), customers_df.cache()
        orders_df.count()
        customers_df.count()
        out[wid] = (orders_df, customers_df, meta)
        print(f"  Cached '{wid}': {meta['num_orders']} orders")
    return out


def run_and_measure(spark, action_fn, run_metadata: dict,
                     warmup_runs: int = 1, measured_runs: int = 5,
                     use_sparkmeasure: bool = True) -> list:
    """
    Runs action_fn() (warmup_runs + measured_runs) times total, discards
    the warm-ups, and returns a list of metric dicts -- one per measured
    repetition. Does NOT write to disk; call append_results() with the
    return value when you're ready to persist.

    Parameters
    ----------
    action_fn : callable, zero-arg
        Must trigger a Spark action (e.g. `lambda: result_df.count()`).
        Must be safe to call repeatedly (idempotent).
    run_metadata : dict
        Static fields attached to every row: workload_id, experiment_action,
        config_label, shuffle_partitions, adaptive_enabled, and the
        workload metadata from load_workload().
    """
    global _sparkmeasure_warned

    stagemetrics = None
    if use_sparkmeasure:
        try:
            from sparkmeasure import StageMetrics
            stagemetrics = StageMetrics(spark)
            # The JAR-not-loaded failure doesn't surface until .begin() is
            # actually called (StageMetrics(spark) itself succeeds even
            # without the JAR) -- so do a cheap begin()/end() pair here as
            # a pre-flight check, before entering the timed measurement
            # loop below.
            stagemetrics.begin()
            stagemetrics.end()
        except Exception as e:
            if not _sparkmeasure_warned:
                print(f"[instrumentation] WARNING: sparkmeasure unavailable ({e}).\n"
                      f"  Most likely cause: the SparkSession was built with "
                      f"enable_sparkmeasure=False (or omitted -- it defaults to False).\n"
                      f"  Fix: pass enable_sparkmeasure=True to get_spark_session().\n"
                      f"  (Less likely: no internet access to Maven Central on first use.)\n"
                      f"  Falling back to wall-clock + system resource metrics only "
                      f"for this session. Stage/shuffle-level metrics will be missing.")
                _sparkmeasure_warned = True
            stagemetrics = None

    results = []
    total_runs = warmup_runs + measured_runs

    for i in range(total_runs):
        is_warmup = i < warmup_runs
        # Callers that invoke run_and_measure() once per repetition block
        # (the Phase 6/7 runners) pass "repetition_block" so every row gets
        # its true repetition number and a unique run_id. v1 omitted this,
        # so all 60 pilot rows said repetition 1.
        block = run_metadata.get("repetition_block")
        rep_number = 0 if is_warmup else (block if block is not None else (i - warmup_runs + 1))

        monitor = _ResourceMonitor()
        monitor.start()

        if stagemetrics is not None:
            stagemetrics.begin()

        start = time.perf_counter()
        action_fn()
        wall_clock = time.perf_counter() - start

        sm_metrics = {}
        if stagemetrics is not None:
            stagemetrics.end()
            try:
                sm_metrics = stagemetrics.aggregate_stagemetrics()
            except Exception as e:
                print(f"[instrumentation] WARNING: could not aggregate sparkmeasure "
                      f"metrics for this run ({e}). Continuing without them.")
                sm_metrics = {}

        avg_cpu, peak_mem = monitor.stop()

        row = {
            "run_id": f"{run_metadata.get('experiment_action', 'run')}_{run_metadata.get('workload_id', '')}_"
                      f"{run_metadata.get('config_label', '')}_"
                      f"{('warmup' + str(block or '')) if is_warmup else f'rep{rep_number}'}",
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
            "sparkmeasure_available": stagemetrics is not None,
            "stage_count": sm_metrics.get("numStages"),
            "task_count": sm_metrics.get("numTasks"),
            "elapsed_time_ms": sm_metrics.get("elapsedTime"),
            "stage_duration_ms": sm_metrics.get("stageDuration"),
            "executor_run_time_ms": sm_metrics.get("executorRunTime"),
            "executor_cpu_time_ms": sm_metrics.get("executorCpuTime"),
            "jvm_gc_time_ms": sm_metrics.get("jvmGCTime"),
            "shuffle_total_bytes_read": sm_metrics.get("shuffleTotalBytesRead"),
            "shuffle_bytes_written": sm_metrics.get("shuffleBytesWritten"),
            "shuffle_records_written": sm_metrics.get("shuffleRecordsWritten"),
            "memory_bytes_spilled": sm_metrics.get("memoryBytesSpilled"),
            "disk_bytes_spilled": sm_metrics.get("diskBytesSpilled"),
            "peak_execution_memory": sm_metrics.get("peakExecutionMemory"),
            "bytes_read": sm_metrics.get("bytesRead"),
            "records_read": sm_metrics.get("recordsRead"),
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
        print(f"[instrumentation] {run_metadata.get('config_label', '')} [{tag}]: "
              f"wall_clock={wall_clock:.3f}s")

    if stagemetrics is not None:
        # Detach this run's listener now that we're done with it -- without
        # this, a script calling run_and_measure() many times in a loop
        # (Phase 6/7's experiment runners call it dozens of times) would
        # silently accumulate listeners on the SparkSession for its entire
        # lifetime, each still receiving every subsequent stage-completion
        # event. Harmless for a handful of calls, real overhead at
        # matrix-experiment scale.
        try:
            stagemetrics.remove_listener()
        except Exception:
            pass

    return results


def append_results(rows: list, path: str = RESULTS_CSV_PATH, include_warmups: bool = False):
    """
    Appends measured rows to the results CSV. Warm-up rows are excluded by
    default (is_warmup=True rows exist in the return of run_and_measure()
    for transparency, but shouldn't pollute your analysis dataset unless
    you deliberately want to inspect warm-up behavior).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rows_to_write = rows if include_warmups else [r for r in rows if not r["is_warmup"]]
    if not rows_to_write:
        return

    file_exists = os.path.exists(path)
    if file_exists:
        with open(path, newline="") as f:
            existing_header = next(csv.reader(f), [])
        if existing_header != RESULTS_CSV_COLUMNS:
            raise RuntimeError(
                f"{path} was written with a different column schema than this version of "
                f"instrumentation.py. Appending would silently misalign columns. Archive it "
                f"first, e.g. rename it to pilot_a1_v1_results.csv, then re-run.")
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in rows_to_write:
            writer.writerow(row)

    print(f"[instrumentation] Appended {len(rows_to_write)} row(s) to {path}")


def check_measurement_quality(results: list, cv_threshold: float = 0.20) -> dict:
    """
    Implements the pilot-gate noise check from the execution plan: computes
    median, IQR, and coefficient of variation (CV) across measured
    (non-warmup) wall-clock times for one configuration, and flags if CV
    exceeds the threshold -- meaning: don't trust this config's numbers yet,
    investigate noise sources before proceeding.
    """
    times = [r["wall_clock_seconds"] for r in results if not r["is_warmup"]]
    if len(times) < 2:
        return {"median": times[0] if times else None, "iqr": None, "cv": None, "cv_ok": None}

    median = statistics.median(times)
    mean = statistics.mean(times)
    stdev = statistics.stdev(times)
    sorted_times = sorted(times)
    q1 = sorted_times[len(sorted_times) // 4]
    q3 = sorted_times[(3 * len(sorted_times)) // 4]
    cv = stdev / mean if mean > 0 else None
    cv_ok = (cv is not None) and (cv <= cv_threshold)

    quality = {
        "median": round(median, 4),
        "iqr": round(q3 - q1, 4),
        "cv": round(cv, 4) if cv is not None else None,
        "cv_ok": cv_ok,
    }

    if not cv_ok:
        print(f"[instrumentation] WARNING: CV = {quality['cv']:.2%} exceeds "
              f"{cv_threshold:.0%} threshold. Results are noisy -- investigate "
              f"before trusting this configuration's numbers.")

    return quality


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from src.spark_config import get_spark_session
    from src.baseline_pipeline import execute_pipeline, PIPELINE_VERSION
    from src.workload_generator import generate_workload
    import os as _os

    spark = get_spark_session("phase5_instrumentation_test", shuffle_partitions=50,
                               adaptive_enabled=False, enable_sparkmeasure=True)

    # Generate a single small workload directly for this smoke test --
    # no need to regenerate the full 10-workload matrix just to verify
    # instrumentation works.
    if not _os.path.exists(WORKLOAD_INDEX_PATH):
        print("[instrumentation] No workload index found -- generating one test workload.")
        m = generate_workload(spark, "smoke_test_workload", data_volume="small",
                               file_count_level="medium", shuffle_volume_level="low", seed=42)
        # Write a minimal one-row index so load_workload() can find it.
        pd.DataFrame([{
            "workload_id": m["workload_id"],
            "data_volume": m["requested"]["data_volume"],
            "file_count_level": m["requested"]["file_count_level"],
            "shuffle_volume_level": m["requested"]["shuffle_volume_level"],
            "seed": m["seed"],
            "num_orders": m["measured"]["num_orders_generated"],
            "actual_file_count": m["measured"]["actual_order_file_count"],
            "actual_size_mb": m["measured"]["actual_order_size_mb"],
            "orders_path": m["paths"]["orders"],
            "customers_path": m["paths"]["customers"],
        }]).to_csv(WORKLOAD_INDEX_PATH, index=False)
        workload_id_to_test = "smoke_test_workload"
    else:
        workload_id_to_test = "A1_small_low"

    orders_df, customers_df, workload_meta = load_workload(spark, workload_id_to_test)

    def action():
        execute_pipeline(spark, orders_df, customers_df)

    run_metadata = {
        "workload_id": workload_id_to_test,
        "pipeline_version": PIPELINE_VERSION,
        "experiment_action": "A1_smoke_test",
        "config_label": "shuffle50",
        "shuffle_partitions": 50,
        "adaptive_enabled": False,
        **workload_meta,
    }

    # use_sparkmeasure=True is attempted; if this sandbox can't reach Maven
    # Central, it will print one warning and fall back automatically --
    # that fallback path itself is part of what's being verified here.
    results = run_and_measure(spark, action, run_metadata,
                               warmup_runs=1, measured_runs=3, use_sparkmeasure=True)

    quality = check_measurement_quality(results)
    print(f"\n[instrumentation] Measurement quality: {quality}")

    append_results(results)

    spark.stop()
    print("\n[instrumentation] Phase 5 smoke test complete.")
