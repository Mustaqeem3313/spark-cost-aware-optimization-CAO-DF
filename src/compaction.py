"""Phase 7 -- action A2: file compaction.

Compaction rewrites a fragmented Parquet dataset into ``ceil(total_size /
target_file_bytes)`` files at a *new* path, leaving the source untouched so it
can be compacted again in every repetition block. The rewrite is a regular
Spark job, so its cost (``Copt`` for A2) is measured with the same harness as
the pipeline itself.

``coalesce`` is used rather than ``repartition``: like bin-packing compaction
in Delta Lake ``OPTIMIZE`` or Iceberg ``rewrite_data_files``, it merges input
splits without a full shuffle, so ``Copt`` reflects a realistic rewrite.

Usage (self-test on one workload)::

    python src/compaction.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from src.paths import PROJECT_ROOT

TARGET_FILE_BYTES = 128 * 1024 * 1024
COMPACTED_DATA_DIR = PROJECT_ROOT / "data" / "compacted"


def parquet_files(path: Path) -> list[Path]:
    """Data files (``*.parquet``) of a Spark-written Parquet directory."""
    return sorted(Path(path).glob("*.parquet"))


def dataset_size_bytes(path: Path) -> int:
    """Total on-disk size of the Parquet data files under ``path``."""
    return sum(f.stat().st_size for f in parquet_files(path))


def target_file_count(size_bytes: int, target_file_bytes: int = TARGET_FILE_BYTES) -> int:
    """Number of output files for a dataset of ``size_bytes`` (at least 1)."""
    return max(1, math.ceil(size_bytes / target_file_bytes))


def compact(
    spark: SparkSession,
    source_path: Path,
    output_path: Path,
    target_file_bytes: int = TARGET_FILE_BYTES,
) -> int:
    """Rewrite the Parquet dataset at ``source_path`` into target-sized files at ``output_path``.

    Any existing data at ``output_path`` is overwritten.

    Returns:
        The number of files the compacted dataset was written as.

    Raises:
        FileNotFoundError: If ``source_path`` holds no Parquet files.
    """
    size = dataset_size_bytes(source_path)
    if size == 0:
        raise FileNotFoundError(f"No Parquet files under {source_path}")
    num_files = target_file_count(size, target_file_bytes)
    (spark.read.parquet(str(source_path))
          .coalesce(num_files)
          .write.mode("overwrite")
          .parquet(str(output_path)))
    return num_files


def main() -> None:
    """Self-test: compact A2_large_high and check row count and file count are as expected."""
    import pandas as pd

    from src.paths import WORKLOAD_INDEX_PATH, from_project_relative
    from src.spark_config import get_spark_session

    workload_id = "A2_large_high"
    row = pd.read_csv(WORKLOAD_INDEX_PATH).set_index("workload_id").loc[workload_id]
    source = from_project_relative(row["orders_path"])
    output = COMPACTED_DATA_DIR / "selftest" / workload_id

    spark = get_spark_session("compaction_selftest")
    expected_files = compact(spark, source, output)
    source_rows = spark.read.parquet(str(source)).count()
    output_rows = spark.read.parquet(str(output)).count()
    spark.stop()

    actual_files = len(parquet_files(output))
    print(f"[compaction] {workload_id}: {len(parquet_files(source))} files "
          f"({dataset_size_bytes(source) / 1e6:.1f} MB) -> {actual_files} file(s) "
          f"({dataset_size_bytes(output) / 1e6:.1f} MB); rows {source_rows} -> {output_rows}")
    if source_rows != output_rows or actual_files != expected_files:
        print("[compaction] Self-test FAILED.")
        sys.exit(1)
    print("[compaction] Self-test PASSED.")


if __name__ == "__main__":
    main()
