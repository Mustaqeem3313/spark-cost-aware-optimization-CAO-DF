"""Filesystem locations used across the project, resolved from the repository root.

Resolving every path from this module (rather than from the current working
directory) means each script behaves identically however it is launched.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_GENERATED_DIR = PROJECT_ROOT / "data" / "generated"
WORKLOAD_INDEX_PATH = PROJECT_ROOT / "experiments" / "configs" / "workload_index.csv"
VALIDATION_INDEX_PATH = PROJECT_ROOT / "experiments" / "configs" / "validation_index.csv"

RESULTS_RAW_DIR = PROJECT_ROOT / "results" / "raw"
RESULTS_PROCESSED_DIR = PROJECT_ROOT / "results" / "processed"
RESULTS_FIGURES_DIR = PROJECT_ROOT / "results" / "figures"
RESULTS_CSV_PATH = RESULTS_RAW_DIR / "experiment_results.csv"

ENVIRONMENT_MD_PATH = PROJECT_ROOT / "environment.md"
SPARK_EVENT_LOG_DIR = PROJECT_ROOT / "spark-event-logs"


def to_project_relative(path: Path) -> str:
    """Return ``path`` relative to the project root in POSIX form (portable across OSes)."""
    return Path(path).resolve().relative_to(PROJECT_ROOT).as_posix()


def from_project_relative(relative: str) -> Path:
    """Resolve a project-relative path (as stored in the workload index) to an absolute path."""
    return PROJECT_ROOT / Path(relative)
