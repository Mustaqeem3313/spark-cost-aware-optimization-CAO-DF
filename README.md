# CAO-DF: Cost-Aware Optimization Decision Framework for Apache Spark

**A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines**
M.Sc. Computer Science research project (Research Work–I), Shaikh Mustaqeem Ishratali, AKI's Poona College of Arts, Science & Commerce, Pune (Savitribai Phule Pune University). Guide: Dr. Reshma Kadam.

**Status (28 Oct 2026):** experiments, analysis and the 8-page IEEE paper (`paper/`, tag `v1.0-paper`) are complete. The college report (black book) is complete; guide review and submission remain.

Optimizations such as shuffle-partition tuning or file compaction are not free: they cost time to find or apply, and they pay off only if the pipeline then runs often enough. CAO-DF models the total cost of an optimization action *a* over *N* pipeline runs

```
TC(a, N) = C_opt(a) + N * C_run(a)
```

and recommends an action only when *N* exceeds its break-even point

```
N* = C_opt(a) / (C_run(static) - C_run(a))
```

Two actions are studied on a frozen reference pipeline (filter, sort-merge join, high-cardinality aggregation):

| Action | Optimization | Factors varied |
|---|---|---|
| **A1** | `spark.sql.shuffle.partitions` tuning | data volume x shuffle volume |
| **A2** | small-file compaction | data volume x file count |

## Requirements

| Component | Version | Notes |
|---|---|---|
| Python | **3.11** | PySpark 3.5.1 does not work on Python 3.13 |
| Java | JDK 17 (e.g. Eclipse Temurin) | `JAVA_HOME` must be set |
| Windows only | `winutils.exe` and `hadoop.dll` (Hadoop 3.3.x) in `C:\hadoop\bin` | `HADOOP_HOME=C:\hadoop`; needed for Parquet writes |
| Internet | first run only | sparkMeasure JAR is downloaded from Maven Central, then cached |

Python packages are pinned in [`requirements.txt`](requirements.txt).

## Setup (Windows, PowerShell)

```powershell
# from the repository root
& "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

java -version                         # must print 17.x
python src\verify_phase2_setup.py     # must end with "ENVIRONMENT READY."
```

On Linux/macOS use `python3.11 -m venv venv` and `source venv/bin/activate`; winutils is not needed.

Scripts resolve every path from the repository root, so they can be run from any directory.

## Reproducing the study

Before any timed step: connect AC power, select the best-performance power mode and close other applications. The runners refuse to start on battery or with less than 3.5 GB free RAM.

| Step | Command | Output |
|---|---|---|
| 1. Record the machine | `python src\environment_report.py` | `environment.md` (confirm the disk type by hand) |
| 2. Check the environment | `python src\verify_phase2_setup.py` | pass/fail report |
| 3. Generate workloads | `python src\workload_generator.py` | `data/generated/` (10 workloads), `experiments/configs/workload_index.csv` |
| 4. Shuffle-scaling gate | `python src\verify_workloads.py` | must print `GATE PASSED` |
| 5. Exploratory sweep | `python src\explore_partition_curve.py` | `results/raw/pilot_a1_v2_sweep.csv` |
| 6. Formal A1 matrix | `python src\run_a1_experiments.py 2>&1 \| Tee-Object logs\a1_formal.log` | `results/raw/experiment_results.csv`, `results/processed/a1_*.csv` |
| 7. A2 pilot (gate) | `python src\run_a2_experiments.py --pilot` | `results/raw/pilot_a2.csv`, `results/processed/pilot_a2_*.csv` |
| 8. Formal A2 matrix | `python src\run_a2_experiments.py 2>&1 \| Tee-Object logs\a2_formal.log` | `results/raw/experiment_results.csv`, `results/processed/a2_*.csv` |
| A1 outlier sensitivity (no Spark) | `python src\robustness_check.py` | `results/processed/a1_outlier_robustness.csv` |
| 9. Cost model and break-even (no Spark) | `python src\cost_model.py [--price 0.0505] [--vcpus 12]` | `results/processed/cost_per_cell.csv`, `breakeven_table.csv`, `cost_sensitivity.csv`; `results/figures/total_cost_curves_*` |
| 10. Unseen validation workloads | `python src\workload_generator.py --validation` | `data/generated/V_*`, `experiments/configs/validation_index.csv` |
| 11. Framework validation runs (E6) | `python src\run_validation.py 2>&1 \| Tee-Object logs\validation.log` | `results/raw/experiment_results.csv` (`VAL` rows), `results/processed/validation_cell_summary.csv` |
| 12. Decision framework + validation (no Spark) | `python src\decision_framework.py [--delta 0.10]` | `validation_decisions.csv`, `validation_summary.csv`, `framework_loo.csv` |
| Ask the framework for one workload | `python src\decision_framework.py --decide --orders 500000 --files 25 --shuffle 0.5 --runs 5` | printed choice: Nothing / Tune / Compact |
| ...or answer five questions instead | `python src\decision_framework.py --interactive` | same printout; warns if inputs lie outside the study range |
| 13. Paper figures (no Spark) | `python src\make_figures.py` | `paper/figures/fig_*.pdf`, `results/figures/fig_*.png` |
| 14. Statistical analysis (no Spark) | `python src\stats_analysis.py` | `results/processed/stats_*.csv` (bootstrap CIs, effect sizes, verdicts) |
| 15. Analysis notebook | `python analysis\build_notebook.py`, then `jupyter nbconvert --to notebook --execute --inplace analysis\analysis.ipynb` | `analysis/analysis.ipynb`, `analysis/figures/fig*.png` |
| 16. Reference check (internet) | `python tools\check_references.py` | DOI/title/pages vs Crossref and arXiv; URL status |
| 17. Similarity check (internet on first run) | `python tools\similarity_check.py` | `docs/similarity_report.md` (local proxy, not Turnitin) |
| Tests | `python -m unittest discover -s tests -v` | 20 tests |

Costs are estimated from published AWS EC2 on-demand pricing, never billed; the price, proxies and assumptions are documented in [`Cost_Model_Assumptions.md`](Cost_Model_Assumptions.md).

Workload data is deterministic (NumPy seed 42) and is not committed; step 3 recreates it exactly.

The formal A1 matrix is 4 workloads x 8 partition settings {1, 2, 4, 8, 16, 50, 200, 400} x 5 randomized blocks = 160 measured runs. Each measured run is preceded by a discarded warm-up, and every cell is executed once untimed before block 1.

The formal A2 matrix is 6 workloads (small/large x 1/10/50 files) x 3 stages (fragmented, compaction, compacted) x 5 blocks = 90 measured runs, with inputs read from Parquet on every run (no caching) and shuffle partitions held at the default 200.

All findings, anomalies, limitations and decisions are recorded in [`docs/research_log.md`](docs/research_log.md).

## Repository layout

```
src/
  paths.py                    project-root-relative paths used by every script
  spark_config.py             the only SparkSession builder; verifies effective settings
  baseline_pipeline.py        frozen reference pipeline (v2)
  workload_generator.py       deterministic synthetic workloads + manifest per workload
  instrumentation.py          pre-flight check, measurement harness, results CSV writer
  experiment_runner.py        randomized-block executor (A1) and per-cell summaries (A1, A2)
  verify_phase2_setup.py      environment check
  verify_workloads.py         shuffle-scaling gate
  explore_partition_curve.py  exploratory A1 sweep (chooses the formal grid)
  run_a1_experiments.py       formal A1 matrix
  compaction.py               A2 action: rewrite fragmented Parquet into target-sized files
  run_a2_experiments.py       A2 pilot and formal matrix (fragmented -> compaction -> compacted)
  robustness_check.py         A1 sensitivity: do conclusions change if noisy-cell outliers are dropped?
  cost_model.py               estimated cost (proxies P1/P2), TC(a, N), break-even N*, price sensitivity
  run_validation.py           E6: all actions on the unseen validation workloads
  decision_framework.py       CAO-DF rule (nearest measured workload + noise floor) and its validation
  make_figures.py             builds every paper figure from the result files
  stats_analysis.py           Phase 10: bootstrap CIs, effect sizes, hypothesis verdicts
  environment_report.py       generates environment.md
analysis/                     build_notebook.py -> analysis.ipynb (+ HTML, figures)
tests/                        unit tests for stats_analysis.py and decision_framework.py
tools/                        check_references.py, similarity_check.py (paper checks)
docs/research_log.md          dated record of runs, findings, limitations and decisions
experiments/configs/          workload index
results/raw/                  per-run measurements (append-only)
results/processed/            per-cell summaries
results/figures/              paper figures
logs/                         console logs of formal runs
analysis/                     analysis notebooks
paper/                        IEEE manuscript
literature/                   literature review, matrix, master plan
archive/pilot_v1/             superseded v1 pilot code and machine record
environment.md                study machine specification (Experimental Setup)
```

## Results schema

`results/raw/experiment_results.csv` has one row per measured run with 34 columns, defined in `RESULTS_CSV_COLUMNS` in `src/instrumentation.py`: run identity (workload, action, configuration, repetition block), workload factor levels, wall-clock seconds, sparkMeasure stage metrics (executor run/CPU time, shuffle bytes, spill, GC) and system CPU and memory utilisation. The writer refuses to append to a file with a different header.

## Research integrity rules

1. Every SparkSession is built by `get_spark_session()` in `src/spark_config.py`.
2. Adaptive Query Execution stays disabled for all A1 and A2 experiments.
3. Results files are append-only and script-written; never edit them by hand. Mark bad runs in the `notes` column instead of deleting them.
4. The pipeline in `src/baseline_pipeline.py` is frozen. Any change requires a new `PIPELINE_VERSION`, and results from different versions are not compared.
5. A machine change requires a new `environment.md` with a dated note; earlier records are archived.