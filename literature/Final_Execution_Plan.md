# 🎯 Final Execution Plan
### *A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines*

**Researcher:** Mustaqeem Shaikh · M.Sc. Computer Science 2 · Roll No. 9251
**Target venue:** IEEE-sponsored regional conference (Xplore-indexed)
**Timeline:** 8 weeks (compressed from the original 12-week draft)
**Infrastructure:** Local-first (free tier only) → optional cloud validation if time/budget allow
**Status as of this document:** Phase 0 ✅ and Phase 1 ✅ complete

---

## 📌 Project Snapshot

| | |
|---|---|
| **Research question** | Is applying a Spark optimization action economically worthwhile, given workload characteristics, optimization cost, and expected future executions? |
| **Actions under study** | **A1** — Spark shuffle/partition tuning · **A2** — File compaction |
| **Core contribution** | A lightweight, interpretable, workload-aware **cost-benefit decision framework** with an explicit **break-even (OBEP)** criterion — not another Spark performance optimizer |
| **What this is NOT** | Not a new tuning algorithm, not a production system, not a multi-tool cloud pipeline. Stay inside the box (see §6). |
| **Novelty framing for your guide** | *"I'm not making Spark faster. I'm researching whether making it faster — or compacting its data — is worth the cost for a given workload."* |

---

## ✅ Phase 0 & Phase 1 — Already Complete

| Phase | Deliverable | Status |
|---|---|---|
| **Phase 0** — Research preparation | `Phase0_Technical_Understanding.md` | ✅ Done |
| **Phase 1** — Literature review | `Literature_Matrix.xlsx` (24 papers, 6 categories) + `Literature_Review_Shaikh.docx` (prose, IEEE-cited) | ✅ Done |

Everything below assumes these two are finished and picks up at Phase 2.

---

## 🗓️ Compressed 8-Week Timeline

```
Week 1   ████████  Environment Setup + Baseline Pipeline           (Phase 2 + 3)
Week 2   ████████  Workload Generator + Instrumentation + PILOT     (Phase 4 + 5)
Week 3   ████████  A1 Experiments — Shuffle/Partition Tuning        (Phase 6)
Week 4   ████████  A2 Experiments — File Compaction                 (Phase 7)
Week 5   ████████  Cost Model + Break-Even Analysis                 (Phase 8)
Week 6   ████████  Decision Framework — Build + Validate            (Phase 9)
Week 7   ████████  Statistical Analysis + Figures + H1/H2           (Phase 10)
Week 8   ████████  Paper Writing + Revision + Submission Package    (Phase 11)
```

**Why this fits in 8 weeks where the original needed 12:**
- Phases that don't depend on each other now run **in parallel** (workload generator + instrumentation, Week 2).
- The experiment matrix is **deliberately narrowed** (see §4) — fewer combinations, same scientific validity.
- Cost model + break-even analysis are **merged into one phase** (they're tightly coupled — no reason to separate them).
- Framework build + framework validation are **merged into one phase** (you validate as you build, not after).

---

## 🔬 Phase-by-Phase Execution Plan

### Phase 2 — Environment Setup *(Week 1, first half)*

**Goal:** A reproducible, documented, confound-free local Spark environment.

- [ ] Pin exact versions: Python 3.11, Java 17, PySpark 3.5.x, `pandas`, `pyarrow`, `matplotlib`, `seaborn`, `psutil`, `sparkmeasure` → freeze into `requirements.txt`
- [ ] Write `environment.md`: CPU, RAM, disk type, OS, Java/Spark/Python versions, fixed `spark.driver.memory` / `spark.executor.memory`
- [ ] Set `spark.sql.adaptive.enabled=false` for all A1 experiments *(prevents AQE from silently overriding your manual shuffle-partition settings — see §5)*
- [ ] Enable `spark.eventLog.enabled=true` with a fixed local log directory
- [ ] Establish the measurement isolation protocol: AC power, closed background apps, no other users on the machine during a run session

**Deliverable:** Working local PySpark environment + `environment.md` + `requirements.txt`

---

### Phase 3 — Baseline Pipeline *(Week 1, second half)*

**Goal:** One reproducible pipeline that provably shuffles, provably compacts, and produces correct output.

```
Input Parquet → Filter → Join → Aggregation → Transformation → Output Parquet
```

- [ ] Join against a second dataset with real cardinality (not a trivial self-join)
- [ ] GroupBy aggregation on a column with meaningful cardinality
- [ ] Run `.explain(True)` and **confirm `Exchange` (shuffle) nodes appear** in the physical plan — do this before anything else
- [ ] Seed all data generation (fixed random seed) for full reproducibility
- [ ] Run once, unmeasured, and validate correctness (row counts, aggregate sums) before instrumenting

**Deliverable:** `src/baseline_pipeline.py`, verified to shuffle and to produce correct output

---

### Phase 4 — Workload Generator *(Week 2, parallel with Phase 5)*

**Goal:** Deterministic, parameterized dataset generation.

```python
generate_workload(
    data_volume="medium",      # small / medium / large
    file_count="high",          # low / medium / high  (fragmentation)
    shuffle_volume="high"       # low / medium / high
)
```

- [ ] Seeded generation for full reproducibility
- [ ] Confirm generated file counts and sizes match the intended fragmentation level (sanity-check with `ls`/`du`, not assumption)

**Deliverable:** `src/workload_generator.py`

---

### Phase 5 — Instrumentation *(Week 2, parallel with Phase 4)*

**Goal:** Capture real Spark metrics, not just wall-clock time.

For every run, record:

| Field | Source |
|---|---|
| Run ID, Workload ID, Configuration | Your experiment harness |
| Data volume, file count, shuffle volume | Workload generator metadata |
| Execution time (median of repeats) | `sparkmeasure` / event log |
| Input/output bytes, shuffle read/write, task count, spill | `sparkmeasure` |
| CPU-time, peak memory | `psutil` |

- [ ] Wire `sparkmeasure` into the pipeline runner
- [ ] Confirm metrics appear correctly on a single test run before scaling up
- [ ] Design `results.csv` schema now — this is what Phase 8's cost model will read

**Deliverable:** `src/instrumentation.py`, validated on one manual run

---

### 🚦 Pilot Gate *(end of Week 2 — mandatory checkpoint)*

**Pre-registered pass criterion** (decide this now, not after seeing results):
> *Pilot passes if the largest vs. smallest shuffle-partition configuration shows a consistent-direction runtime difference greater than 15%, replicated across all 5 repetitions — AND the largest vs. smallest fragmentation level shows a similarly consistent compaction benefit.*

Run protocol for the pilot:
- 1–2 discarded warm-up runs per configuration
- 5 recorded repetitions per configuration, **order randomized**
- Report median + IQR + coefficient of variation (CV) per configuration
- **If CV > 15–20%:** stop, investigate noise sources, do not scale up yet

**If pilot passes →** proceed to Phase 6 with confidence.
**If pilot fails →** adjust workload extremes or configuration range before committing to the full matrix. This is a normal, expected part of the process — not a failure of the project.

---

### Phase 6 — A1: Shuffle/Partition Tuning *(Week 3)*

**Goal:** Measure the cost-performance trade-off of `spark.sql.shuffle.partitions` across workloads.

**Compressed experiment matrix:**

| Factor | Levels (narrowed from 3×3×3) |
|---|---|
| Data volume | Small, Large *(2 levels)* |
| Shuffle volume | Low, High *(2 levels)* |
| Partition config | Default (200), 50, 400 *(3 levels)* |
| Repetitions | 5, order-randomized |

→ **2 × 2 × 3 × 5 = 60 runs** (manageable in a day or two of wall-clock time on small local data)

- [ ] Run full matrix with warm-up + randomization protocol from Phase 5
- [ ] Append every run to `experiment_results.csv` automatically — never hand-edit this file
- [ ] Identify best-performing partition config per workload condition

**Deliverable:** Populated `experiment_results.csv` (A1 rows) + best-config-per-workload table

---

### Phase 7 — A2: File Compaction *(Week 4)*

**Goal:** Measure compaction's cost (overhead) and benefit (subsequent-read savings) across fragmentation levels.

**Compressed experiment matrix:**

| Factor | Levels |
|---|---|
| Fragmentation | Low, Medium, High *(3 levels)* |
| Data volume | Small, Large *(2 levels)* |
| Repetitions | 5, order-randomized |

→ **3 × 2 × 5 = 30 runs**, each measured **before** and **after** compaction

- [ ] Measure: runtime, file count, cost — before compaction
- [ ] Run compaction; measure its own runtime/resource cost (this is `Copt` for A2)
- [ ] Measure: runtime, file count, cost — after compaction

**Deliverable:** Populated `experiment_results.csv` (A2 rows) + compaction cost/benefit table

---

### Phase 8 — Cost Model + Break-Even Analysis *(Week 5)*
*(merged from the original two separate phases — they're one coherent unit of work)*

**Goal:** Convert raw measurements into estimated dollar costs, and compute break-even points.

- [ ] Write `Cost_Model_Assumptions.md`: document the published cloud pricing figures you're using (cite source, e.g., AWS EC2/Glue pricing page) and exactly how measured CPU-time/memory maps to an estimated cost — **be explicit this is an estimate, not billed cost**
- [ ] Implement:
  ```
  TC(a, N) = Copt(a) + N × Crun(a)
  ```
- [ ] For each action (A1, A2) and each workload condition, compute the break-even N:
  ```
  N > Copt / (Crun_static − Crun_optimized)
  ```
- [ ] Build break-even curves (execution frequency → decision)

**Deliverable:** `src/cost_model.py` + `Cost_Model_Assumptions.md` + break-even table per workload condition

---

### Phase 9 — Decision Framework: Build + Validate *(Week 6)*
*(merged — validate as you build, not as a separate later phase)*

**Goal:** A simple, rule-based framework that picks the economical action per workload.

```
Workload → [Volume, Files, Shuffle, Frequency] → Cost-Benefit Model → {Nothing | Tune | Compact}
```

- [ ] Implement as a rule-based decision function (no ML — explicitly out of scope, see §7)
- [ ] **Immediately validate**: generate workloads the framework hasn't "seen," compare Static pipeline vs. Framework decision, record total estimated cost for each
- [ ] Never fabricate or round example numbers — every figure in the eventual paper must trace back to `experiment_results.csv`

**Deliverable:** `src/decision_framework.py` + validation results table (Static vs. Framework, per workload)

---

### Phase 10 — Statistical Analysis, Figures & Hypothesis Testing *(Week 7)*
*(merged from original Phases 12–15)*

**Analysis, in order of evidentiary strength:**
1. Descriptive statistics (mean, median, range, % change)
2. Performance plots (data volume → runtime; shuffle volume → optimal partition config)
3. Cost plots (execution frequency → total cost)
4. Break-even curves
5. Significance tests — **only** where repeated-run counts genuinely support it; no manufactured significance

**Target figures:**

| # | Figure |
|---|---|
| 1 | Workload size vs. execution time |
| 2 | Shuffle volume vs. optimal partition configuration |
| 3 | File count vs. compaction benefit |
| 4 | Optimization overhead (Copt) |
| 5 | Total cost vs. execution frequency |
| 6 | Break-even point curves |
| 7 | Static vs. cost-aware framework (total cost comparison) |

**Hypothesis analysis:**
- [ ] **H1** — Does optimization benefit vary significantly with workload characteristics? *(Either outcome is a legitimate, publishable result.)*
- [ ] **H2** — Does the framework reduce total estimated cost vs. static configuration, across workloads? *(Same — a null result here is still a real finding, analyzed honestly.)*

**Deliverable:** `analysis/analysis.ipynb`, all 7 figures, H1/H2 verdicts with supporting evidence

---

### Phase 11 — Paper Writing, Revision & Submission Package *(Week 8)*

**Structure:**
1. Introduction
2. Related Work *(already drafted from your literature review — adapt, don't rewrite from scratch)*
3. Problem Definition
4. Proposed Framework
5. Experimental Methodology
6. Results
7. Discussion
8. Threats & Limitations *(local single-machine execution, estimated not billed cost, narrowed matrix — state these plainly)*
9. Conclusion
10. References

- [ ] Draft Sections 1–5 early in the week (mostly reusable from your framework docs)
- [ ] Sections 6–8 written directly from Phase 10 output — no numbers invented at writing time
- [ ] Run through a plagiarism checker (Turnitin/iThenticate if available) before submission
- [ ] Package the reproducibility bundle: code, configs, `experiment_results.csv`, instructions

**Deliverable:** Submission-ready IEEE-formatted paper + reproducibility package

---

## 🛡️ Methodological Safeguards *(the fixes baked into this plan)*

| Risk | Safeguard | Where |
|---|---|---|
| AQE silently overrides manual partition tuning | `spark.sql.adaptive.enabled=false` for A1 | Phase 2 |
| JIT/JVM warm-up bias | 1–2 discarded warm-up runs before every recorded run | Phase 6, 7 |
| Systematic drift (thermal, background load) | Randomized run order per repetition block | Phase 6, 7 |
| Untraceable, unreproducible results | `environment.md`, seeded generation, pinned versions | Phase 2, 3, 4 |
| Local ≠ distributed cluster | Explicitly stated as a scope decision in the paper | Phase 11 (Limitations) |
| Fabricated/rounded example numbers reaching the paper | Every figure traced back to `experiment_results.csv`, no manual edits | Phase 9, 10 |
| Noisy results treated as signal | CV threshold (15–20%) checked before trusting any config's numbers | Phase 6 pilot gate |
| Scope creep | §7 boundary — stay inside the box | All phases |

---

## 📁 Repository Structure

```
spark-cost-aware-optimization/
│
├── README.md
├── environment.md
├── requirements.txt
│
├── literature/
│   ├── literature_matrix.xlsx
│   └── literature_review.docx
│
├── data/
│   ├── raw/
│   └── generated/
│
├── src/
│   ├── workload_generator.py
│   ├── baseline_pipeline.py
│   ├── spark_tuning.py          # A1
│   ├── compaction.py            # A2
│   ├── instrumentation.py
│   ├── cost_model.py
│   └── decision_framework.py
│
├── experiments/
│   ├── configs/
│   └── experiment_matrix.csv
│
├── results/
│   ├── raw/
│   ├── processed/
│   └── figures/
│
├── analysis/
│   └── analysis.ipynb
│
├── paper/
│   └── research_paper.docx
│
└── Cost_Model_Assumptions.md
```

---

## 🚧 Research Scope Boundary — Stay Inside the Box

```
                 SPARK PIPELINE
                       │
              ┌────────┴────────┐
              │                 │
          A1 Tuning        A2 Compaction
              │                 │
              └────────┬────────┘
                       │
                Cost-aware decision
                       │
              ┌────────┴────────┐
              │                 │
       Workload features    Frequency
              │                 │
              └────────┬────────┘
                       │
                  Total Cost
```

**Explicitly out of scope** (do not add, even if it seems easy): Kafka, Airflow, Databricks, Kubernetes, autoscaling, data skew handling, caching, scheduling, ML/RL-based decision-making, LLMs. Any of these turns a completable 8-week MSc project into an unfinishable one.

---

## 💬 What You'll Tell Your Guide

**"What are you researching?"**
> *"I'm studying whether Spark pipeline optimization is economically worthwhile under different workload conditions — focusing on two techniques, shuffle/partition tuning and file compaction. I vary data volume, fragmentation, shuffle volume, and execution frequency, measure optimization overhead and resulting cost, and build a workload-aware decision framework that determines whether applying an optimization is justified."*

**"What's your novelty?"**
> *"Not a new Spark optimization algorithm — the decision problem of when an existing optimization should be applied, combining workload characteristics, optimization overhead, and expected future execution cost."*

**"How will you calculate cloud cost?"**
> *"Measured resource usage experimentally, estimated using published cloud pricing — not claimed as billed expenditure."*

**"Why not machine learning?"**
> *"The goal is first establishing the empirical workload-to-cost relationship and validating a simple, interpretable decision rule. ML is reasonable future work once that relationship is established."*

---

## ⚠️ Risk Register

| # | Problem | Response |
|---|---|---|
| 1 | Cloud costs become an issue | Run locally; use published pricing for estimates only |
| 2 | Dataset too large for local machine | Scale down — the research is about relationships, not data volume bragging rights |
| 3 | Spark tuning shows negligible difference | Adjust workload/config range; a null result is still a valid, analyzable finding |
| 4 | Compaction doesn't help | Same — scientifically valid, analyze why |
| 5 | Framework doesn't beat static baseline | Also a valid result — discuss why in Section 7 (Discussion) |
| 6 | Results are noisy | Repeated runs, randomized order, warm-ups, CV threshold, documented environment |
| 7 | Timeline slips | Compressed matrix (§4/§6/§7) has built-in slack; cut repetitions to 3 before cutting phases |

---

## ✅ Definition of Done

By the end of Week 8, you should have all eight of these, not just a PDF:

1. ✅ Research paper (submission-ready, IEEE format)
2. ✅ Experimental framework (Python + PySpark)
3. ✅ Workload generator (seeded, reproducible)
4. ✅ `experiment_results.csv` (raw measurements, every run)
5. ✅ Cost model (published pricing + measured usage, documented assumptions)
6. ✅ Decision framework (rule-based: Nothing / Tune / Compact)
7. ✅ Results (7 figures, break-even curves, H1/H2 verdicts)
8. ✅ Reproducibility package (code + configs + instructions)

---

## ▶️ Immediate Next Step

**Phase 2 — Environment Setup**, when you're ready to start. Say the word and we'll go through it properly: pinned versions, `environment.md`, the AQE fix, and the measurement isolation protocol — built once, correctly, so nothing needs to be redone in Week 5.
