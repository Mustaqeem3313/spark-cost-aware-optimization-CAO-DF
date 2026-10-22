# CAO-DF — Project Master Document
### *A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines*

| | |
|---|---|
| **Researcher** | Mustaqeem Shaikh · M.Sc. Computer Science (Year 2) · Roll No. 9251 |
| **Target venue** | IEEE-sponsored regional conference (Xplore-indexed) — *specific venue and deadline still to be confirmed (see §32)* |
| **Code repository** | https://github.com/Mustaqeem3313/spark-cost-aware-optimization-CAO-DF |
| **Document status** | Living document · snapshot of **20 October 2026** |
| **Supersedes** | The phase checkboxes in `Final_Execution_Plan.md` (those were never updated after Phase 4) |
| **Infrastructure** | Local-first, free tier only · Windows laptop (2 physical / 4 logical cores, 7.9 GB RAM) |

> **How to read this document.** Sections 1–9 define *what* the research is. Sections 10–13 define *how* it will be tested. Sections 14–16 describe *what has been built and learned so far*. Sections 17–20 give the *plan and next steps*. Sections 21–25 cover *data, analysis and paper writing*. Sections 26–32 cover risks, reproducibility, decisions and open questions. Appendices hold the reference list, CSV schema, command cheat-sheet and result tables.

---

## Table of Contents

1. [Project at a Glance](#1-project-at-a-glance)
2. [Research Topic](#2-research-topic)
3. [Background and Motivation](#3-background-and-motivation)
4. [Research Problem](#4-research-problem)
5. [Research Gap](#5-research-gap)
6. [Scope](#6-scope)
7. [Objectives](#7-objectives)
8. [Research Questions](#8-research-questions)
9. [Hypotheses](#9-hypotheses)
10. [Research Proposal](#10-research-proposal)
11. [Literature Review](#11-literature-review)
12. [Experimentation Model](#12-experimentation-model)
13. [Experimentation Design](#13-experimentation-design)
14. [Research Implementation](#14-research-implementation)
15. [Complete Code Overview](#15-complete-code-overview)
16. [What We Have Done Till Now](#16-what-we-have-done-till-now)
17. [Complete Execution Plan](#17-complete-execution-plan)
18. [Phase Completion Status](#18-phase-completion-status)
19. [Research Plan and Timeline](#19-research-plan-and-timeline)
20. [Next Steps](#20-next-steps)
21. [Data Collection Strategy](#21-data-collection-strategy)
22. [Data Analysis Plan](#22-data-analysis-plan)
23. [Paper Writing Plan](#23-paper-writing-plan)
24. [Experimental Paper Format](#24-experimental-paper-format)
25. [IEEE Paper Format and Template](#25-ieee-paper-format-and-template)
26. [Risks, Threats to Validity and Limitations](#26-risks-threats-to-validity-and-limitations)
27. [Reproducibility and Artifacts](#27-reproducibility-and-artifacts)
28. [Decision Log](#28-decision-log)
29. [Troubleshooting Reference](#29-troubleshooting-reference)
30. [Guide Meeting Q&A](#30-guide-meeting-qa)
31. [Glossary](#31-glossary)
32. [Open Questions to Confirm](#32-open-questions-to-confirm)
- [Appendix A — Reference List (IEEE)](#appendix-a--reference-list-ieee)
- [Appendix B — Results CSV Schema](#appendix-b--results-csv-schema)
- [Appendix C — Command Cheat-Sheet](#appendix-c--command-cheat-sheet)
- [Appendix D — Workload Catalogue](#appendix-d--workload-catalogue)
- [Appendix E — Pilot and Sweep Result Tables](#appendix-e--pilot-and-sweep-result-tables)

---

## 1. Project at a Glance

### 1.1 One-paragraph summary

Optimizing a Spark pipeline is not free: tuning shuffle partitions or compacting small files itself consumes compute, so an optimization that speeds up one run is not automatically worth its cost over the life of a recurring pipeline. This project builds and experimentally evaluates a **lightweight, interpretable, workload-aware decision framework** that, given a workload's characteristics and its expected number of future executions, decides between three outcomes — **do nothing, tune shuffle partitions (A1), or compact files (A2)** — using an explicit total-cost model and an **Optimization Break-Even Point (OBEP)**.

### 1.2 What this project is — and is not

| It **is** | It is **not** |
|---|---|
| A controlled empirical study of *when* two existing optimizations pay off | A new Spark tuning algorithm |
| A simple, explicit cost-benefit decision rule (no ML) | A production system or a multi-tool cloud pipeline |
| An evaluation of an *execution-level* action and a *storage-level* action under one cost objective | A competitor to PTO (SIGMOD 2026) or AutoComp (SIGMOD-Companion 2025) |
| Local, reproducible, and honest about estimated (not billed) cost | A claim about distributed-cluster behaviour |

### 1.3 Elevator pitch for your guide

> *"I'm not making Spark faster. I'm researching whether making it faster — or compacting its data — is worth the cost for a given workload."*

### 1.4 Status dashboard (22 Oct 2026)

| Metric | Value |
|---|---|
| Total phases | **12** (Phase 0 – Phase 11) |
| Completed | **8** (Phases 0 – 7) |
| In progress | **0** — Phase 8 (cost model) is next |
| Not started | **4** (Phases 8, 9, 10, 11) |
| Overall progress | `████████░░░░` **8 / 12 complete (67 %)** |
| Experimental data collected | **Formal A1** (E3, 160 runs, tag `a1-formal-v2`) · **Formal A2** (E5, 90 runs, tag `a2-formal-v2`) · A2 pilot (E4, 54 runs) |
| Schedule | Re-baselined in §19.2; college deadline **15 Nov 2026** |

> **Status note.** Formal results now exist for both actions. Findings, anomalies, limitations and decisions are recorded in `docs/research_log.md`, which is the authoritative record for the Results and Threats-to-Validity sections. Numbers in §16.5–16.6 and Appendix E remain pilot/exploratory evidence.

---

## 2. Research Topic

**Topic.** Cost-Aware Decision-Making for Adaptive Optimization of Apache Spark Data Pipelines.

**Paper title.** *A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines.*

**Research domain.** Cloud data engineering · Big-data processing · Apache Spark · Cloud cost optimization.

**Index terms (IEEE style).** Apache Spark, cost-aware optimization, break-even analysis, shuffle partitions, file compaction, data pipelines, lakehouse, workload characterization.

**How the topic was chosen (short history).**

1. The goal: a publishable IEEE paper in 1–2 months, matching the researcher's profession (data engineering: Python, SQL, ETL, Spark, Databricks, AWS/Azure) and a free-tier-only budget.
2. A realistic venue path was chosen: an **IEEE-sponsored regional conference** (Xplore-indexed after review), not IEEE Access or a Transactions journal.
3. A first framework (framework PDF) proposed four actions (A0–A3), three hypotheses and ten workload characteristics.
4. A literature search found three close neighbours — **PTO**, **AutoComp** and **Tuneful / "To Tune or Not to Tune?"** — so novelty, not plagiarism, was the real risk. The scope was deliberately narrowed (§6.3).
5. The final proposal (29 Aug 2026) fixed two actions, three research questions and two hypotheses.

---

## 3. Background and Motivation

**Spark in one paragraph.** A Spark program runs as a *driver* plus *executors*. An action triggers a *job*, split into *stages* at every shuffle; each stage runs one *task* per partition. A **shuffle** redistributes data across partitions for joins and aggregations (write → transfer → read) and is typically the dominant cost of a pipeline.

**Why shuffle partitions matter.** `spark.sql.shuffle.partitions` (default **200**) sets how many partitions a shuffle produces. Too few → under-used parallelism (and, at larger scale, memory pressure and spill). Too many → per-task scheduling overhead dominates. The best value is workload-dependent, which is why a fixed default is often wrong.

**Why file layout matters.** Parquet is columnar and splittable, but many small files multiply fixed costs (listing, opening, scheduling a task per file). **Compaction** rewrites many small files into fewer large ones — at the one-time cost of the rewrite job.

**Why cost matters.** Both actions are themselves Spark work. Whether they pay back depends on their cost (`Copt`), the per-run saving, and **how many times the pipeline will run again (`N`)**. This is the decision the project studies.

**Adaptive Query Execution (AQE).** Spark 3.x can coalesce shuffle partitions at runtime and so silently override a manual setting. AQE is therefore **disabled** for the A1 study and the interaction is left as future work (§26).

*Full technical primer: `Phase0_Technical_Understanding.md`.*

---

## 4. Research Problem

Cloud-based Apache Spark pipelines offer many optimization mechanisms — configuration tuning, adaptive execution, compaction, partitioning, data-layout optimization — and recent research and commercial platforms automate individual mechanisms. However, **applying an optimization consumes resources and cost**. An action that improves one execution may not minimize the cost of operating the pipeline over its lifetime: compaction can speed up later reads, but its own cost must be recovered; a shuffle-partition change helps only when enough data is shuffled and the pipeline runs often enough.

Therefore there is a need for a **workload-aware, cost-sensitive decision framework** that decides *whether* an optimization should be applied, based on its expected benefit, its overhead, and the expected number of future executions.

**Problem statement (formal).** Given a recurring Spark pipeline with workload characteristics *w* (data volume, file fragmentation, shuffle volume), an expected number of future executions *N*, and candidate actions *a ∈ {none, A1, A2}*, choose the action that minimizes expected total cost

```
TC(a, N) = Copt(a) + N × Crun(a)         with  TC(none, N) = N × Crun_static
```

and identify the workload conditions and execution frequencies under which each action becomes economically justified.

---

## 5. Research Gap

**What exists.**

- Spark execution tuning is mature (Section 11, theme A). It treats tuning overhead as a *search-efficiency* cost to be minimized, not as a term in a break-even decision.
- Workload characteristics are an accepted signal for optimization (theme B) — but used to speed up a performance search, not to decide whether to optimize at all.
- Compaction and layout optimization have strong recent systems — **AutoComp** and **PTO** — using production-scale tooling and, for PTO, machine-learning predictors (themes C, D).
- The **break-even / cost-amortization principle** has been shown valid for general configuration tuning (**Tuneful**, KDD '20) and, in an adjacent domain, for hardware-accelerator offload decisions (an unpublished 2026 preprint) (theme E).

**What is missing (the focused gap).** A **controlled, empirical evaluation** that applies the break-even, cost-amortization principle **jointly to two different categories of Spark optimization action** — an *execution-level* action (shuffle-partition tuning) and a *storage-level* action (file compaction) — under **one explicit, interpretable decision rule**, and that determines the workload conditions and execution frequencies at which each becomes economically justified relative to a static baseline.

**How this differs from the closest work.**

| Work | What it does | How this project differs |
|---|---|---|
| PTO (SIGMOD 2026) | ML predictor of layout-optimization benefit at lakehouse catalog scale | Simple explicit cost rule, small controlled experiment, *two* action types |
| AutoComp (SIGMOD-Companion 2025) | Production compaction framework balancing benefit and cost | Compaction is one of two controlled actions, not the system under study |
| Tuneful / KDD '20 | Break-even idea for general configuration tuning | Applies the idea to a specific execution action *and* a storage action together |
| OptEx (CCGrid 2016) | Closed-form cost model for cluster sizing under a deadline | Decides *whether to optimize* over recurring runs, not cluster size |

> **Positioning rule.** Never claim that Spark tuning, workload-aware optimization, adaptive layout or cloud cost optimization are new. The contribution is the *focused empirical decision study*.

---

## 6. Scope

### 6.1 In scope

| Element | Definition |
|---|---|
| **Actions** | **A1** — Spark shuffle/partition tuning (`spark.sql.shuffle.partitions`); **A2** — file compaction (rewrite many small Parquet files into fewer, larger files) |
| **Baseline** | Static configuration: default 200 shuffle partitions, no compaction |
| **Workload factors** | Data volume · file count / fragmentation · shuffle volume · execution frequency |
| **Outputs** | Cost-benefit model · break-even points (OBEP) · rule-based decision framework `{Nothing \| Tune \| Compact}` |
| **Environment** | Local single-machine PySpark (`local[*]`), synthetic seeded data, *estimated* cloud cost from published pricing |

### 6.2 Out of scope (do not add)

Kafka · Airflow · Databricks · Kubernetes · autoscaling · data-skew handling · scheduling · ML/RL/LLM-based decision-making · multi-node cluster experiments · real billed cloud spend.

> **Clarification on caching.** Caching is *not* an optimization action in this study. It is used only as an **experimental-control device** in A1 so that timed runs measure compute/shuffle cost rather than repeated Parquet I/O.

### 6.3 How the scope evolved

| Aspect | Framework PDF (first draft) | Final proposal (29 Aug 2026) | Why |
|---|---|---|---|
| Actions | A0 none, A1 Spark exec., A2 layout, A3 joint | A1 + A2 only (joint arm deliberately excluded) | Keep the study controlled and finishable |
| Research questions | RQ1–RQ3 (broad) | RQ1–RQ3 (narrowed, two actions) | Match the narrowed scope |
| Hypotheses | H1, H2, H3 | H1, H2 | H3 was dropped in the final proposal; its idea survives only as the optional performance-only comparator |
| Workload characteristics | 10 (skew, join cardinality, selectivity…) | 4 (volume, fragmentation, shuffle volume, frequency) | Free-tier, 8-week feasibility |
| Baselines | Static, performance-only, cost-only, independent, joint | Static (primary); performance-only as an optional extra | Fewer arms, same central claim |

### 6.4 Design-imposed limits (state plainly in the paper)

Single machine, not a distributed cluster · synthetic data · estimated, not billed, cost · AQE disabled · small data scale (≤ 2 M rows) · Windows laptop with 7.9 GB RAM.

---

## 7. Objectives

**Primary objective.** Develop and experimentally evaluate a cost-aware decision framework that determines when Spark shuffle/partition tuning or file compaction is economically justified for a given workload.

**Specific objectives.**

| # | Objective | Delivered by |
|---|---|---|
| O1 | Identify a small set of measurable workload characteristics that influence the effectiveness of A1 and A2 | Workload generator + pilot/formal experiments |
| O2 | Measure execution time, resource usage and optimization overhead across workload conditions | `instrumentation.py` + experiment runners |
| O3 | Develop a simple cost-benefit decision model for choosing whether to apply an action | `cost_model.py`, `decision_framework.py` |
| O4 | Determine the workload conditions and execution frequencies at which each action becomes worthwhile | Break-even analysis (Phase 8) |
| O5 | Compare the cost-aware approach with a static Spark configuration | Framework validation (Phase 9) |

**Supporting (engineering and rigor) objectives.**

| # | Objective |
|---|---|
| S1 | Make every result reproducible: seeded data, pinned versions, documented hardware, versioned pipeline, public repository |
| S2 | Control confounds: AQE off, broadcast joins off, randomized block order, session warm-up, cache control, resource pre-flight |
| S3 | Report variability honestly: median/IQR/CV, no silent outlier removal, no manufactured significance |

---

## 8. Research Questions

| ID | Question | Primary evidence |
|---|---|---|
| **RQ1** | How do data volume, file fragmentation, shuffle volume and execution frequency affect the cost and performance benefits of Spark shuffle/partition tuning and file compaction? | A1 and A2 formal matrices; performance and cost plots |
| **RQ2** | Can a workload-aware cost-benefit framework reduce total workload cost compared with a static Spark pipeline configuration? | Framework validation on unseen workloads (Static vs Framework) |
| **RQ3** | Under what workload conditions does applying shuffle/partition tuning or file compaction become economically justified? | Break-even table and curves (OBEP) |

---

## 9. Hypotheses

| ID | Hypothesis |
|---|---|
| **H1** | The cost-performance benefits of Spark shuffle/partition tuning and file compaction vary significantly with workload characteristics. |
| **H2** | A workload-aware cost-benefit decision framework reduces total workload cost compared with a static configuration by avoiding actions whose expected savings do not justify their overhead. |

**Operational definitions (proposed — confirm before the formal run so the criteria are fixed in advance).**

| Hypothesis | Proposed "supported" criterion |
|---|---|
| **H1 (A1)** | The best partition count and/or the speed-up over the default differ between workloads by more than measurement noise — e.g. non-overlapping 95 % bootstrap CIs of speed-up for at least one workload pair, with an effect of ≥ 15 % |
| **H1 (A2)** | Compaction benefit changes monotonically with the number of files (low → medium → high fragmentation), with the high-fragmentation effect outside the noise band |
| **H2** | Over a set of *unseen* validation workloads and N ∈ {1, 5, 10, 50}, the framework's total estimated cost is ≤ static in a clear majority of cases and lower on average, with a paired-bootstrap CI for the mean difference that excludes zero |

> **Statistical honesty note.** With 5 repetition blocks, an exact two-sided Wilcoxon signed-rank test cannot go below *p* = 0.0625 (2 / 2⁵). Formal significance tests therefore need ≥ 6 blocks (min *p* = 0.031) — or, more defensibly, the analysis relies on **effect sizes and bootstrap confidence intervals** and says so. Either outcome (supported / not supported) is a legitimate, publishable result.

---

## 10. Research Proposal

*(Condensed from the formal proposal submitted 29 August 2026; scope as finalized.)*

| Field | Content |
|---|---|
| **Title** | A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines |
| **Domain** | Cloud data engineering, big-data processing, Apache Spark, cloud cost optimization |
| **Problem** | Optimization actions consume resources; an action that improves one run may not be economical over a pipeline's lifetime (§4) |
| **Aim** | Develop and experimentally evaluate a cost-aware decision framework that determines when shuffle/partition tuning or file compaction is economically justified |
| **Objectives / RQs / Hypotheses** | §7, §8, §9 |
| **Method (summary)** | Controlled local experiments on seeded synthetic workloads. Measure runtime, resource use, shuffle volume and optimization overhead for a static baseline and two actions; build a simple total-cost / break-even model; evaluate a rule-based decision framework against the static baseline on unseen workloads |
| **Workload design** | Data volume (small/large), file fragmentation (1/10/50 files), shuffle volume (low/high filter selectivity), execution frequency (N = 1/5/10/50, modelled analytically) |
| **Baseline** | Static Spark configuration (200 shuffle partitions, no compaction); an optional performance-only comparator |
| **Measurements** | Execution time, executor run/CPU time, shuffle read/write bytes, spill, GC time, file count/size distribution, optimization time/overhead, estimated cost per execution, total cost across frequencies |
| **Analysis** | Descriptive statistics, effect sizes, bootstrap CIs, workload-vs-performance plots, cost comparisons, break-even curves; significance tests only where run counts support them |
| **Expected contributions** | (1) simple interpretable decision framework for two actions; (2) empirical analysis of how four workload factors affect optimization value; (3) quantitative break-even analysis; (4) practical guidance on when to apply or skip an optimization |
| **Significance** | Gives practitioners an explicit rule for "is this optimization worth running?" instead of treating tuning and compaction as always-on |
| **Ethics** | Synthetic data only; no human subjects, personal data or proprietary data |
| **Resources** | Local laptop, open-source stack (PySpark 3.5.1, sparkMeasure 0.28), published cloud pricing for cost estimation; no paid cloud spend |
| **Limitations (declared up front)** | Single machine, synthetic data, estimated cost, AQE disabled, limited scale |

---

## 11. Literature Review

**Deliverables.** `Literature_Matrix.xlsx` (29 papers, 6 themes) · `Literature_Review_Shaikh.docx` (prose, IEEE-cited, dated 29 Aug 2026; covers [1]–[24]) · full IEEE reference list in [Appendix A](#appendix-a--reference-list-ieee).

> ✅ **Spreadsheet metadata corrected (22 Oct 2026).** The Micky [22], PARIS [23], Smartpick [24] and cross-layer [20] rows now match the verified Appendix A list. Five further papers [25]–[29] were added after Crossref DOI verification. [27] and [28] are recorded at title/metadata level only: read the abstracts before citing them for anything beyond their existence.

### 11.1 Theme A — Spark configuration and execution tuning [1]–[7]

Automatic Spark tuning is a large field: ML performance-influence models [1], tuning from a single profiling run [2], low-overhead online tuning (LOCAT) [3], a generalized online formulation with multiple goals and constraints [4], stage-level fine-grained tuning integrated with AQE [5], and overhead-versus-flexibility trade-offs (LOFTune) [6]. A survey [7] explicitly names cost-aware, workload-adaptive tuning as an open direction.
**Common limitation:** the objective is execution performance; tuning overhead is a *search-efficiency* cost to minimize, not a term in an economic trade-off against future savings. None of these ask whether tuning a given pipeline is worthwhile given how often it will run.

### 11.2 Theme B — Workload-aware optimization [8]–[11]

Workload characteristics are an accepted and effective signal: transfer learning across workloads (Rover) [8], runtime-statistics-driven AQE for lakehouses [9], large-scale cloud workload characterization [10], learning-based parameter recommendation from workload features [11]. **Limitation:** workload-awareness accelerates a performance search; it does not decide whether an optimization is justified.

### 11.3 Theme C — Data compaction and small files [12]–[14], [27]–[29]

**AutoComp** [12] (LinkedIn) is production compaction automation that explicitly balances compaction benefit and cost — the closest prior work on compaction economics. Practitioner sources [13] (Iceberg on EMR) and [14] (Delta Lake `OPTIMIZE` bin-packing) describe mechanisms and best practice without controlled quantitative evaluation of *when* compaction pays off. LST-Bench [29] benchmarks log-structured tables over time, including maintenance operations, but measures rather than decides. Recent work learns compaction policies with reinforcement learning [27] or improves *how* small files are merged in HDFS [28]; neither offers an interpretable rule for *whether* compaction pays off over N future runs.

### 11.4 Theme D — Table and data-layout optimization [15]–[16], [25]–[26]

**PTO** [15] (SIGMOD 2026) is the single closest related work: an ML-driven, workload-driven predictor of whether a layout optimization (partitioning, target file size, Z-order…) will help *before* it is applied. It operates at production-catalog scale with learned cost models. Practitioner discussion of sort vs bin-pack compaction [16] shows multiple strategies exist but stays qualitative. **AutoLiquid** [25] (Databricks, PVLDB 2026) autonomously changes clustering keys from workload telemetry with a verify-before-commit step, and **LayoutPilot** [26] (PVLDB 2026 demo) is an interactive what-if advisor for lakehouse layouts. Both confirm the industry trend toward workload-driven layout decisions; neither expresses the decision as a break-even over future executions.

### 11.5 Theme E — Optimization cost and break-even analysis [17]–[20]

The conceptual foundation of this study. **Fekry et al.** [17], [18] show empirically that tuning cost may need to be weighed against the number of future executions and propose a break-even perspective. **OptEx** [19] is a closed-form Spark cost/time model for cost-optimal cluster composition under a deadline (single job, not recurring-optimization decisions). A 2026 single-author arXiv preprint [20] derives a measurement-driven break-even point for hardware-accelerator offload in database systems — *unpublished and not peer-reviewed*, to be cited only as evidence that the break-even mechanism is being applied across adjacent domains.

### 11.6 Theme F — Cost-aware cloud resource and analytics optimization [21]–[24]

CherryPick [21] (Bayesian optimization of cloud VM configurations), Micky [22] (one configuration near-optimal for a group of workloads), PARIS [23] (cross-cloud VM selection) and Smartpick [24] (serverless vs VM selection on Spark). All decide *infrastructure/resource selection*; none decide whether to apply a *pipeline-level optimization action* to an already-provisioned Spark pipeline. The layers are complementary.

### 11.7 Literature matrix at a glance

| Ref | Work | Venue / year | Theme | Role in this study |
|---|---|---|---|---|
| [1] | Nguyen et al. | IEEE CLOUD 2018 | A | Early Spark config tuning |
| [2] | Buchaca Prats et al. | IEEE TNSM 2020 | A | Tuning from one run |
| [3] | LOCAT (Xin et al.) | SIGMOD 2022 | A | Low-overhead online tuning |
| [4] | Li et al. | VLDB 2023 | A | General online tuning |
| [5] | Lyu et al. | VLDB 2024 | A | Fine-grained, AQE-integrated |
| [6] | LOFTune (Li et al.) | IEEE TKDE 2025 | A | Overhead vs flexibility |
| [7] | Herodotou et al. | ACM CSUR 2020 | A | Survey; names the open direction |
| [8] | Rover (Shen et al.) | KDD 2023 | B | Transfer-learned tuning |
| [9] | Xue et al. | VLDB 2024 | B | AQE for lakehouses |
| [10] | Parayil et al. | ICPE 2025 | B | Workload characterization |
| [11] | Bao et al. | IEEE Big Data 2018 | B | Learning-based recommendation |
| [12] | **AutoComp** (Gruenheid et al.) | SIGMOD-Companion 2025 | C | **Closest on compaction economics** |
| [13] | Goswami & Sarkar | AWS blog 2023 | C | Practitioner (grey literature) |
| [14] | Delta Lake docs | 2025 | C | Mechanism reference |
| [15] | **PTO** (Meduri et al.) | PACMMOD / SIGMOD 2026 | D | **Closest overall** — must be differentiated precisely |
| [16] | Gilad | Industry blog 2025 | D | Practitioner (grey literature) |
| [17] | **Fekry et al. ("To Tune or Not to Tune?")** | KDD 2020 | E | **Primary anchor for the break-even idea** |
| [18] | Tuneful (Fekry et al.) | arXiv 2020 | E | Extended version of [17] |
| [19] | OptEx (Sidhanta et al.) | IEEE/ACM CCGrid 2016 | E | Spark cost model precedent |
| [20] | Chang | arXiv 2026 (preprint) | E | Break-even in an adjacent domain — flag as unreviewed |
| [21] | CherryPick | NSDI 2017 | F | Cloud configuration search |
| [22] | Micky (Hsu et al.) | IEEE CLOUD 2018 | F | Cheaper configuration search |
| [23] | PARIS (Yadwadkar et al.) | SoCC 2017 | F | Cross-cloud VM selection |
| [24] | Smartpick (Mohapatra & Oh) | Middleware 2023 | F | Serverless vs VM on Spark |
| [25] | **AutoLiquid** (Nakandala et al.) | PVLDB 2026 | D | Autonomic, telemetry-driven layout (Databricks) |
| [26] | LayoutPilot (Liu et al.) | PVLDB 2026 (demo) | D | Interactive layout advisor |
| [27] | AutoCompactionAI (Venkatesan) | IEEE FMLDS 2025 | C | RL compaction policy — abstract to be read |
| [28] | Chettaoui & Hkiri | J. Supercomput. 2025 | C | Small-file merging in HDFS — abstract to be read |
| [29] | LST-Bench (Camacho-Rodríguez et al.) | PACMMOD / SIGMOD 2024 | C | Benchmark incl. table maintenance |

### 11.8 Synthesis

1. Spark tuning [1]–[7] and layout/compaction optimization [12]–[16] are mature; PTO and AutoComp already consider benefit and, to varying degrees, cost.
2. Workload characteristics are a validated signal for optimization decisions [8]–[11], [15].
3. The break-even / cost-amortization principle is valid in general tuning [17], [18], Spark cost modelling [19] and an adjacent domain [20].
4. **Gap:** a controlled, interpretable, *dual-action* (execution + storage) evaluation under a single break-even rule — the focus of this study (§5).

### 11.9 Literature to-do before submission

- [ ] Re-verify every DOI and page range once more before the final reference list.
- [ ] Limit grey literature ([13], [14], [16]) in the paper; add URL + access date where kept.
- [ ] Keep [20] clearly marked "preprint, not peer-reviewed", or drop it if space is tight.
- [x] Optional: add 3–5 more recent (2025–2026) papers on compaction/layout to strengthen themes C–D — added [25]–[29] (22 Oct 2026).
- [x] Fix the spreadsheet metadata (see note above) — done 22 Oct 2026.
- [ ] Read the abstracts of [27] and [28] before citing them in detail; add [25]–[29] to `Literature_Review_Shaikh.docx`.

---

## 12. Experimentation Model

### 12.1 Conceptual model

```
                         ┌───────────────────────────────┐
   Workload features  ──►│                               │
   (volume, files,       │      COST-BENEFIT MODEL       │──► decision
    shuffle volume)      │                               │      │
                         │  TC(a,N) = Copt(a) + N·Crun(a)│      ▼
   Expected executions──►│  OBEP:  N* = Copt/(Cs − Ca)   │   {Nothing | Tune (A1) | Compact (A2)}
   N                     │                               │
                         └───────────────▲───────────────┘
                                         │ measured Copt, Crun
                              ┌──────────┴──────────┐
                              │  Controlled local   │
                              │  experiments        │
                              │  (A1 matrix, A2     │
                              │   matrix)           │
                              └─────────────────────┘
```

### 12.2 Variables

| Type | Variable | Levels / definition |
|---|---|---|
| **Independent — action** | Optimization action | `none` (static) · **A1** shuffle partitions · **A2** compaction |
| **Independent — A1 setting** | `spark.sql.shuffle.partitions` | **1, 2, 4, 8, 16, 50, 200 (default), 400** *(grid chosen from the exploratory sweep; sized around the 4 logical cores)* |
| **Independent — workload** | Data volume | small = 50 K orders · large = 2 M orders (medium = 500 K reserved for validation) |
| | File count / fragmentation | low = 1 · medium = 10 · high = 50 files |
| | Shuffle volume | low = 15 % of rows survive the filter · high = 85 % |
| | Execution frequency **N** | 1, 5, 10, 50 — **a parameter of the cost model, not physically executed** |
| **Dependent** | Wall-clock time; executor run time (task-seconds); executor CPU time; shuffle bytes written/read; spill; GC time; stage/task counts; `Copt`; estimated cost per run; total cost `TC`; break-even `N*` | via sparkMeasure, timer, psutil |
| **Controlled** | Held constant | AQE off · broadcast joins off (sort-merge join) · Spark 3.5.1 · Python 3.11 · Java 17 · driver 3 GB · `local[*]` · pipeline v2 · data seed 42 · run-order seed 123 · cached inputs (A1) · AC power · RAM pre-flight |
| **Uncontrolled** | Noise sources | OS scheduling, thermal throttling, background processes, Windows file cache — mitigated, not eliminated |

### 12.3 Cost model

```
TC(a, N)       = Copt(a) + N × Crun(a)          # total cost of choosing action a for N future runs
TC(none, N)    = N × Crun_static
Apply a  iff   TC(a, N) < TC(none, N)
OBEP  N*       = Copt(a) / ( Crun_static − Crun(a) )     valid only if Crun_static > Crun(a)
                 (if savings ≤ 0 the action is never worth applying)
```

**Cost proxies (to be fixed in Phase 8 and documented in `Cost_Model_Assumptions.md`).**

| Proxy | Definition | Use |
|---|---|---|
| **P1 — time-based** | `wall_clock_seconds × price of an equivalent instance` | Simple, intuitive |
| **P2 — resource-based** | `executor_run_time / 3600 × price per vCPU-hour` (or per DPU-hour) | Closer to how managed Spark is billed; less sensitive to idle cores |

*Report both and show the decision is robust to the choice.* **Cost is always labelled "estimated from published pricing", never "billed".**

> **Useful property.** If `Copt` and `Crun` are expressed with the *same* proxy, `N*` is independent of the price scalar: prices matter for absolute dollar figures and for comparing different resource types, not for the break-even count. Sensitivity analysis (±50 % price, both proxies) is still required.

**Defining `Copt` per action (working definitions).**

| Action | `Copt` | `Crun(a)` |
|---|---|---|
| **A1** | Cost of the tuning step. Two candidate definitions: (i) *search* — run K candidate settings once each and pick the best (K = 8 in the sweep grid); (ii) *rule* — choose from workload features with ≈ 0 search cost | Per-run cost at the chosen setting |
| **A2** | Cost of the compaction job itself (read fragmented data, write compacted data), measured with sparkMeasure | Per-run pipeline cost after compaction |

### 12.4 Decision framework (Phase 9 design)

```
decide(workload w, expected executions N):
    cost[none] = N * Crun_static(w)
    for a in {A1, A2}:
        (Copt, Crun) = estimate(a, w)          # from the measured cost table / simple rule on features
        cost[a]      = Copt + N * Crun
    return argmin(cost)                          # Nothing | Tune | Compact
```

- **Rule-based, no ML** (explicitly out of scope).
- Open design choice for Phase 9: how `estimate()` generalizes to *unseen* workloads — options in increasing complexity: (a) nearest measured workload, (b) threshold rules on shuffle MB / file count, (c) simple interpolation. **Prefer the simplest interpretable option that validates.**
- Validation: unseen workloads (e.g. medium volume, intermediate levels, different seeds) × N ∈ {1, 5, 10, 50}; compare Static vs Framework (and optionally Performance-only) on total estimated cost.

### 12.5 Methodological safeguards (the "experiment protocol")

| Risk | Safeguard | Where enforced |
|---|---|---|
| AQE silently overrides manual partitions | `spark.sql.adaptive.enabled=false`, read back from Spark at startup | `spark_config.py` |
| Trivial shuffle (v1 pilot finding) | Broadcast joins off; high-cardinality aggregation; **shuffle-scaling gate** | `spark_config.py`, `baseline_pipeline.py` v2, `verify_workloads.py` |
| Settings silently ignored | Effective config read back and compared; abort on mismatch | `spark_config.py` |
| Cold-start bias (JVM/JIT/cache) | Session warm-up pass + one discarded warm-up before every measured run | `run_a1_experiments.py` |
| Systematic drift (thermal/background) | Randomized-block design: fresh random cell order in every block | `run_a1_experiments.py` |
| Disk-I/O noise in A1 | Inputs cached once, untimed | `instrumentation.py` helper |
| Memory pressure / battery | Pre-flight refuses to run below 3.5 GB free RAM or on battery | `instrumentation.py` |
| Silent schema drift in results | Header check on append; archive old files | `instrumentation.py` |
| Untraceable results | Pinned versions, seeded data, `environment.md`, pipeline version column | whole project |
| Noisy results treated as signal | CV / IQR check per cell; outliers disclosed, never silently dropped | `instrumentation.py`, analysis |
| Scope creep | §6.2 boundary | all phases |

---

## 13. Experimentation Design

### 13.1 Experiment catalogue

| ID | Experiment | Purpose | Status |
|---|---|---|---|
| E0 | A1 pilot, pipeline **v1** (60 runs) | First end-to-end test of the runner | ✅ Done — **archived as pilot evidence**; exposed a degenerate shuffle |
| E1 | Shuffle-scaling gate (pipeline v2) | Prove workload factors really change shuffle bytes | ✅ **Passed** on the study machine |
| E2 | Exploratory partition sweep (2 workloads × 8 settings × 3 blocks) | See the shape of the runtime-vs-partitions curve; fix the formal grid | ✅ Done (run under low free RAM; used for guidance only) |
| E3 | **Formal A1 matrix** | RQ1 / RQ3 evidence for shuffle tuning | ✅ Done 21 Oct — 160 runs; tag `a1-formal-v2` |
| E4 | A2 pilot (compaction) | Check compaction benefit is detectable above noise; pilot gate | ✅ Done 21 Oct — gate **PASSED** |
| E5 | **Formal A2 matrix** | RQ1 / RQ3 evidence for compaction | ✅ Done 21 Oct — 90 runs; tag `a2-formal-v2` |
| E6 | Framework validation on unseen workloads | RQ2 / H2 | ☐ |

### 13.2 A1 — shuffle/partition tuning (formal)

| Item | Specification |
|---|---|
| Workloads | `A1_small_low`, `A1_small_high`, `A1_large_low`, `A1_large_high` |
| Settings | `spark.sql.shuffle.partitions` ∈ {1, 2, 4, 8, 16, 50, 200, 400}; **200 always included** (static baseline) |
| Blocks (repetitions) | 5 (consider 7–8 if formal significance tests are wanted — §9 note) |
| Cells | 4 workloads × 8 settings = **32** |
| Measured runs | 32 × 5 = **160** |
| Non-recorded executions | 32 (session warm-up) + 160 (per-run warm-ups) = 192 → **352 executions in total** |
| Estimated wall time | ≈ 15–25 min on the study machine *(estimate from pilot timings, not measured)* |
| Command | `python src/run_a1_experiments.py --partitions 1 2 4 8 16 50 200 400` |
| Outputs | `results/raw/experiment_results.csv` (append-only) · `results/processed/a1_cell_summary.csv` · `results/processed/a1_best_config_per_workload.csv` |

**Per-run protocol.**

1. Pre-flight: free RAM ≥ 3.5 GB, AC power (else abort).
2. One shared SparkSession (AQE off, broadcast off, effective config verified).
3. Cache each workload once (untimed `count()`).
4. **Session warm-up:** execute every cell once, untimed, random order.
5. For each block 1…5: shuffle the cell order; for each cell set `spark.sql.shuffle.partitions`, run one discarded warm-up, then one measured run (sparkMeasure + timer + psutil sampler).
6. Append the measured row; print per-cell median, IQR, CV and best setting by wall-clock and by executor time.

**Why the grid changed from 3 to 8 settings.** The pilot (v1) tested only {50, 200, 400} — all on the "too many partitions" side. The v2 sweep showed the optimum depends on the workload (about 1–2 for small data, about 4 for large data on this 4-thread machine), so the formal grid spans under-parallel (1, 2), near-core-count (4, 8), moderate (16, 50), default (200) and high (400).

### 13.3 A2 — file compaction (built and run in Phase 7)

*As run: shuffle partitions held at the static 200; compaction implemented with `coalesce`; run-order seed 456; no extra 500-file level (pilot gate passed). See `docs/research_log.md` §3.*

| Item | Specification |
|---|---|
| Workloads | `A2_{small,large}_{low,medium,high}` (1 / 10 / 50 files; shuffle level fixed *high*) |
| Compaction action | Read the fragmented `orders` Parquet files and rewrite to `ceil(total_size / 128 MB)` files (= **1 file** at current sizes) in a *new* path |
| Per block (6 workloads × 5 blocks) | (1) measure pipeline on the **fragmented** data (**before**) → (2) run compaction on a fresh copy and measure it (**`Copt`**) → (3) measure pipeline on the **compacted** data (**after**) |
| Measured executions | 6 workloads × 5 blocks × 3 = **90** (+ warm-ups) |
| **No `.cache()`** | Unlike A1, A2 measures read-side overhead (listing/opening files, per-file tasks), so inputs must be read from Parquet each time; the OS file cache is addressed by warm-up and acknowledged as a limitation |
| Fixed settings | Shuffle partitions held constant (document the value chosen after the A1 result); AQE off; broadcast off |

**A2 design risks (check in the A2 pilot).**

- At ≤ 50 files and ≤ 28 MB on a local SSD/HDD, per-file overhead may be **below the noise floor**. If the pilot gate fails, add an extreme fragmentation level (e.g. 500 files) rather than weakening the claim. A null result is still publishable if analysed honestly.
- Windows Parquet writes require `winutils.exe`/`HADOOP_HOME` (already working — Phase 4 wrote all datasets).

### 13.4 Pilot gate (pre-registered)

> Pilot passes if the largest vs smallest shuffle-partition configuration shows a **consistent-direction runtime difference > 15 %, replicated across all repetitions**, **and** the largest vs smallest fragmentation level shows a similarly consistent compaction benefit. If CV > 15–20 % → stop and investigate noise sources before scaling up.

| Half of the gate | Status |
|---|---|
| A1 (v1 pilot) | Direction consistent in all pairs; one pair at 14.5 % (cold-start run) — superseded by v2 |
| A1 (v2 sweep) | Differences far exceed 15 % for both workloads; formal run will re-confirm |
| A2 | **Pending** (E4) |

### 13.5 Quality gates

| Gate | Tool | Rule |
|---|---|---|
| G1 Environment | `verify_phase2_setup.py` | 5/5 checks pass |
| G2 Shuffle scaling | `verify_workloads.py` | Data volume ≥ 5×, shuffle level ≥ 2×, largest workload ≥ 1 MB shuffled |
| G3 Resources | `preflight_resource_check()` | ≥ 3.5 GB free RAM; AC power |
| G4 Config | `get_spark_session()` read-back | Effective settings equal requested |
| G5 Schema | `append_results()` | Header must match; otherwise archive and restart |
| G6 Noise | `check_measurement_quality()` | CV ≤ 20 % per cell, else flag `[NOISY]` and investigate (report IQR as well — CV with n = 5 is fragile to one outlier) |
| G7 Pilot gate | §13.4 | Before scaling any matrix |

---

## 14. Research Implementation

### 14.1 Environment (documented in `environment.md`)

| Component | Version / value | Note |
|---|---|---|
| OS | Windows | Native Windows, not WSL |
| CPU | Intel64 Family 6 Model 69 · **2 physical / 4 logical cores** · max 2601 MHz | Effective parallelism measured ≈ 2.4× |
| RAM | **7.9 GB** | The binding constraint (see §26) |
| Disk type | *to be confirmed manually in `environment.md`* | Not auto-detectable |
| Python | **3.11** | Python 3.13 is **incompatible** with PySpark 3.5.1 on Windows |
| Java | Temurin **17** (17.0.20.1) | |
| Spark | **PySpark 3.5.1**, `local[*]` | Executors run inside the driver JVM |
| sparkMeasure | Python **0.28.0** + Maven `ch.cern.sparkmeasure:spark-measure_2.12:0.28` | `0.24` never existed on Maven Central |
| Other pins | psutil 7.0.0 · pandas 2.2.2 · pyarrow 16.1.0 · matplotlib 3.9.0 · seaborn 0.13.2 | `requirements.txt` |
| Windows extras | `winutils.exe` + `hadoop.dll`, `HADOOP_HOME` set | Required for local Parquet writes |
| Path rule | **No spaces** in the project path | Spaces break PySpark's Windows scripts |

### 14.2 Fixed Spark configuration (single source of truth: `src/spark_config.py`)

| Setting | Value | Reason |
|---|---|---|
| `spark.master` | `local[*]` | Study scope |
| `spark.driver.memory` | **3g** | 4g pushed a 7.9 GB machine to 87–96 % RAM in the pilot |
| `spark.executor.memory` | 3g (documentation only) | No effect in local mode |
| `spark.sql.adaptive.enabled` | **false** | Prevents AQE overriding manual partitions |
| `spark.sql.autoBroadcastJoinThreshold` | **-1** | Forces a sort-merge join so orders are really shuffled |
| `spark.sql.shuffle.partitions` | set per run (default 200) | The A1 variable |
| `spark.pyspark.python` / `PYSPARK_PYTHON` | `sys.executable` (set at import time) | Fixes the Windows Store-alias worker failure |
| Event log | **off** by default | Needs winutils and was never load-bearing |
| Arrow conversion | **off** (workload generation) | Needs JVM module-access flags on Java 9+ |
| sparkMeasure JAR | loaded via `spark.jars.packages` when requested | First run downloads, then cached |

### 14.3 System architecture and data flow

```
                ┌──────────────────────────────┐
                │  workload_generator.py       │  numpy RNG, seed 42
                │  (Phase 4)                   │
                └───────────────┬──────────────┘
                                │ Parquet + manifest.json
                                ▼
          data/generated/<workload_id>/{orders,customers}/
          experiments/configs/workload_index.csv   ◄── lookup table
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│ run_a1_experiments.py   (A2 runner to come)                           │
│   preflight_resource_check ─► get_spark_session (config read-back)    │
│   load_and_cache_workloads ─► session warm-up                         │
│   for block in 1..B:  shuffle(cells)                                  │
│       for (workload, p) in cells:                                     │
│           spark.conf.set(shuffle.partitions, p)                       │
│           run_and_measure( execute_pipeline )                         │
│                 ├─ sparkMeasure StageMetrics (JVM metrics)            │
│                 ├─ perf_counter (wall clock)                          │
│                 └─ psutil sampler thread (CPU %, RAM %)               │
│           append_results ─► results/raw/experiment_results.csv        │
└───────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
       results/processed/*.csv  ──►  cost_model.py ──► decision_framework.py
                                              │
                                              ▼
                              analysis/analysis.ipynb ──► figures, tables ──► paper
```

### 14.4 The baseline pipeline (versioned)

| | **v1** (pilot; archived) | **v2** (current) |
|---|---|---|
| Aggregation key | `region, product_category` → only 20 groups | `customer_id, region, product_category` → ≈ 5 × customers groups |
| Join | Broadcast (customers table small) | **Sort-merge** (broadcast disabled) |
| Shuffle observed | ≈ 9–29 KB, identical for all workloads | **0.19 → 35.4 MB**, scaling with workload |
| Action | `.count()` (lets Spark prune aggregate columns) | **`noop` write sink** (full plan executes, no I/O) |
| Exchange operators | 2 | 2 (both join sides; aggregation reuses the join's hash partitioning) |
| Status | Pilot evidence only — **not comparable** to v2 | Used for all formal experiments; frozen once formal runs start |

Every results row carries a `pipeline_version` column so versions can never be mixed silently.

### 14.5 Engineering principles applied

1. **One source of truth** for session configuration; nobody calls `SparkSession.builder` directly.
2. **Verify the manipulation, not just the mechanism** — shuffle *bytes* must scale, not merely Exchange nodes exist.
3. **Read back, never echo** — effective Spark settings are read from Spark and compared with what was requested.
4. **Append-only, schema-guarded results**; never hand-edit.
5. **Version the pipeline**; archive pilots; never mix versions.
6. **Graceful degradation** where safe (missing sparkMeasure JAR) but **hard failure** where silence would invalidate results (config mismatch, schema mismatch, low RAM).

---

## 15. Complete Code Overview

### 15.1 Repository layout

```
spark-cost-aware-optimization/
├── README.md
├── requirements.txt              # pinned versions
├── environment.md                # hardware/software record (hand-confirm the disk type)
├── Cost_Model_Assumptions.md     # (Phase 8)
├── src/
│   ├── __init__.py
│   ├── spark_config.py           ✅ session builder + fixed settings + read-back
│   ├── baseline_pipeline.py      ✅ pipeline v2 + execute_pipeline + checks
│   ├── workload_generator.py     ✅ seeded synthetic workloads
│   ├── instrumentation.py        ✅ measurement protocol, CSV writer, pre-flight
│   ├── run_a1_experiments.py     ✅ formal A1 runner (E3 done)
│   ├── verify_workloads.py       ✅ shuffle-scaling gate
│   ├── explore_partition_curve.py✅ exploratory sweep
│   ├── environment_report.py     ✅ generates environment.md
│   ├── verify_phase2_setup.py    ✅ 5-check environment verifier
│   ├── compaction.py             ✅ A2 action (coalesce to ceil(size / 128 MB))
│   ├── run_a2_experiments.py     ✅ A2 pilot + formal runner (E4, E5 done)
│   ├── robustness_check.py       ✅ A1 outlier sensitivity check
│   ├── cost_model.py             ☐ (Phase 8)
│   └── decision_framework.py     ☐ (Phase 9)
├── experiments/configs/workload_index.csv     # written by the generator
├── data/{raw,generated}/                       # generated Parquet (git-ignored)
├── results/
│   ├── raw/        experiment_results.csv, pilot_a1_v1_results.csv (archive), pilot_a1_v2_sweep.csv
│   ├── processed/  a1_cell_summary.csv, a1_best_config_per_workload.csv, pilot_a1_v1_best_config.csv
│   └── figures/
├── analysis/analysis.ipynb       ☐ (Phase 10)
├── literature/                   # matrix + review
└── paper/                        # draft + IEEE template
```

> `spark_tuning.py` from the original repository plan is **not needed**: A1 is a runtime configuration change performed inside the A1 runner.
> **Recommended `.gitignore`:** `venv/`, `data/generated/`, `spark-event-logs/`, `derby.log`, `metastore_db/`, `__pycache__/`, `*.pyc`.

### 15.2 Module reference

#### `src/spark_config.py` ✅
*Purpose:* the only place a `SparkSession` is built.
*Public API:* `get_spark_session(app_name, shuffle_partitions=200, adaptive_enabled=False, enable_sparkmeasure=False, enable_event_log=False)` · `check_windows_setup()`.
*Constants:* `DRIVER_MEMORY="3g"`, `EXECUTOR_MEMORY="3g"`, `LOCAL_CORES="*"`, `SHUFFLE_PARTITIONS_DEFAULT=200`, `BROADCAST_JOIN_THRESHOLD=-1`.
*Behaviour:* sets `PYSPARK_PYTHON` before importing pyspark; pre-flight check for `HADOOP_HOME`/`winutils.exe` on Windows; builds the session; **reads effective settings back** and raises if shuffle partitions, AQE or broadcast threshold differ from the request; prints the effective config.

#### `src/baseline_pipeline.py` ✅ (PIPELINE_VERSION = "v2")
*API:* `run_pipeline(spark, orders_df, customers_df)` (filter → join → aggregate → transform) · `execute_pipeline(...)` (runs it to a `noop` sink — **the action every timed run uses**) · `verify_shuffle_plan(df)` (≥ 2 `Exchange`) · `verify_correctness(orders_df, result_df)`.
*Logic frozen once formal runs begin.* Self-test: `python src/baseline_pipeline.py`.

#### `src/workload_generator.py` ✅
*API:* `generate_workload(spark, workload_id, data_volume, file_count_level, shuffle_volume_level, seed=42, output_root)` · `generate_all_workloads(spark, seed=42)`.
*Levels:* data volume small 50 000 / medium 500 000 / large 2 000 000 orders · file count low 1 / medium 10 / high 50 · shuffle volume low 0.15 / high 0.85 (fraction of orders with `amount > 0`).
*Schema:* `orders(order_id, customer_id, product_category, amount, order_date)`, `customers(customer_id, region, segment)`; customers = `max(500, orders // 100)`; 5 categories, 4 regions, 3 segments.
*Determinism:* numpy `default_rng(seed)` on the driver; Arrow disabled for portability. *Note:* all current workloads share seed 42 — use **different seeds** for validation workloads.
*Outputs:* Parquet under `data/generated/<id>/{orders,customers}/` (orders repartitioned into the requested number of files; customers fixed at 2), a per-workload `manifest.json` (requested vs *measured* rows/files/MB/positive-fraction) and `experiments/configs/workload_index.csv`.

#### `src/instrumentation.py` ✅
*API:*
- `preflight_resource_check(min_available_gb=3.5, force=False)` — aborts on low free RAM or battery.
- `load_workload(spark, id)` / `load_and_cache_workloads(spark, ids)` — read via the index; cache + untimed materialize.
- `run_and_measure(spark, action_fn, run_metadata, warmup_runs=1, measured_runs=5, use_sparkmeasure=True)` → list of row dicts. Wraps sparkMeasure `StageMetrics`, a `perf_counter` timer and a background `_ResourceMonitor` (0.2 s system-wide CPU %/RAM %). Falls back gracefully if the JAR is unavailable; removes its listener afterwards. If `run_metadata["repetition_block"]` is given, rows get true repetition numbers and unique `run_id`s.
- `append_results(rows, path, include_warmups=False)` — append-only CSV with **schema guard**.
- `check_measurement_quality(results, cv_threshold=0.20)` → median, IQR, CV, `cv_ok`.
*Schema:* 34 columns ([Appendix B](#appendix-b--results-csv-schema)).

#### `src/run_a1_experiments.py` ✅ (Phase 6 runner, v2 protocol)
*CLI:* `--partitions <ints…>` (default 4 16 50 200 400; 200 always added) · `--blocks 5` · `--force`.
*Behaviour:* §13.2. *Seeds:* run order `RUN_ORDER_SEED=123` (independent of the data seed). *Writes:* raw rows plus `a1_cell_summary.csv` and `a1_best_config_per_workload.csv`.

#### `src/verify_workloads.py` ✅
Runs the pipeline once per A1 workload at 50 partitions and reads shuffle bytes from **Spark's own REST API** (no JAR dependency). Passes only if data volume moves shuffle bytes ≥ 5×, shuffle level ≥ 2×, and the largest workload shuffles ≥ 1 MB. Exit code 0/1.

#### `src/explore_partition_curve.py` ✅
Exploratory sweep (small/large high-shuffle workloads × {1, 2, 4, 8, 16, 50, 200, 400} × 3 blocks) → `results/raw/pilot_a1_v2_sweep.csv`; prints a summary table (median/min/max wall, executor time, spill, shuffle MB).

#### `src/environment_report.py` ✅ · `src/verify_phase2_setup.py` ✅
Generate `environment.md` from real hardware/software and the fixed config constants (writes `environment.generated.md` instead of overwriting a hand-edited file); verify imports, Java, session config, `environment.md` and the pipeline.

#### Planned modules ☐
| Module | Purpose |
|---|---|
| `compaction.py` | A2 action: rewrite fragmented Parquet into target-sized files; measurable as a job |
| `run_a2_experiments.py` | Before / compaction / after measurement loop (no caching) |
| `cost_model.py` | Convert measured resources to estimated cost; `TC(a, N)`; break-even `N*`; sensitivity analysis |
| `decision_framework.py` | `decide(workload, N)` → Nothing / Tune / Compact; Static-vs-Framework validation |
| `analysis/analysis.ipynb` | Descriptive stats, bootstrap CIs, the 7 figures, H1/H2 verdicts |

---

## 16. What We Have Done Till Now

### 16.1 Timeline of work

| Date (2026) | Milestone |
|---|---|
| Aug 2026 (up to 29 Aug) | Topic selection; venue strategy; literature search; novelty assessment vs PTO / AutoComp / Tuneful; scope narrowed; literature matrix (24 papers) and prose review; 17-phase plan; Phase 0 primer |
| **29 Aug** | Formal proposal and 8-week compressed execution plan; literature review dated |
| **29–30 Aug** | **Phase 2 environment verified on the study machine (5/5)** after fixing Python-version, wheel, winutils, path-space and worker-spawn problems |
| **30 Aug** | **Phase 4:** 10 workloads generated on the study machine and integration-checked |
| **12 Sep** | **Phase 5:** instrumentation verified with real sparkMeasure metrics (after the 0.24 → 0.28 fix) |
| **11 Oct** | **Phase 6 pilot (v1):** 60 A1 runs completed in ≈ 7 min — revealed a degenerate shuffle |
| **16 Oct** | **Phase 6 redesign (v2):** shuffle-scaling gate passed; exploratory sweep completed |
| 20 Oct | This master document; environment rebuilt on the official study machine; codebase refactored |
| **21 Oct** | **Phase 6 done:** E2 sweep + **formal A1** (160 runs) · **Phase 7 done:** `compaction.py`, A2 pilot (gate passed), **formal A2** (90 runs) · research log started |
| 22 Oct | Literature matrix corrected and extended to 29 papers; A1 outlier sensitivity check |

### 16.2 Deliverables produced

| Deliverable | Phase | Status |
|---|---|---|
| `Phase0_Technical_Understanding.md` | 0 | ✅ |
| `Literature_Matrix.xlsx`, `Literature_Review_Shaikh.docx` | 1 | ✅ (matrix metadata fixed and extended to 29 papers, 22 Oct 2026) |
| Proposal PDF, framework PDF, execution plans | — | ✅ |
| Verified environment + `environment.md` + `requirements.txt` | 2 | ✅ |
| Pipeline v1 → **v2** | 3 | ✅ |
| Workload generator + 10 workloads + index | 4 | ✅ |
| Instrumentation (34-column schema, sparkMeasure, pre-flight) | 5 | ✅ |
| A1 runner, shuffle-scaling gate, exploratory sweep, **formal A1** | 6 | ✅ (tag `a1-formal-v2`) |
| `compaction.py`, A2 runner, A2 pilot, **formal A2** | 7 | ✅ (tag `a2-formal-v2`) |
| `docs/research_log.md` (findings, limitations, decisions) | 6–7 | ✅ (living document) |
| GitHub repository | — | ✅ https://github.com/Mustaqeem3313/spark-cost-aware-optimization-CAO-DF |

### 16.3 The v1 pilot — what it showed (and why we redesigned)

60 runs (4 workloads × {50, 200, 400} × 5 blocks). Median wall-clock: **50 partitions won every workload** (≈ 0.9–1.7 s) vs 200 (≈ 2.6–2.9 s) vs 400 (≈ 4.3–4.8 s) — see [Appendix E](#appendix-e--pilot-and-sweep-result-tables).

Diagnosis from the sparkMeasure columns:

1. **Runtime tracked task count, not data.** Tasks 57 → 207 → 407; ≈ 9–10 ms per extra task, identical across workloads.
2. **Data volume barely mattered:** 50 K vs 2 M rows took 2.6 s vs 2.9 s at 200 partitions.
3. **The shuffle was tiny and identical:** ≈ 9 / 18 / 29 KB for 50 / 200 / 400 partitions in *every* workload — the "shuffle volume" factor changed nothing.
4. **Cause:** the small customers table was broadcast, and aggregating to 20 groups let map-side partial aggregation collapse the shuffle.
5. **Data-quality issues:** `repetition_number` always 1 and duplicate `run_id`s; a cold-start inflation of block 1; system RAM at 87–96 %.

> **Takeaway.** "Fewer partitions is always faster" was *true but trivial* — the pilot measured per-task scheduling overhead and could never have supported H1. The pilot gate exists to catch exactly this, and it did.

### 16.4 The v2 redesign

Pipeline v2 (§14.4) · broadcast joins off · `noop` sink · shuffle-scaling gate · config read-back · true repetition numbers and unique run IDs · spill / peak-execution-memory columns · schema guard · session warm-up pass · RAM / AC pre-flight · driver memory 3 g · 8-point partition grid.

### 16.5 v2 shuffle-scaling gate (study machine) — **PASSED**

| Workload | Orders | Shuffle written |
|---|---:|---:|
| A1_small_low | 50 000 | 0.19 MB |
| A1_small_high | 50 000 | 0.85 MB |
| A1_large_low | 2 000 000 | 6.45 MB |
| A1_large_high | 2 000 000 | 35.39 MB |

Large/small = 33.8× (low) and 41.5× (high); high/low = 4.5× (small) and 5.5× (large); largest workload 35.4 MB (≈ 1 000× the v1 shuffle).

### 16.6 v2 exploratory sweep — what it suggests

*(Run with `--force` at ≈ 2.3 GB free RAM — treat as guidance, not results.)*

- **The optimum depends on the workload:** small data ≈ 1–2 partitions (0.20–0.22 s); large data ≈ 4 partitions (1.34 s).
- **Too few hurts on large data:** 1 partition = 2.14 s vs 1.34 s at 4 (lost parallelism on 4 threads).
- **The default (200) is far from best** — ≈ 12× slower than the best on small data; 3–7× slower on large data (the 200-partition large cell is unreliable: 11.5 s, 9.5 s, 3.7 s).
- **No spill (0.0 MB) anywhere** — the "too few partitions → spill" mechanism from the primer does not occur at this scale; the penalty here is lost parallelism. State this in the paper.
- **Noise was high in several cells** (e.g. 8 partitions: 1.4–4.5 s), consistent with low free RAM.
- **Back-of-envelope economics (not a result):** trying all 8 settings once ≈ 27 s (large) / ≈ 8 s (small) vs a per-run saving of ≈ 8 s / ≈ 2.2 s ⇒ break-even after only ≈ 3–4 runs.

### 16.7 Problems found and permanently fixed

| # | Problem | Fix |
|---|---|---|
| 1 | `pandas==2.2.2` built from source on Python 3.13 → pip aborted entirely | Use **Python 3.11** (also required by PySpark 3.5.1) |
| 2 | `winutils.exe` / `HADOOP_HOME` missing → Parquet writes crashed | Install winutils; event log made opt-in; clear pre-flight message |
| 3 | `'cost' is not recognized` — spaces in the project path | Project path must contain **no spaces** |
| 4 | "Python worker failed to connect back" (Microsoft Store alias) | `PYSPARK_PYTHON = sys.executable` **at import time** |
| 5 | Arrow `createDataFrame` failed on Java 17 | Arrow disabled for data generation |
| 6 | sparkMeasure "jar not loaded" — pinned version `0.24` never existed; session built without the JAR flag | Correct to **0.28**; improved warning; flag passed |
| 7 | sparkMeasure listeners accumulating over many calls | `remove_listener()` after each call |
| 8 | `repetition_number` always 1, duplicate `run_id`s | `repetition_block` passed through; unique IDs |
| 9 | Degenerate shuffle (v1) | Pipeline v2 + broadcast off + `noop` sink + gate |
| 10 | Config patch silently not applied (broadcast threshold) | **Read-back verification** of effective settings |
| 11 | Cold-start inflation of block 1 | Session warm-up pass |
| 12 | Memory pressure (87–96 % RAM) | Driver 3 g + RAM/AC pre-flight |
| 13 | Editor auto-renamed an import to `baseline_pipeline_v1` | Imports must read `from src.baseline_pipeline import …`; keep archives outside `src/` |

### 16.8 Lessons learned

1. A pilot is a **design test**, not a dry run — keep it, archive it, cite it.
2. Check that an experimental factor actually **moves the measured quantity** (shuffle bytes), not just that the mechanism exists.
3. Never trust a printed variable as proof of a setting — **read it back**.
4. The binding constraint on a 7.9 GB machine is **free RAM**; enforce isolation in code.
5. Windows + PySpark has a known set of rough edges (§29); each is now closed in code.

---

## 17. Complete Execution Plan

### 17.1 Phase map (12 phases, 0 – 11)

| Phase | Name | Goal | Key deliverable(s) | Status |
|---|---|---|---|---|
| **0** | Research preparation | Understand Spark, shuffle, Parquet, compaction, metrics, cost, break-even, experimental design | `Phase0_Technical_Understanding.md` | ✅ **Done** |
| **1** | Literature review | 20–30 useful papers in six themes; identify the gap | `Literature_Matrix.xlsx`, `Literature_Review_Shaikh.docx` | ✅ **Done** |
| **2** | Environment setup | Reproducible, documented, confound-free local Spark | `requirements.txt`, `environment.md`, verifier | ✅ **Done** |
| **3** | Baseline pipeline | One reproducible pipeline that provably shuffles and is correct | `baseline_pipeline.py` (**v2**) | ✅ **Done** |
| **4** | Workload generator | Deterministic, parameterized datasets | `workload_generator.py`, 10 workloads, index | ✅ **Done** |
| **5** | Instrumentation | Real Spark metrics, warm-up/repetition protocol, results CSV | `instrumentation.py` | ✅ **Done** |
| — | **Pilot gate** | Check effects exceed noise before scaling | Pre-registered criterion (§13.4) | ✅ A1 and A2 passed |
| **6** | A1 — shuffle/partition tuning | Measure cost-performance of `spark.sql.shuffle.partitions` | Formal A1 rows + best-config table | ✅ **Done** (21 Oct) |
| **7** | A2 — file compaction | Measure compaction cost (`Copt`) and benefit | A2 rows + compaction cost/benefit table | ✅ **Done** (21 Oct) |
| **8** | Cost model + break-even | Convert measurements to estimated cost; compute OBEP | `cost_model.py`, `Cost_Model_Assumptions.md`, break-even table | ☐ Not started |
| **9** | Decision framework | Rule-based {Nothing \| Tune \| Compact}; validate vs static | `decision_framework.py`, validation table | ☐ Not started |
| **10** | Analysis, figures, hypotheses | Statistics, 7 figures, H1/H2 verdicts | `analysis/analysis.ipynb`, figures | ☐ Not started |
| **11** | Paper, revision, submission | IEEE paper + reproducibility package | Submission-ready paper | ☐ Not started |

### 17.2 Remaining phases in detail

#### Phase 6 — A1 formal run ✅ *(done 21 Oct 2026)*
- [x] Pre-flight: free RAM ≥ 3.5 GB; AC power; apps closed.
- [x] Archive v1 results.
- [x] `python src/verify_workloads.py` → `GATE PASSED`.
- [x] `python src/run_a1_experiments.py --partitions 1 2 4 8 16 50 200 400` (log: `logs/a1_formal.log`).
- [x] Review per-cell CV/IQR: 5 of 32 cells noisy (single outliers); kept and disclosed; sensitivity check shows no conclusion changes (research log F-A1-6).
- [x] Commit results; tag the repository (`a1-formal-v2`).
- **Deliverable:** populated A1 rows in `experiment_results.csv` + `a1_best_config_per_workload.csv`.

#### Phase 7 — A2 compaction ✅ *(done 21 Oct 2026)*
- [x] Build `compaction.py` (read fragmented orders → write to a new path with target file count; `Copt` measurable).
- [x] Build `run_a2_experiments.py` (before → compaction → after, randomized blocks, **no caching**, per-run warm-up, session warm-up).
- [x] **A2 pilot** (E4): gate passed — no extreme level needed.
- [x] Formal A2: 6 workloads × 5 blocks × 3 measurements = 90 measured executions (tag `a2-formal-v2`).
- **Deliverable:** A2 rows + compaction cost/benefit table (`results/processed/a2_compaction_summary.csv`).

#### Phase 8 — Cost model + break-even
- [ ] Write `Cost_Model_Assumptions.md`: cite the **current official** pricing page(s) (e.g. EC2/Glue) with access date; state the proxies P1/P2; state "estimated, not billed".
- [ ] Implement `cost_model.py`: `Crun`, `Copt`, `TC(a, N)`, `N*`.
- [ ] Define `Copt` for A1 (search vs rule — §12.3) and for A2 (compaction job).
- [ ] Break-even table per workload and N ∈ {1, 5, 10, 50}; curves; **sensitivity** (price ±50 %, both proxies).
- **Deliverable:** `cost_model.py`, assumptions file, break-even table.

#### Phase 9 — Decision framework (build + validate)
- [ ] Implement `decide(workload, N)` as a rule-based function (no ML).
- [ ] Generate **unseen** validation workloads (new seeds, medium volume, intermediate levels).
- [ ] Compare Static vs Framework (and optionally Performance-only) on total estimated cost per workload × N.
- [ ] Never round or invent figures: every number must trace to `experiment_results.csv`.
- **Deliverable:** `decision_framework.py` + validation table.

#### Phase 10 — Analysis, figures, hypotheses
- [ ] Descriptive statistics, effect sizes, bootstrap CIs (§22).
- [ ] Seven target figures (§22.3) and supporting tables.
- [ ] **H1** and **H2** verdicts with evidence — either outcome is a result.
- **Deliverable:** `analysis/analysis.ipynb`, figures, verdicts.

#### Phase 11 — Paper, revision, submission
- [ ] Draft §§ 1–5 early (they do not depend on results); write §§ 6–8 directly from Phase 10 outputs.
- [ ] Similarity check; AI-assistance disclosure; reference verification.
- [ ] IEEE template, PDF eXpress validation, reproducibility package, submission.
- **Deliverable:** submission-ready paper + package.

### 17.3 Definition of Done (end of project)

| # | Deliverable | Status |
|---|---|---|
| 1 | Research paper (submission-ready, IEEE format) | ☐ |
| 2 | Experimental framework (Python + PySpark) | ◐ (A1 + A2 done; cost model / decision framework pending) |
| 3 | Workload generator (seeded, reproducible) | ✅ |
| 4 | `experiment_results.csv` (raw measurements, every run) | ✅ (formal A1 + A2; validation runs to come) |
| 5 | Cost model (published pricing + measured usage, documented assumptions) | ☐ |
| 6 | Decision framework (rule-based Nothing / Tune / Compact) | ☐ |
| 7 | Results (7 figures, break-even curves, H1/H2 verdicts) | ☐ |
| 8 | Reproducibility package (code + configs + instructions) | ◐ (README + tags done; push to GitHub pending) |

---

## 18. Phase Completion Status

| Category | Count | Phases |
|---|---:|---|
| ✅ **Completed** | **8** | 0, 1, 2, 3, 4, 5, 6, 7 |
| ◐ **In progress** | **0** | — |
| ☐ **Not started** | **4** | 8, 9, 10, 11 |
| **Total** | **12** | |
| **Remaining** | **4** | 8, 9, 10, 11 |

```
Phase  0 Research prep         ████████████ 100 %  ✅
Phase  1 Literature review     ████████████ 100 %  ✅
Phase  2 Environment           ████████████ 100 %  ✅
Phase  3 Baseline pipeline     ████████████ 100 %  ✅ (v2)
Phase  4 Workload generator    ████████████ 100 %  ✅
Phase  5 Instrumentation       ████████████ 100 %  ✅
Phase  6 A1 experiments        ████████████ 100 %  ✅ (21 Oct)
Phase  7 A2 compaction         ████████████ 100 %  ✅ (21 Oct)
Phase  8 Cost model            ░░░░░░░░░░░░   0 %
Phase  9 Decision framework    ░░░░░░░░░░░░   0 %
Phase 10 Analysis & figures    ░░░░░░░░░░░░   0 %
Phase 11 Paper & submission    ░░░░░░░░░░░░   0 %
```

> **Reading the numbers.** By *count* the project is two-thirds through its phases, but the remaining phases (cost model, framework, analysis, paper) carry most of the intellectual and writing effort.

---

## 19. Research Plan and Timeline

### 19.1 Original 8-week plan vs reality

| Week (planned) | Planned | What actually happened |
|---|---|---|
| 1 | Environment + baseline pipeline (P2–3) | ✅ Done 29–30 Aug (ahead of plan) |
| 2 | Workload generator + instrumentation + pilot (P4–5) | ✅ P4 on 30 Aug; P5 verified 12 Sep |
| 3 | A1 experiments (P6) | ✅ Pilot 11 Oct → redesign; sweep + **formal run 21 Oct** |
| 4 | A2 (P7) | ✅ Pilot + formal run 21 Oct |
| 5 | Cost model (P8) | ☐ |
| 6 | Framework (P9) | ☐ |
| 7 | Analysis (P10) | ☐ |
| 8 | Paper (P11) | ☐ |

The first two weeks of work were completed quickly; the project then stalled for several weeks (mid-Sep → early Oct) before the A1 pilot. The original plan would have ended ≈ 24–27 Oct 2026.

### 19.2 Re-baselined plan (from 20 Oct 2026)

*Assumes ≈ 15–20 working hours per week. **Confirm the real submission deadline first** (§32).*

| Week | Dates | Work | Exit criterion |
|---|---|---|---|
| **R1** | 20 – 26 Oct | Formal A1 run + review · build `compaction.py` + A2 pilot · draft paper §1–3 (intro, related work, problem) | A1 data collected and archived · A2 pilot gate decided |
| **R2** | 27 Oct – 2 Nov | Formal A2 · first look at A1/A2 results · `Cost_Model_Assumptions.md` + `cost_model.py` | A2 data collected · cost model runs |
| **R3** | 3 – 9 Nov | Break-even tables/curves · decision framework + validation · draft §4–5 (framework, methodology) | Static-vs-Framework table |
| **R4** | 10 – 16 Nov | Analysis notebook · 7 figures · H1/H2 verdicts · draft §6–8 | Figures + verdicts frozen |
| **R5** | 17 – 23 Nov | Complete draft · revision · similarity check · IEEE formatting · reproducibility package · submission | Submission-ready paper |

**Critical path:** formal A1 → A2 pilot → A2 formal → cost model → framework → analysis → paper. **Parallelizable:** all paper sections that do not depend on results (Introduction, Related Work, Problem Definition, Methodology) can be written *while experiments run*.

### 19.3 If time is shorter — descoping options (in order)

1. Keep 5 blocks; do **not** add extra fragmentation levels unless the A2 pilot fails.
2. Limit framework validation to 4–6 unseen workloads.
3. Drop the optional performance-only baseline.
4. Write a 5–6 page paper (not longer); cut non-essential figures (keep F1, F3, F5, F6, F7).
5. Never cut: the pilot gate, the quality gates, the limitations section, the reproducibility package.

---

## 20. Next Steps

### 20.1 Immediate (next working session)

*Steps 1–8 of the 20 Oct list (reboot, RAM check, archive v1, environment, gate, formal A1, commit/tag) and the A2 build and runs were completed on 21 Oct 2026.*

1. ☐ Choose the cloud **pricing basis** (EC2 vCPU-hour / Glue DPU / Databricks DBU) and the cost proxy.
2. ☐ Phase 8: `Cost_Model_Assumptions.md` + `cost_model.py`; break-even table and sensitivity.
3. ☐ Push commits and tags to GitHub (needs the personal-account login on this laptop).

### 20.2 Then

4. ☐ Phase 9 → 10 → 11 as in §17.2; draft paper §§ I–V in parallel (`paper/`, IEEEtran LaTeX).

### 20.3 Housekeeping

- ☑ Fix `Literature_Matrix.xlsx` metadata (Micky, PARIS, Smartpick, cross-layer paper) — done 22 Oct 2026.
- ☐ Retire the stale checkboxes in `Final_Execution_Plan.md` — this document is the live status.
- ☐ Decide the **target venue and deadline**; confirm expectations with the guide (cloud validation? page limit?).
- ☑ Write the repository `README.md` (run order, requirements, expected outputs) — done 20–21 Oct 2026.
- ☑ Keep the 10 generated workloads reproducible: only the **seed and the scripts** are committed, not the Parquet data (`data/` is git-ignored).

---

## 21. Data Collection Strategy

### 21.1 Sources and instruments

| Data | Source | Instrument |
|---|---|---|
| Input workloads | Synthetic, numpy-seeded (seed 42) | `workload_generator.py`; per-workload `manifest.json` records requested vs *measured* rows/files/size |
| Job metrics | Spark task/stage metrics | **sparkMeasure** `StageMetrics` (executor run/CPU time, shuffle bytes, spill, GC, tasks/stages, bytes read) |
| Elapsed time | Python | `time.perf_counter()` around the action |
| System load | OS | `psutil` sampler thread (system-wide CPU %, RAM %, 0.2 s interval) |
| Shuffle verification | Spark REST API | `verify_workloads.py` |
| Environment | OS / runtime | `environment_report.py` → `environment.md` |

### 21.2 Sampling plan (formal)

| Experiment | Cells | Blocks | Measured rows | Non-recorded executions |
|---|---:|---:|---:|---:|
| A1 formal | 32 (4 workloads × 8 settings) | 5 | **160** | 192 (session + per-run warm-ups) |
| A2 formal | 6 workloads × 3 stages | 5 | **90** | warm-ups as in A1 |
| Framework validation | analytical on measured cost table + a small number of confirmation runs on unseen workloads | — | TBD | — |

### 21.3 Randomization and blocking

Each block contains every cell exactly once in a freshly shuffled order (run-order seed 123, independent of the data seed), so session-long drift (thermal throttling, background load) is spread across all cells instead of piling up on whichever ran last.

### 21.4 Run hygiene checklist (before every formal session)

- ☐ Reboot → only PowerShell open; AC power; best-performance power mode; sleep/screen-off disabled.
- ☐ Free RAM ≥ 3.5 GB (the pre-flight enforces this; **do not use `--force` for formal runs**).
- ☐ No other heavy process (browser, IDE indexing, antivirus scans, updates) — check Task Manager.
- ☐ Same machine, same Python/Java/Spark versions, same `environment.md`.
- ☐ Console output saved to `logs/`.
- ☐ Note the start/end time and anything unusual in a run log.

### 21.5 Storage, naming and versioning

| Item | Convention |
|---|---|
| Raw results | `results/raw/experiment_results.csv` — **append-only, script-written, never hand-edited**; schema-guarded |
| Pilot / sweep files | `pilot_a1_v1_results.csv`, `pilot_a1_v2_sweep.csv` — archived, never mixed with formal data |
| Processed summaries | `results/processed/*.csv` — regenerated by scripts |
| Exclusions | A separate `results/raw/exclusions.csv` (`run_id`, reason) — the raw file is never edited |
| Version tracking | `pipeline_version` column in every row; Git tags per stage (`a1-formal-v2`, `a2-formal`, `paper-v1`) |
| Backup | Push to the GitHub repository after every formal session |

### 21.6 Validity rules for runs (pre-registered)

- A run is **invalid** only if it crashed, violated the pre-flight, or coincided with a documented external event. Invalid runs are listed in `exclusions.csv` with a reason and re-run.
- **Outliers are never removed by value.** They stay in the data, are disclosed, and the analysis uses robust statistics (median/IQR) and reports sensitivity.
- A cell with CV > 20 % is flagged `[NOISY]` and investigated; the decision to repeat the *whole block* (not individual points) is documented.

### 21.7 Data dictionary

See [Appendix B](#appendix-b--results-csv-schema) (34 columns).

---

## 22. Data Analysis Plan

### 22.1 Pipeline of analysis (`analysis/analysis.ipynb`)

1. **Load and validate** — header match, `experiment_action`, `pipeline_version == v2`, no warm-up rows, no exclusions.
2. **Cell summaries** — median, IQR, CV, min/max per (workload, setting) and per (workload, stage).
3. **Effect sizes** — speed-up `= median(default) / median(best)`, percent saving, in both wall-clock and executor time.
4. **Uncertainty** — 95 % **block-paired bootstrap** CIs (resample blocks, 10 000 resamples) for medians and for speed-ups.
5. **A2** — before vs after compaction per workload; `Copt`; benefit vs file count (trend, e.g. Spearman).
6. **Cost transformation** — apply proxies P1/P2 with cited prices (labelled *estimated*).
7. **Break-even** — `N*` per workload and action; TC-vs-N lines.
8. **Framework validation** — Static vs Framework (± Performance-only) on unseen workloads × N ∈ {1, 5, 10, 50}: total cost, % reduction, win/tie/loss counts, paired-bootstrap CI.
9. **Hypotheses** — H1 and H2 judged against the criteria in §9.
10. **Sensitivity** — price ±50 %, proxy P1 vs P2, with/without the noisiest cells.
11. **Figures and tables** — generated by code only.

### 22.2 Statistical principles

- **Robust statistics first**: median and IQR; CV is reported but treated with care at n = 5.
- **Effect sizes and CIs over p-values.** An exact two-sided Wilcoxon cannot go below *p* = 0.0625 at n = 5 (0.031 at n = 6, 0.0078 at n = 8); use more blocks only if formal tests are wanted, and never manufacture significance.
- **No silent exclusions**; sensitivity analysis shows conclusions do not hinge on a few runs.
- **Estimated, not billed:** every cost figure carries that label.
- **No invented numbers:** every figure traces back to `experiment_results.csv`.

### 22.3 Target figures

| # | Figure | Answers |
|---|---|---|
| F1 | Workload size vs execution time (default vs best setting) | RQ1 |
| F2 | Shuffle volume vs optimal partition configuration | RQ1, H1 |
| F3 | File count vs compaction benefit | RQ1, H1 |
| F4 | Optimization overhead `Copt` (A1 search, A2 compaction) | RQ3 |
| F5 | Total cost vs execution frequency N | RQ3 |
| F6 | Break-even point curves | RQ3 |
| F7 | Static vs cost-aware framework — total cost | RQ2, H2 |
| *Supporting* | Runtime-vs-partitions curve per workload (U-shape); CV heat-map; task-count / overhead breakdown | RQ1, validity |

### 22.4 Target tables

| # | Table |
|---|---|
| T1 | Experimental environment |
| T2 | Workload catalogue (levels, measured rows/files/size/shuffle MB) |
| T3 | A1: best partition setting and speed-up per workload |
| T4 | A2: compaction cost and benefit per workload |
| T5 | Break-even `N*` per workload and action |
| T6 | Framework validation (Static vs Framework) |
| T7 | Limitations / threats (optional, if space allows) |

---

## 23. Paper Writing Plan

### 23.1 Principles

1. **Write what does not depend on results now** (Introduction, Related Work, Problem Definition, Methodology) — it runs in parallel with experiments.
2. **Results sections are written only from Phase 10 outputs.** No number is typed from memory; every figure traces to `experiment_results.csv`.
3. **Claim exactly what was shown**, no more: local single-machine, synthetic data, *estimated* cost, two actions, AQE off.
4. **Either outcome is publishable** if analysed honestly — a null result for compaction, or a framework that only sometimes beats the static baseline, is a finding.
5. **Terminology stays fixed:** A1, A2, OBEP, static baseline, estimated cost, pipeline v2.

### 23.2 Section-by-section source map

| Paper section | Content | Draw from | Can write now? |
|---|---|---|---|
| I. Introduction | Problem, gap, contributions, roadmap | §3–§5, §7–§8 | ✅ |
| II. Related Work | Six themes condensed; differentiate PTO / AutoComp / Tuneful | §11, `Literature_Review_Shaikh.docx` (adapt — do not paste) | ✅ |
| III. Problem Definition and Cost Model | `TC(a, N)`, OBEP, assumptions | §4, §12.3 | ✅ |
| IV. Proposed Framework | Inputs, decision rule, flow | §12.4 | ◐ (final rule after Phase 9) |
| V. Experimental Methodology | Environment, workloads, pipeline v2, actions, protocol, metrics, cost estimation, quality gates | §12–§14, §21 | ✅ (A2 details after Phase 7) |
| VI. Results | RQ1 (A1, A2), RQ3 (break-even), RQ2 (validation) | Phase 6–10 outputs | ❌ |
| VII. Discussion | Interpretation, practical guidance, the v1-pilot lesson, "no spill at this scale" | Phase 10 | ❌ |
| VIII. Threats and Limitations | §26, stated plainly | §26 | ✅ |
| IX. Conclusion and Future Work | AQE interaction, distributed clusters, learned predictors, more actions, billed-cost validation | Phase 10 | ❌ |
| Acknowledgment | Guide; **AI-assistance disclosure** (§23.5) | — | ✅ |
| Data / code availability | Repository link, seeds, versions | §27 | ✅ |
| References | Verified IEEE list | Appendix A | ◐ |

### 23.3 Page budget (6-page two-column conference paper — adjust to the venue's call for papers)

| Section | Pages |
|---|---:|
| Abstract + Index Terms | 0.25 |
| I. Introduction | 0.75 |
| II. Related Work | 0.75 |
| III. Problem and Cost Model | 0.6 |
| IV. Framework | 0.5 |
| V. Methodology | 1.0 |
| VI. Results (incl. 4–6 figures/tables) | 1.4 |
| VII–VIII. Discussion, Threats | 0.6 |
| IX. Conclusion | 0.2 |
| References | 0.5 |
| **Total** | **≈ 6.5** (trim figures/related work if the limit is 6) |

### 23.4 Reusable text blocks (safe to draft now — no results)

- **Problem sentence.** *Optimizing a Spark pipeline is not free: tuning execution parameters or compacting small files itself consumes compute, so an action that speeds up one run is not automatically economical over the lifetime of a recurring pipeline.*
- **Gap sentence.** *Prior work automates individual optimizations and, in some cases, weighs their benefit against cost, but a controlled evaluation of a single break-even decision rule applied jointly to an execution-level action and a storage-level action is missing.*
- **Contribution list.** (1) a simple, interpretable cost-benefit decision framework for two Spark optimization actions; (2) an empirical characterization of how data volume, file fragmentation, shuffle volume and execution frequency affect optimization value; (3) a quantitative break-even analysis linking optimization overhead to future savings; (4) practical guidance on when to apply an optimization and when to keep the static pipeline.
- **Position sentence.** *We do not claim that Spark tuning, workload-aware optimization, adaptive layout or cost optimization are new; our contribution is the focused empirical evaluation of when two existing actions pay for themselves.*
- **Limitations sentence.** *All experiments run on a single machine with synthetic data; costs are estimated from published pricing rather than billed; Adaptive Query Execution is disabled to isolate manual partition tuning.*

### 23.5 Originality, plagiarism and AI-assistance policy

| Topic | Rule |
|---|---|
| **Own words** | Paraphrase every source; quote only very short, essential phrases; never lift sentences from abstracts. |
| **Self-similarity** | The literature-review prose will overlap with the paper's Related Work — adapt and condense; if the college report is indexed by a similarity checker, expect self-match and mention it to the guide. |
| **Similarity check** | Run Turnitin / iThenticate before submission; venues commonly screen with CrossCheck-type tools and many set a limit in the ~15–25 % range — **check the venue's stated limit**. |
| **AI assistance** | This project used an AI assistant for design discussion, code and drafting. IEEE's author guidance requires disclosure of AI-generated content (text, figures, images, code) in the **Acknowledgments** section — **check the chosen venue's current policy and disclose accordingly.** The researcher remains responsible for every claim, number and citation. |
| **Reference integrity** | Verify each reference against the publisher page/DOI yourself before submission; treat grey literature and preprints with care. |
| **Numbers** | Every reported figure must be regenerable from the repository. |

### 23.6 Review checklist before submission

- [ ] Every claim is supported by a figure, table or citation.
- [ ] RQ1–RQ3 and H1–H2 are each answered explicitly (supported / not supported / inconclusive).
- [ ] "Estimated, not billed" appears wherever cost is reported.
- [ ] Limitations section is present and plain.
- [ ] All acronyms defined at first use; units everywhere; figures readable in grayscale.
- [ ] Similarity report acceptable; AI disclosure present; references verified.
- [ ] Reproducibility package tagged and linked.

---

## 24. Experimental Paper Format

An empirical systems paper is organized so a reviewer can find the *question*, the *method*, the *evidence* and the *limits* quickly. Recommended structure for this study:

| Block | Section | Must contain |
|---|---|---|
| **Motivate** | Abstract · Introduction | Problem, gap, contributions, key result (with numbers) |
| **Position** | Related Work | Themes A–F in ≤ 1 page; explicit differences from PTO, AutoComp, Tuneful |
| **Define** | Problem Definition · Cost Model | Notation, `TC`, OBEP, decision rule, assumptions |
| **Method** | Experimental Methodology | Environment table; workload generator and levels; pipeline v2 and why; actions A1/A2; baselines; metrics and instruments; run protocol (blocks, warm-ups, caching); cost estimation; quality gates |
| **Evidence** | Results | One subsection per RQ, each with figure/table, effect size and CI |
| **Interpret** | Discussion | What the results mean for practitioners; surprises (no spill at this scale; the v1 pilot's degenerate shuffle) |
| **Bound** | Threats to Validity · Limitations | Internal, construct, external, conclusion validity |
| **Close** | Conclusion · Future Work | Answers to RQs; AQE interaction, distributed clusters, learned predictors, billed-cost validation |
| **Enable** | Data/Code Availability · References | Repository link, seeds, versions, pipeline version, run protocol |

### 24.1 RQ → evidence map

| Question | Experiment | Figure / table | Reported as |
|---|---|---|---|
| RQ1 | A1 and A2 formal matrices | F1, F2, F3, T3, T4 | Medians, IQR, speed-ups with 95 % CIs per workload |
| RQ3 | Cost model on measured `Copt`/`Crun` | F4, F5, F6, T5 | `N*` per workload and action; sensitivity range |
| RQ2 | Framework validation on unseen workloads | F7, T6 | Total cost, % reduction, win/tie/loss, paired CI |

### 24.2 Empirical-paper checklist

- [ ] Research questions and hypotheses stated up front, with decision criteria.
- [ ] Baselines justified; the static configuration is the *default*, not a straw man.
- [ ] Metrics defined (wall-clock vs executor time vs estimated cost).
- [ ] Number of repetitions, warm-up, randomization and exclusions stated.
- [ ] Environment fully specified; all versions pinned.
- [ ] Negative / null results reported.
- [ ] Threats to validity discussed.
- [ ] Artifacts available.

---

## 25. IEEE Paper Format and Template

> Always follow the **specific call for papers**: page limit, template version, blind/non-blind review, and submission system. The points below are the common IEEE conference conventions.

### 25.1 Template and layout

- Use the **IEEE conference template** (Word) or **`\documentclass[conference]{IEEEtran}`** (LaTeX). Do **not** change margins, fonts or spacing; use the template's styles.
- US Letter, **two columns**, Times-style 10 pt body text.
- Order: Title → Author block → **Abstract** (commonly ~150–250 words, no citations or abbreviations) → **Index Terms** → numbered sections.
- Section headings use **Roman numerals** (I. INTRODUCTION); subsections use letters (A., B.).
- **Figures:** captions *below*, "Fig. 1." Use vector or ≥ 300 dpi images; readable at column width and in grayscale.
- **Tables:** captions *above*, "TABLE I". Keep tables within a column or span both columns deliberately.
- **Equations:** numbered at the right margin, e.g. (1); define every symbol.
- **Citations:** numbered in square brackets in order of first appearance — [1], [2]–[4]; one list, ordered by first citation.
- Define abbreviations at first use; avoid footnotes for essential content.
- Final PDF should pass **IEEE PDF eXpress** validation if the venue requires Xplore-compatible PDFs.

### 25.2 Reference formats (IEEE style)

| Type | Pattern |
|---|---|
| Journal | `[n] A. Author, B. Author, "Title," *Journal*, vol. x, no. x, pp. xxx–xxx, Mon. year, doi: …` |
| Conference | `[n] A. Author et al., "Title," in *Proc. Conf. Name*, City, Country, year, pp. xxx–xxx, doi: …` |
| arXiv preprint | `[n] A. Author, "Title," arXiv preprint arXiv:ID, year.` (mark "unpublished preprint" if relevant) |
| Online documentation | `[n] Organisation, "Page title," Documentation, year. [Online]. Available: URL (accessed Mon. DD, YYYY).` |
| Blog | `[n] A. Author, "Title," Site/Blog, year. [Online]. Available: URL` |

List up to six authors; use "et al." when there are more than six. Reproduce the verified list in [Appendix A](#appendix-a--reference-list-ieee).

### 25.3 Paper skeleton (fill only with results traceable to the CSV)

```
Title: A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines
Authors: Mustaqeem Shaikh [, guide name — confirm], [affiliation], [email]

Abstract—Optimizing an Apache Spark pipeline is not free: tuning execution parameters or
compacting small files consumes compute that must be recovered by future executions. We
present a lightweight, interpretable cost-benefit framework that decides, for a workload and
an expected number of future runs, whether to keep a static configuration, tune shuffle
partitions, or compact files, using a total-cost model and an optimization break-even point.
We evaluate it in a controlled local study across [X] workloads varying data volume, file
fragmentation and shuffle volume, using a randomized-block protocol. Results show that
[A1 finding], [A2 finding], and that the framework reduces estimated total cost by [Y %]
relative to a static configuration on [Z] unseen workloads while avoiding [W] unprofitable
optimizations. We discuss the limits of single-machine execution and estimated cost.
[Replace every bracket with a number traceable to experiment_results.csv.]

Index Terms—Apache Spark, cost-aware optimization, break-even analysis, shuffle partitions,
file compaction, data pipelines.

I.   INTRODUCTION            problem · gap · contributions · roadmap
II.  RELATED WORK            A–F themes; PTO / AutoComp / Tuneful differences
III. PROBLEM DEFINITION AND COST MODEL     TC(a,N) · OBEP · assumptions
IV.  PROPOSED FRAMEWORK      inputs · decision rule · flow figure
V.   EXPERIMENTAL METHODOLOGY   environment table · workloads · pipeline v2 · actions ·
                                 protocol · metrics · cost estimation · quality gates
VI.  RESULTS                 A. A1 (RQ1)  B. A2 (RQ1)  C. Break-even (RQ3)  D. Framework (RQ2)
VII. DISCUSSION              practical guidance · surprises · why the v1 pilot was discarded
VIII.THREATS TO VALIDITY AND LIMITATIONS
IX.  CONCLUSION AND FUTURE WORK
     ACKNOWLEDGMENT         guide · AI-assistance disclosure per venue policy
     DATA AND CODE AVAILABILITY    https://github.com/Mustaqeem3313/spark-cost-aware-optimization-CAO-DF
     REFERENCES
```

### 25.4 Submission checklist

- [ ] Venue, deadline, page limit, template and review model (single/double-blind) confirmed.
- [ ] Anonymize if double-blind (including the repository link).
- [ ] PDF validated (IEEE PDF eXpress if required); fonts embedded; figures legible.
- [ ] Similarity report within the venue's limit; AI-assistance disclosed.
- [ ] Copyright / e-form completed; at least one author registered to present.
- [ ] Supplementary material and repository tagged.

---

## 26. Risks, Threats to Validity and Limitations

### 26.1 Risk register

| # | Risk | Likelihood | Impact | Response |
|---|---|---|---|---|
| R1 | **Schedule:** project is behind the original 8-week plan | **High** | High | Re-baselined plan (§19.2); confirm the real deadline; write non-result sections in parallel; use descoping order (§19.3) |
| R2 | **Memory pressure** on a 7.9 GB machine causes noisy runs or OOM | **High** | High | Reboot, close apps, pre-flight guard, no `--force`; fallback 2 g driver heap |
| R3 | **A2 effect below the noise floor** at ≤ 50 files / ≤ 28 MB | Medium | Medium | A2 pilot first; add an extreme fragmentation level; report a null result honestly |
| R4 | Framework does not beat the static baseline | Medium | Low (still a finding) | Analyse why (e.g. small savings, high `Copt`); discuss in §VII |
| R5 | Reviewers see overlap with PTO / AutoComp / Tuneful | Medium | High | Precise differentiation (§5); frame as a focused empirical study; target a regional venue |
| R6 | "Local single machine" objection | High | Medium | State as a scope decision and limitation; optional cloud validation only if time/budget allow |
| R7 | Cost-proxy dependence (wall-clock vs executor time) | Medium | Medium | Report both proxies and a sensitivity analysis |
| R8 | Mixing pilot (v1) and formal (v2) data | Low | High | `pipeline_version` column, archived files, schema guard |
| R9 | Unit misreading of sparkMeasure fields | Low | Medium | Verify units on a known run before using them in the cost model |
| R10 | Data loss / hardware failure | Low | High | Push to GitHub after every session |
| R11 | Spark-on-Windows rough edges resurface | Low | Low | §29 troubleshooting; fixes live in code |

### 26.2 Threats to validity

| Type | Threat | Mitigation |
|---|---|---|
| **Internal** | Noise, thermal throttling, background load; order effects; cache and OS file-cache effects; shared data seed across workloads | Randomized blocks; warm-ups; pre-flight; cached inputs in A1; explicit caveat for A2's OS cache; different seeds for validation workloads |
| **Construct** | Wall-clock vs resource cost; *estimated* cost; synthetic workload realism; execution frequency modelled analytically, not executed | Two cost proxies; label estimates; document the generator; state the modelling assumption |
| **External** | Single machine / `local[*]`, ≤ 2 M rows, one synthetic schema, one Spark version, AQE off, broadcast off | State as limits; avoid cluster-level claims; list as future work |
| **Conclusion** | Small n (5 blocks), CV fragility, multiple comparisons | Effect sizes + bootstrap CIs; robust statistics; no manufactured significance; sensitivity analysis |

### 26.3 Limitations to state in the paper

1. Single-machine, multi-threaded execution (not a distributed cluster, no real network shuffle).
2. Synthetic data and a single pipeline shape at small scale (≤ 2 M rows, ≤ 36 MB shuffled).
3. Cost is **estimated** from published pricing, not billed.
4. AQE disabled and broadcast joins disabled to isolate the studied effects; interactions are future work.
5. No spill occurs at this scale, so the "too few partitions → spill" mechanism is not exercised.
6. Two actions only; no joint (A1 + A2) arm.

---

## 27. Reproducibility and Artifacts

### 27.1 What makes the results reproducible

| Element | How |
|---|---|
| Code | GitHub repository: https://github.com/Mustaqeem3313/spark-cost-aware-optimization-CAO-DF (tag each stage) |
| Data | Regenerated from the seed (42) by `workload_generator.py`; per-workload `manifest.json` and `workload_index.csv` |
| Versions | `requirements.txt` pins; Java/Spark/Python recorded in `environment.md` |
| Configuration | One session builder with read-back verification |
| Pipeline | Versioned (`PIPELINE_VERSION`), logic frozen for formal runs |
| Protocol | Randomized blocks, warm-ups, caching rules, pre-flight — all in code |
| Raw results | Append-only CSV with a fixed 34-column schema; logs saved |

### 27.2 Reproduction order (for a reviewer)

```
python -m venv venv && venv\Scripts\Activate.ps1      # Python 3.11
pip install -r requirements.txt
python src/environment_report.py                       # then confirm the disk type
python src/verify_phase2_setup.py                      # 5/5 checks
python src/workload_generator.py                       # 10 workloads (seed 42)
python src/verify_workloads.py                         # shuffle-scaling gate
python src/run_a1_experiments.py --partitions 1 2 4 8 16 50 200 400
# A2, cost model, framework, analysis notebook: Phases 7–10
```

### 27.3 Release checklist

- [ ] README with prerequisites (Windows notes: winutils, no-space path, Python 3.11) and expected outputs.
- [ ] `.gitignore` for generated data, venv and Spark scratch files.
- [ ] Tags for pilot, formal A1, formal A2 and the paper version.
- [ ] Optional: archive a release on Zenodo for a citable DOI.
- [ ] Licence chosen (e.g. MIT/Apache-2.0) — confirm with the guide.

---

## 28. Decision Log

| # | Decision | Reason | Consequence |
|---|---|---|---|
| D1 | Target an **IEEE-sponsored regional conference** | Realistic for a first paper and a short timeline; Xplore-indexed after review | Novelty bar is a focused empirical contribution |
| D2 | Frame as a **decision framework**, not a new optimizer | PTO, AutoComp and Tuneful already cover optimizers; novelty lies in the dual-action break-even decision | Careful differentiation in Related Work |
| D3 | **Two actions only** (A1, A2); drop the joint arm, A0/A3 and extra hypotheses | Controlled, finishable scope | Joint optimization becomes future work |
| D4 | **Local-first**, free tier; cost *estimated* from published pricing | No budget risk; relationships matter more than scale | State as a limitation; optional cloud validation later |
| D5 | **Rule-based decision framework, no ML** | Interpretability; ML is future work | Simple, defensible |
| D6 | **Disable AQE** for A1 | Otherwise AQE overrides manual partitions | Interaction left as future work |
| D7 | **Python 3.11**, Java 17, PySpark 3.5.1 | Python 3.13 incompatible; Spark 3.5 supports Java 17 | Documented hard requirement |
| D8 | **Event log off**; `winutils` installed for writes | Event log never load-bearing; writes need winutils | Platform fragility removed from the default path |
| D9 | **Arrow off** in data generation | Needs JVM module flags on Java 9+ | Slower generation, no effect on experiments |
| D10 | **sparkMeasure 0.28** (Python + Maven) | Version `0.24` never existed | Real stage/shuffle/spill metrics |
| D11 | numpy-seeded generation (seed 42) | True portable determinism vs Spark `rand(seed)` | Validation workloads need other seeds |
| D12 | Shuffle volume controlled by **filter selectivity** | Ties the factor to what the pipeline does | Verified by the shuffle-scaling gate |
| D13 | **One shared SparkSession**, `spark.conf.set()` per cell | Avoids JVM restarts; stable sparkMeasure attachment | Faster, cleaner |
| D14 | **Cache inputs in A1** (not in A2) | Isolate compute/shuffle cost in A1; A2 must read files | Control device, not an optimization |
| D15 | **Randomized blocks** + warm-ups | Spread drift evenly; remove JIT/cold-start bias | Protocol in code |
| D16 | Keep the **v1 pilot as evidence**; build **pipeline v2** | v1's shuffle was ~10 KB and workload-independent | `pipeline_version` column; v1 archived |
| D17 | **Broadcast joins off** | Force a real shuffle of orders | Documented as a fixed setting |
| D18 | Action = **`noop` sink** | Executes the full plan; no I/O; no column pruning | Used by every timed run |
| D19 | **Read back** effective Spark settings | A silently ignored setting would invalidate results | Hard failure on mismatch |
| D20 | Driver **3 g** + RAM/AC pre-flight | Pilot ran at 87–96 % RAM | Formal runs refuse `--force` |
| D21 | A1 grid **1, 2, 4, 8, 16, 50, 200, 400** | Sweep showed optimum depends on the workload and sits near the core count | 160 measured runs |
| D22 | Report **effect sizes + bootstrap CIs**; tests only where n allows | Exact Wilcoxon cannot go below p = 0.0625 at n = 5 | Honest statistics |

---

## 29. Troubleshooting Reference

### 29.1 Harmless messages (ignore)

| Message | Why it appears |
|---|---|
| `Did not find winutils.exe … HADOOP_HOME unset` (WARN, but the run continues) | Hadoop's Windows shim warning |
| `Unable to load native-hadoop library` | Normal on Windows |
| `Exception while deleting Spark temp dir … Failed to delete … .jar` / `NoSuchFileException` (after "complete") | Windows locks loaded JARs during shutdown; happens *after* results are saved |
| `Stage N contains a task of very large size` (workload generation) | Side effect of Arrow being off; generation is untimed |
| `SUCCESS: The process with PID … has been terminated.` | Windows cleaning up child processes |

### 29.2 Real errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| pip tries to compile pandas / `vswhere.exe` error | Python 3.13 | Install Python 3.11; rebuild the venv with `py -3.11 -m venv venv` |
| `'cost' is not recognized as a command` | Spaces in the project path | Move the project to a path with **no spaces** |
| `HADOOP_HOME … unset` crash when **writing** Parquet | winutils missing | Install `winutils.exe` + `hadoop.dll`; set `HADOOP_HOME`; add `%HADOOP_HOME%\bin` to PATH; restart PowerShell |
| `Python worker failed to connect back` | Microsoft Store `python` alias | Already fixed by `PYSPARK_PYTHON = sys.executable` at import (keep `spark_config.py` current) |
| `ModuleNotFoundError: src.baseline_pipeline_v1` | An editor renamed an import | Imports must read `from src.baseline_pipeline import …`; keep archives outside `src/` |
| `the sparkMeasure jar is like not loaded` | Session built without `enable_sparkmeasure=True`, wrong Maven version, or no internet on first use | Use 0.28; pass the flag; check internet once |
| `[preflight] ABORTING … RAM available` | Less than 3.5 GB free, or on battery | Reboot, close apps, plug in; do not `--force` for formal runs |
| `RuntimeError … different column schema` | Appending to an older results file | Archive the old CSV, then re-run |
| `RuntimeError: SparkSession ignored requested settings` | A session already existed with other settings | Run in a fresh Python process |
| `JAVA_GATEWAY_EXITED` when requesting sparkMeasure | Package resolution failed hard (no network) | Restore network access; first run downloads the JARs, later runs use the cache |
| `GATE FAILED` in `verify_workloads.py` | Workload factors don't move shuffle bytes | Do **not** run formal experiments; check the Spark config read-back and the pipeline version |

---

## 30. Guide Meeting Q&A

**What are you researching?**
> Whether applying a Spark optimization is economically worthwhile for a given workload — shuffle-partition tuning and file compaction. I vary data volume, file fragmentation and shuffle volume, measure optimization overhead and resulting cost, and build a workload-aware decision framework that says when to optimize and when not to.

**What is your novelty?**
> Not a new optimizer. The decision problem of *when* an existing optimization should be applied, with one explicit break-even rule applied jointly to an execution-level and a storage-level action, evaluated in a controlled empirical study.

**How is this different from PTO and AutoComp?**
> They are production-scale systems for layout or compaction decisions (PTO uses ML predictors). I do a small, controlled, interpretable study of two different action types under one cost objective.

**How do you calculate cloud cost?**
> I measure resource usage experimentally and convert it with published cloud pricing. It is an estimate, not billed spend, and I report two cost proxies plus a sensitivity analysis.

**Why not machine learning?**
> First establish the empirical workload-to-cost relationship and validate a simple interpretable rule; ML is reasonable future work.

**Why local and synthetic?**
> Free-tier constraint and control: local runs allow randomized repeated measurements; seeded synthetic data makes every workload factor controllable and the study reproducible. The limits are stated explicitly.

**Why disable AQE?**
> Otherwise AQE can override my manual partition setting at runtime and I would be measuring AQE, not my action. The interaction is future work.

**How do you know your workload factors actually work?**
> I check it, not assume it: a shuffle-scaling gate measures shuffle bytes — data volume moves them ≈ 34–42× and the shuffle level ≈ 4.5–5.5×.

**Why did you change your pipeline after the pilot?**
> The pilot showed the first design shuffled only ≈ 10 KB regardless of workload, so it measured scheduling overhead, not shuffle cost. The pilot gate caught it; the pilot is archived and disclosed, and pipeline v2 is versioned.

**What if the framework doesn't beat the static baseline?**
> That is still a valid result; I will analyse why (small savings, high overhead) and report it.

**Is execution frequency N actually run 50 times?**
> No — N is a parameter of the cost model. I measure per-run cost and overhead once with repetitions, then compute total cost for N = 1, 5, 10, 50.

---

## 31. Glossary

| Term | Meaning |
|---|---|
| **A1** | Action 1 — tuning `spark.sql.shuffle.partitions` |
| **A2** | Action 2 — file compaction |
| **AQE** | Adaptive Query Execution — Spark's runtime re-optimization (disabled here) |
| **Block** | One repetition of the full set of cells, in a freshly randomized order |
| **Broadcast join** | Join that ships a small table to every task; avoids shuffling the large side (disabled here) |
| **Cell** | One (workload, setting) combination |
| **Copt** | One-time cost of applying an optimization |
| **Crun** | Cost of one pipeline execution (static or after optimization) |
| **CV** | Coefficient of variation = standard deviation / mean |
| **Estimated cost** | Measured resource use × published price; never billed spend |
| **Exchange** | Spark's physical shuffle operator |
| **Executor run time** | Sum of task run times (task-seconds) — a resource-based cost proxy |
| **IQR** | Interquartile range |
| **Lakehouse** | Data-lake storage with warehouse-style table management |
| **N / N\*** | Number of future executions / its break-even value |
| **Noop sink** | Spark write format that executes the plan and discards output |
| **OBEP** | Optimization Break-Even Point — executions needed to recover `Copt` |
| **Parquet** | Columnar, splittable, self-describing file format |
| **Pre-flight** | Code check of RAM and power before a run |
| **Session warm-up** | Untimed execution of every cell before block 1 |
| **Shuffle partitions** | Number of partitions a shuffle produces |
| **Small-file problem** | Many small files multiply per-file fixed costs |
| **Sort-merge join** | Join that shuffles and sorts both sides by key |
| **sparkMeasure** | Library capturing Spark stage/task metrics |
| **Static baseline** | Default configuration (200 partitions, no compaction) |
| **TC(a, N)** | Total cost of action *a* over *N* runs = `Copt(a) + N × Crun(a)` |

---

## 32. Open Questions to Confirm

| # | Question | Why it matters |
|---|---|---|
| Q1 | **What is the real submission deadline** (college and/or conference)? | The original 8-week plan has run out; §19.2 assumes ≈ 5 more weeks |
| Q2 | **Which conference** is the target (name, CFP, page limit, template, blind/non-blind)? | Fixes page budget, formatting and anonymization |
| Q3 | Does the guide expect **any cloud validation** (even a small one)? | Affects scope and the limitations wording |
| Q4 | Who are the **co-authors / guide** on the paper? | Author block and acknowledgments |
| Q5 | Is the **repository public**, and under what licence? | Reproducibility statement; blind-review anonymization |
| Q6 | Is the college report **indexed by a similarity checker** (Turnitin repository)? | Self-similarity with the paper |
| Q7 | Which **cloud pricing basis** should be cited (EC2 vCPU-hour, Glue DPU-hour, Databricks DBU)? | Phase 8 assumptions |
| Q8 | Can free RAM reach **≥ 3.5 GB** after a reboot? | Determines whether a 2 g heap fallback is needed |
| Q9 | Is a **second machine** available to repeat a subset of runs? | Would strengthen external validity |
| Q10 | Should the **AQE-on** comparison be included as an extra arm or left as future work? | Scope vs time |

---

## Appendix A — Reference List (IEEE)

*(Verified list from `Literature_Review_Shaikh.docx`. Re-verify DOIs before submission.)*

[1] N. Nguyen, M. M. H. Khan, and K. Wang, "Towards automatic tuning of Apache Spark configuration," in *Proc. 2018 IEEE 11th Int. Conf. Cloud Computing (CLOUD)*, San Francisco, CA, USA, 2018, pp. 417–425.

[2] D. Buchaca Prats, F. Albuquerque Portella, C. H. A. Costa, and J. L. Berral, "You only run once: Spark auto-tuning from a single run," *IEEE Trans. Netw. Service Manage.*, vol. 17, no. 4, pp. 2039–2051, Dec. 2020, doi: 10.1109/TNSM.2020.3034824.

[3] J. Xin, K. Hwang, and Z. Yu, "LOCAT: Low-overhead online configuration auto-tuning of Spark SQL applications," in *Proc. 2022 Int. Conf. Management of Data (SIGMOD '22)*, Philadelphia, PA, USA, 2022, pp. 1345–1358, doi: 10.1145/3514221.3526157.

[4] Y. Li *et al.*, "Towards general and efficient online tuning for Spark," *Proc. VLDB Endow.*, vol. 16, no. 12, pp. 3570–3583, 2023.

[5] C. Lyu *et al.*, "A Spark optimizer for adaptive, fine-grained parameter tuning," *Proc. VLDB Endow.*, vol. 17, no. 12, pp. 3565–3578, 2024, doi: 10.14778/3681954.3682021.

[6] J. Li, J. Ye, Y. Mao, Y. Gao, and L. Chen, "LOFTune: A low-overhead and flexible approach for Spark SQL configuration tuning," *IEEE Trans. Knowl. Data Eng.*, 2025.

[7] H. Herodotou, Y. Chen, and J. Lu, "A survey on automatic parameter tuning for big data processing systems," *ACM Comput. Surv.*, vol. 53, no. 2, Art. 43, pp. 1–37, Apr. 2020, doi: 10.1145/3381027.

[8] Y. Shen *et al.*, "Rover: An online Spark SQL tuning service via generalized transfer learning," in *Proc. 29th ACM SIGKDD Conf. Knowledge Discovery and Data Mining (KDD '23)*, Long Beach, CA, USA, 2023, doi: 10.1145/3580305.3599953.

[9] M. Xue *et al.*, "Adaptive and robust query execution for lakehouses at scale," *Proc. VLDB Endow.*, vol. 17, no. 12, pp. 3947–3959, 2024.

[10] A. Parayil *et al.*, "Towards workload-aware cloud efficiency: A large-scale empirical study of cloud workload characteristics," in *Proc. 16th ACM/SPEC Int. Conf. Performance Engineering (ICPE '25)*, 2025, pp. 136–146, doi: 10.1145/3676151.3722008.

[11] L. Bao, X. Liu, and W. Chen, "Learning-based automatic parameter tuning for big data analytics frameworks," in *Proc. 2018 IEEE Int. Conf. Big Data (Big Data)*, Seattle, WA, USA, 2018, pp. 181–190, doi: 10.1109/BigData.2018.8622018.

[12] A. Gruenheid *et al.*, "AutoComp: Automated data compaction for log-structured tables in data lakes," in *Companion Proc. 2025 Int. Conf. Management of Data (SIGMOD-Companion '25)*, 2025, pp. 404–417, doi: 10.1145/3722212.3724430.

[13] A. Goswami and R. Sarkar, "Apache Iceberg optimization: Solving the small files problem in Amazon EMR," *AWS Big Data Blog*, 2023.

[14] Delta Lake Project, "Compaction (bin-packing) — Delta Lake documentation," 2025. [Online].

[15] V. V. Meduri, D. Kreismann, R. Barber, and B. Reinwald, "PTO: A workload-driven predictive table optimizer for lakehouse systems," *Proc. ACM Manage. Data*, vol. 4, no. 1, 2026, doi: 10.1145/3786681.

[16] A. Gilad, "Cracking the ice: The battle between sort and binpack in Apache Iceberg," industry blog, 2025.

[17] A. Fekry, L. Carata, T. Pasquier, A. Rice, and A. Hopper, "To tune or not to tune? In search of optimal configurations for data analytics," in *Proc. 26th ACM SIGKDD Conf. Knowledge Discovery and Data Mining (KDD '20)*, 2020, pp. 2494–2504, doi: 10.1145/3394486.3403299.

[18] A. Fekry, L. Carata, T. Pasquier, A. Rice, and A. Hopper, "Tuneful: An online significance-aware configuration tuner for big data analytics," arXiv preprint, 2020.

[19] S. Sidhanta, W. Golab, and S. Mukhopadhyay, "OptEx: A deadline-aware cost optimization model for Spark," in *Proc. 2016 16th IEEE/ACM Int. Symp. Cluster, Cloud and Grid Computing (CCGrid)*, Cartagena, Colombia, 2016, pp. 193–202, doi: 10.1109/CCGrid.2016.111.

[20] I. Chang, "Cross-layer decision timing orchestration in cost-based database systems: Resolving structural temporal misalignment," arXiv preprint arXiv:2603.02253, Feb. 2026. (Unpublished preprint, not yet peer-reviewed.)

[21] O. Alipourfard, H. H. Liu, J. Chen, S. Venkataraman, M. Yu, and M. Zhang, "CherryPick: Adaptively unearthing the best cloud configurations for big data analytics," in *Proc. 14th USENIX Symp. Networked Systems Design and Implementation (NSDI '17)*, Boston, MA, USA, 2017, pp. 469–482.

[22] C.-J. Hsu, V. Nair, T. Menzies, and V. Freeh, "Micky: A cheaper alternative for selecting cloud instances," in *Proc. 2018 IEEE 11th Int. Conf. Cloud Computing (CLOUD)*, San Francisco, CA, USA, 2018.

[23] N. J. Yadwadkar, B. Hariharan, J. E. Gonzalez, B. Smith, and R. H. Katz, "Selecting the best VM across multiple public clouds: A data-driven performance modeling approach," in *Proc. 2017 Symp. Cloud Computing (SoCC '17)*, Santa Clara, CA, USA, 2017, pp. 452–465, doi: 10.1145/3127479.3131614.

[24] A. Das Mohapatra and K. Oh, "Smartpick: Workload prediction for serverless-enabled scalable data analytics systems," in *Proc. 24th ACM/IFIP Int. Middleware Conf. (Middleware '23)*, 2023, doi: 10.1145/3590140.3592850.

*Added 22 Oct 2026; metadata verified against Crossref.*

[25] S. Nakandala *et al.*, "AutoLiquid: Autonomic data layout optimization for the Databricks lakehouse," *Proc. VLDB Endow.*, vol. 19, no. 12, pp. 4023–4035, 2026, doi: 10.14778/3827998.3828013.

[26] G. Liu, A. Kipf, and R. Borovica-Gajic, "LayoutPilot: A lakehouse physical design advisor," *Proc. VLDB Endow.*, vol. 19, no. 12, pp. 4578–4581, 2026, doi: 10.14778/3827998.3828070.

[27] V. Venkatesan, "AutoCompactionAI: A reinforcement learning framework for adaptive file compaction in serverless data lakes," in *Proc. 2025 IEEE Int. Conf. Future Machine Learning and Data Science (FMLDS)*, 2025, pp. 566–571, doi: 10.1109/FMLDS67896.2025.00081.

[28] H. Chettaoui and F. Hkiri, "Correlation aware solution for merging small files based on similarity and clustering in HDFS," *J. Supercomput.*, vol. 81, no. 15, Art. 1458, 2025, doi: 10.1007/s11227-025-07946-9.

[29] J. Camacho-Rodríguez *et al.*, "LST-Bench: Benchmarking log-structured tables in the cloud," *Proc. ACM Manag. Data*, vol. 2, no. 1, pp. 1–26, 2024, doi: 10.1145/3639314.

*Software cited in the methodology (add to the paper's reference list when used):* Apache Spark documentation ("Performance Tuning — Spark SQL"), sparkMeasure (L. Canali, CERN) — add the exact URL and access date.

---

## Appendix B — Results CSV Schema

`results/raw/experiment_results.csv` — **34 columns**, append-only, header-guarded. *(Confirm the time units of the sparkMeasure fields against a known run before using them in the cost model.)*

| # | Column | Meaning | Source |
|---:|---|---|---|
| 1 | `run_id` | Unique id: `<action>_<workload>_<config>_rep<block>` | runner |
| 2 | `timestamp` | ISO time of the run | runner |
| 3 | `pipeline_version` | e.g. `v2` | pipeline |
| 4 | `workload_id` | e.g. `A1_large_high` | index |
| 5 | `experiment_action` | `A1`, `A1_sweep`, `A1_smoke_test`, later `A2` | runner |
| 6 | `config_label` | e.g. `shuffle50` | runner |
| 7 | `shuffle_partitions` | Setting under test | runner |
| 8 | `adaptive_enabled` | AQE flag (false) | config |
| 9 | `repetition_number` | Block number 1…B (0 = warm-up) | runner |
| 10 | `is_warmup` | Warm-up row flag (not persisted by default) | instrumentation |
| 11 | `data_volume` | small / medium / large | index |
| 12 | `file_count_level` | low / medium / high | index |
| 13 | `shuffle_volume_level` | low / high | index |
| 14 | `num_orders` | Rows in `orders` | index |
| 15 | `wall_clock_seconds` | Elapsed time of the action | timer |
| 16 | `sparkmeasure_available` | Whether JVM metrics were captured | instrumentation |
| 17 | `stage_count` | Stages executed | sparkMeasure |
| 18 | `task_count` | Tasks executed | sparkMeasure |
| 19 | `elapsed_time_ms` | sparkMeasure elapsed time | sparkMeasure |
| 20 | `stage_duration_ms` | Sum of stage durations | sparkMeasure |
| 21 | `executor_run_time_ms` | Sum of task run times | sparkMeasure |
| 22 | `executor_cpu_time_ms` | Executor CPU time | sparkMeasure |
| 23 | `jvm_gc_time_ms` | JVM garbage-collection time | sparkMeasure |
| 24 | `shuffle_total_bytes_read` | Shuffle bytes read | sparkMeasure |
| 25 | `shuffle_bytes_written` | Shuffle bytes written | sparkMeasure |
| 26 | `shuffle_records_written` | Shuffle records written | sparkMeasure |
| 27 | `memory_bytes_spilled` | Memory spilled | sparkMeasure |
| 28 | `disk_bytes_spilled` | Disk spilled | sparkMeasure |
| 29 | `peak_execution_memory` | Peak execution memory | sparkMeasure |
| 30 | `bytes_read` | Input bytes read | sparkMeasure |
| 31 | `records_read` | Input records read | sparkMeasure |
| 32 | `peak_memory_percent` | Max system RAM % during the run | psutil |
| 33 | `avg_cpu_percent` | Mean system CPU % during the run | psutil |
| 34 | `notes` | Free text (exclusions go in `exclusions.csv`) | — |

---

## Appendix C — Command Cheat-Sheet

```powershell
# --- one-time ---
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src/environment_report.py            # writes environment.md (or environment.generated.md)

# --- verification ---
python src/verify_phase2_setup.py           # expect PHASE 2 COMPLETE
python src/workload_generator.py            # generates the 10 workloads (seed 42)
python src/verify_workloads.py              # expect GATE PASSED

# --- experiments ---
mkdir logs                                  # once, for saved console logs
python src/explore_partition_curve.py       # exploratory sweep (informs the grid)
python src/run_a1_experiments.py --partitions 1 2 4 8 16 50 200 400 2>&1 | Tee-Object logs\a1_formal.log

# --- free-RAM check before any formal run ---
python -c "import psutil; print(psutil.virtual_memory().available/2**30)"

# --- Git ---
git add -A
git commit -m "Formal A1 run (pipeline v2)"
git tag a1-formal-v2
git push origin main --tags
```

---

## Appendix D — Workload Catalogue

All workloads: seed 42 · schema `orders(order_id, customer_id, product_category, amount, order_date)`, `customers(customer_id, region, segment)` · customers = `max(500, orders // 100)` (500 for small, 20 000 for large).

| Workload | Orders | Orders files | Size (MB) | % rows with amount > 0 (measured) | Used for |
|---|---:|---:|---:|---:|---|
| `A1_small_low` | 50 000 | 10 | 0.59 | 14.86 | A1 |
| `A1_small_high` | 50 000 | 10 | 0.59 | 84.85 | A1 |
| `A1_large_low` | 2 000 000 | 10 | 21.21 | 15.02 | A1 |
| `A1_large_high` | 2 000 000 | 10 | 25.68 | 85.04 | A1 |
| `A2_small_low` | 50 000 | **1** | 0.55 | 84.85 | A2 |
| `A2_small_medium` | 50 000 | **10** | 0.59 | 84.85 | A2 |
| `A2_small_high` | 50 000 | **50** | 0.77 | 84.85 | A2 |
| `A2_large_low` | 2 000 000 | **1** | 23.31 | 85.04 | A2 |
| `A2_large_medium` | 2 000 000 | **10** | 25.68 | 85.04 | A2 |
| `A2_large_high` | 2 000 000 | **50** | 28.25 | 85.04 | A2 |

*(Requested shuffle levels are 15 % / 85 %; the measured fractions differ slightly because of finite-sample randomness — the manifest records the measured values.)*

---

## Appendix E — Pilot and Sweep Result Tables

### E.1 v1 pilot (pipeline v1; 60 runs; **archived — not for conclusions**)

Median wall-clock seconds (CV in brackets):

| Workload | 50 partitions | 200 (default) | 400 |
|---|---:|---:|---:|
| A1_small_low | **1.10** (0.29) | 2.63 (0.11) | 4.32 (0.11) |
| A1_small_high | **0.87** (0.13) | 2.80 (0.15) | 4.62 (0.21) |
| A1_large_low | **1.20** (0.86) | 2.87 (0.15) | 4.51 (0.12) |
| A1_large_high | **1.73** (0.12) | 2.82 (0.27) | 4.82 (0.12) |

Task counts 57 / 207 / 407; shuffle written ≈ 9 / 18 / 29 KB — identical for all four workloads.

### E.2 v2 shuffle-scaling gate (study machine)

| Workload | Orders | Shuffle written (MB) |
|---|---:|---:|
| A1_small_low | 50 000 | 0.19 |
| A1_small_high | 50 000 | 0.85 |
| A1_large_low | 2 000 000 | 6.45 |
| A1_large_high | 2 000 000 | 35.39 |

### E.3 v2 exploratory sweep (3 blocks per cell; run with ≈ 2.3 GB free RAM — guidance only)

| Workload | Partitions | Median wall (s) | Min | Max | Median executor run (s) | Spill (MB) | Shuffle written (MB) |
|---|---:|---:|---:|---:|---:|---:|---:|
| A1_large_high | 1 | 2.14 | 2.13 | 2.29 | 3.24 | 0.0 | 36.35 |
| | 2 | 1.64 | 1.51 | 2.65 | 3.74 | 0.0 | 36.32 |
| | **4** | **1.34** | 1.26 | 2.58 | 4.36 | 0.0 | 36.26 |
| | 8 | 2.59 | 1.43 | 4.53 | 7.96 | 0.0 | 36.16 |
| | 16 | 1.77 | 1.53 | 3.62 | 5.72 | 0.0 | 35.96 |
| | 50 | 4.03 | 2.15 | 5.54 | 14.77 | 0.0 | 35.39 |
| | 200 | 9.54 | 3.74 | 11.47 | 21.57 | 0.0 | 34.64 |
| | 400 | 4.16 | 3.58 | 4.40 | 12.74 | 0.0 | 34.46 |
| A1_small_high | 1 | 0.22 | 0.18 | 0.27 | 0.13 | 0.0 | 0.84 |
| | **2** | **0.20** | 0.18 | 0.25 | 0.18 | 0.0 | 0.83 |
| | 4 | 0.24 | 0.20 | 0.25 | 0.29 | 0.0 | 0.82 |
| | 8 | 0.37 | 0.34 | 0.38 | 0.66 | 0.0 | 0.82 |
| | 16 | 0.49 | 0.43 | 1.01 | 1.07 | 0.0 | 0.83 |
| | 50 | 1.27 | 1.11 | 1.96 | 2.80 | 0.0 | 0.85 |
| | 200 | 2.44 | 2.39 | 2.98 | 7.17 | 0.0 | 0.94 |
| | 400 | 2.82 | 2.78 | 3.69 | 7.41 | 0.0 | 1.00 |

**Reading the table.** Small data is best at 1–2 partitions; large data at ≈ 4 (the machine's logical core count). Both worsen sharply toward 200 (default) and beyond; large-data cells at 8, 50 and 200 are noisy. Spill is zero everywhere.

---

*End of document. Update the status dashboard (§1.4), phase table (§17.1, §18) and the timeline (§19) after each phase; regenerate Appendix E when the formal A1 results are available.*
