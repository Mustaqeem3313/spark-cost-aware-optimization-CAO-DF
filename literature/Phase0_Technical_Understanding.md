# Phase 0 — Technical Understanding Document
### For: A Cost-Aware Decision Framework for Adaptive Optimization of Apache Spark Data Pipelines
### Purpose: personal study reference — you should be able to explain every section below without looking at notes.

---

## 1. Apache Spark Architecture

Spark runs as a **driver** process plus one or more **executor** processes.

- The **driver** builds your program's logical plan, converts it into a physical execution plan, and coordinates the executors. It's the "brain" — it doesn't process data itself.
- **Executors** are the workers. They run **tasks** — the smallest unit of work — in parallel, each task operating on one **partition** of data.
- A **cluster manager** (local threads in your case, or YARN/Kubernetes/standalone in production) allocates executor resources.

Spark decomposes your program into a hierarchy:
**Job → Stages → Tasks**

- A **job** is triggered by an action (e.g., `.count()`, `.write()`).
- A job is broken into **stages**, split wherever data needs to be **shuffled** (redistributed across the cluster).
- Each stage runs many **tasks** in parallel — one task per partition.

Why this matters for your research: your entire cost model is built on measuring *how many tasks ran, how long stages took, and how much data moved between them* — so understanding this hierarchy is what lets you read Spark's metrics correctly instead of just watching a wall-clock timer.

---

## 2. Spark Shuffle

A **shuffle** is when Spark has to move data between partitions across the cluster — because the next operation (a `join`, `groupBy`, `distinct`, `repartition`, etc.) needs data that shares a key to end up on the same task, and that data currently lives on different partitions.

Shuffle is expensive because it involves:
- **Writing** intermediate data to disk on the source side (shuffle write)
- **Transferring** it over the network (or, locally, between threads)
- **Reading** it back in on the destination side (shuffle read)

This is why shuffle-heavy operations (joins, aggregations) dominate Spark job cost, and why tuning shuffle behavior is a legitimate, high-leverage optimization target — it's the mechanism your A1 action is designed to act on.

**Why would changing shuffle partitions help?**
Because the number of shuffle partitions determines how the shuffled data is divided into tasks for the next stage. Too few partitions → each task handles too much data → memory pressure, spill to disk, poor parallelism. Too many partitions → each task handles too little data → task-scheduling overhead dominates actual work. There is a workload-dependent sweet spot, which is exactly what your experiment is designed to find.

---

## 3. Spark Partitions

Two distinct concepts share the word "partition" — don't conflate them:

1. **Data partitions**: how your input data is physically split (e.g., across Parquet files, or in-memory chunks). Determined by input file layout and Spark's read behavior.
2. **Shuffle partitions**: how many output partitions a shuffle stage produces. Controlled by `spark.sql.shuffle.partitions` (for DataFrame/SQL operations) — independent of how the input was partitioned.

Your A1 experiment is about (2), not (1) — you're not changing how the input data is laid out, you're changing how Spark redistributes it after a shuffle-triggering operation.

---

## 4. `spark.sql.shuffle.partitions`

This is the single configuration parameter your A1 action tunes.

- **Default value: 200** — a legacy default chosen for large clusters, often wrong for smaller data or smaller clusters.
- It sets the number of partitions Spark uses when shuffling data for joins and aggregations in DataFrame/SQL operations.
- **Important caveat you must control for**: Spark 3.x's Adaptive Query Execution (AQE) can *automatically* coalesce shuffle partitions at runtime, overriding this setting's effective behavior. For your controlled A1 experiment, AQE must be explicitly disabled (`spark.sql.adaptive.enabled=false`), or your manual tuning results will be confounded by AQE's own decisions.

**Why would changing shuffle partitions help?** (the question you should be able to answer without notes)
> Because the right number of shuffle partitions depends on how much data is being shuffled. A small shuffle spread across 200 partitions creates mostly empty, wasteful tasks — scheduling overhead exceeds actual work. A large shuffle squeezed into too few partitions creates oversized tasks that spill to disk and can bottleneck on a small number of slow tasks. The optimal setting scales with shuffle volume, which is exactly why this needs to be workload-aware rather than a fixed default.

---

## 5. Parquet

Parquet is a **columnar** storage format, and it's the format your entire pipeline reads and writes.

Key properties relevant to your study:
- **Columnar**: stores data column-by-column, not row-by-row — enables reading only the columns a query needs (projection pushdown) and better compression.
- **Splittable**: a single large Parquet file can be read by multiple tasks in parallel, unlike some compressed formats.
- **Self-describing**: embeds schema and per-column statistics (min/max), enabling predicate pushdown (skipping row groups that can't match a filter).
- **File-count sensitive**: Parquet's benefits erode when data is spread across too many small files — which is precisely the small-file problem your A2 action addresses.

---

## 6. The Small-File Problem

When a table (or a directory of Parquet files) accumulates many small files — often from frequent small writes, streaming micro-batches, or over-partitioned writes — several costs compound:

- **Driver-side overhead**: listing, opening, and reading metadata for thousands of small files is expensive before any actual data processing begins.
- **Task-scheduling overhead**: Spark often assigns roughly one task per file (or file-split); many tiny files mean many tiny tasks, and per-task scheduling overhead starts to dominate actual compute time.
- **Poor I/O efficiency**: small reads don't amortize the fixed cost of opening a file/connection the way large sequential reads do.

This is a well-documented, industry-recognized problem (it's the whole motivation behind AutoComp and Delta Lake's `OPTIMIZE` command, both in your Related Work).

---

## 7. File Compaction

Compaction is the fix for the small-file problem: **rewrite many small files into fewer, larger files** (typically targeting a size like 128MB–1GB per file, aligned with the storage/processing engine's optimal read size).

**Why would file compaction help?** (the question you should be able to answer without notes)
> Because reading N small files costs roughly N times the fixed per-file overhead (open, list, schedule a task) regardless of how little data each file holds. Merging them into fewer, larger files preserves the total data volume but sharply cuts the number of fixed-overhead operations, so subsequent reads become both faster and cheaper — at the one-time cost of the compaction job itself, which is exactly the trade-off your OBEP (Optimization Break-Even Point) model is built to evaluate.

Compaction is not free — it's itself a Spark job that reads and rewrites data, consuming compute and (if applicable) cloud cost. That upfront cost is `Copt` in your cost model.

---

## 8. Spark Execution Metrics

These are the raw signals you'll extract from every experimental run (via Spark's event log / `sparkmeasure`, not just wall-clock time):

| Metric | What it tells you |
|---|---|
| **Stage/task duration** | Where time is actually spent (not just total wall time) |
| **Shuffle read/write bytes** | How much data moved during a shuffle — the core cost driver for A1 |
| **Input/output bytes** | Data volume actually processed |
| **Task count** | Parallelism achieved — too low or too high both hurt |
| **Spill (memory/disk)** | Sign that a task/partition was oversized for available memory — a red flag for badly-tuned partitioning |
| **GC time** | Garbage collection overhead — high GC time often signals memory pressure |

Wall-clock time alone tells you *that* something changed; these metrics tell you *why* — which is what your paper needs to argue convincingly.

---

## 9. Cloud Compute Pricing (for cost estimation)

Since you're running locally and estimating cost (not billing real cloud spend), you need to understand the pricing *models* you'll be approximating:

- **On-demand hourly/per-second billing**: cloud providers charge per vCPU-hour or per instance-hour (e.g., AWS EC2), or per managed-processing-unit-hour (e.g., AWS Glue's DPU-hour, Databricks' DBU-hour).
- Your cost estimate will take a **measured resource consumption** (CPU-seconds × cores used, or wall-clock time × instance size) and multiply it by a **published, cited price** for an equivalent cloud resource — explicitly documented as an estimate, not a bill.

---

## 10. Optimization Cost (Copt, Crun)

The two cost terms your entire framework is built on:

- **Copt** = the one-time cost of *applying* an optimization action (running the tuning search, or running the compaction job itself).
- **Crun** = the cost of *one subsequent execution* of the pipeline, after the optimization has been applied (vs. the baseline/static cost per execution).

The framework's core comparison, for N future executions:
```
TC_optimized = Copt + N × Crun
TC_static    = N × Crun_static
```
Apply the optimization when `TC_optimized < TC_static`.

---

## 11. Break-Even Analysis (OBEP)

Given `Copt` and the per-execution savings (`Crun_static − Crun_optimized`), the **break-even point** is the number of future executions, N, at which cumulative savings exactly offset the one-time optimization cost:

```
N > Copt / (Crun_static − Crun_optimized)
```

Below this N, the optimization is not economically justified (you pay the upfront cost but never earn it back); above it, the optimization is worthwhile. This is the single output your paper is built to produce, for both A1 and A2, across varying workload conditions.

---

## 12. Experimental Design Principles (why they matter here specifically)

- **Control confounds**: hold everything constant except the one variable you're testing (this is why AQE must be explicitly disabled for A1 experiments — otherwise you don't know if a result comes from your tuning or from AQE's own auto-coalescing).
- **Repeat and report variability**: a single run is a sample of one; Spark timing has natural noise (JIT warm-up, OS scheduling, GC pauses). Report median/IQR across repeated runs, not a single number.
- **Randomize run order**: prevents systematic drift (thermal throttling, background load accumulating over a session) from masquerading as a treatment effect.
- **Warm up before measuring**: discard the first run(s) in any session to avoid JVM/JIT startup cost contaminating your "cold" configuration's numbers.
- **Document the environment**: hardware, OS, Java/Spark/Python versions — without this, nobody (including future-you) can tell whether a result is about your framework or about your laptop.

---

*This document is a personal study reference for Phase 0. You should now be able to explain, without notes: why shuffle-partition tuning helps, why file compaction helps, and why AQE, warm-up runs, and run-order randomization all matter to getting a credible result.*
