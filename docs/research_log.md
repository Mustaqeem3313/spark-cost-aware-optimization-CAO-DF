# CAO-DF Research Log

Running record of every experiment run, finding, mechanism, anomaly, limitation and
decision in the study, kept so that the paper can be written without losing context.
Entries are dated; numbers are copied from the committed result files named in each entry.

Conventions: wall-clock medians over blocks; CV = stdev / mean; "default" or "static
baseline" = `spark.sql.shuffle.partitions = 200`, no compaction; pipeline v2 (frozen).

---

## 0. Study machine and fixed configuration

| Item | Value |
|---|---|
| CPU | Intel Core i5-1335U — 10 cores (2 P + 8 E), 12 threads |
| RAM / disk | 15.7 GB / NVMe SSD |
| OS | Windows 11 |
| Software | Python 3.11.9, Temurin JDK 17.0.20.1, PySpark 3.5.1, sparkMeasure 0.28, winutils |
| Spark | `local[*]`, driver 3g, AQE off, broadcast join off (threshold −1), settings read back and verified |
| Pipeline v2 | filter `amount > 0` → sort-merge join on `customer_id` → groupBy(`customer_id`, `region`, `product_category`) → `noop` sink; 2 Exchanges |
| Data | Synthetic, seed 42; small = 50 000 orders, large = 2 000 000 orders |
| Full record | `environment.md` |

The study machine changed from the original 4-thread / 7.9 GB laptop (v1 pilot) to the
machine above on 2026-10-20; all formal results come from the new machine.

---

## 1. Chronology

| Date | Event | Evidence |
|---|---|---|
| (earlier) | v1 pilot (E0, 60 runs): broadcast join left a ~10 KB shuffle no factor could move → pipeline redesigned (v2) | `archive/pilot_v1/` |
| 2026-10-20 | Environment rebuilt on the study machine; setup check, workload regeneration and shuffle-scaling gate (E1) re-passed | `environment.md`, commit `677663d` |
| 2026-10-21 | E2 exploratory sweep (9 settings incl. 12, 3 blocks, seed 7) | `logs/a1_sweep.log`, `results/raw/pilot_a1_v2_sweep.csv` |
| 2026-10-21 | **E3 formal A1** — 4 workloads × 8 settings × 5 blocks = 160 measured runs (seed 123), ~15 min | `logs/a1_formal.log`, tag `a1-formal-v2` |
| 2026-10-21 | E4 A2 pilot — 6 workloads × 3 stages × 3 blocks = 54 measured runs (seed 456); gate **PASSED** | `logs/a2_pilot.log`, `results/raw/pilot_a2.csv` |
| 2026-10-21 | **E5 formal A2** — 6 workloads × 3 stages × 5 blocks = 90 measured runs | `logs/a2_formal.log` |
| 2026-10-23 | **E6 framework validation** — 4 unseen workloads × 10 stages × 3 blocks = 120 measured runs (seed 789), ~15 min | `logs/validation.log`, `experiment_action = VAL` |

### Workload shuffle-scaling gate (E1, study machine)

Shuffle-volume ratios between factor levels: 27.6×, 38.1×, 3.9×, 5.5×; the largest workload
shuffles 35.1 MB. Workloads on disk: small ≈ 0.6–0.8 MB, large ≈ 21–28 MB (large files are
~1.5 % smaller than on the old machine because of a different file split; data identical).

---

## 2. A1 — shuffle-partition tuning (E3, formal)

Median wall-clock (s), 5 blocks:

| Workload | 1 | 2 | 4 | 8 | 16 | 50 | **200** | 400 |
|---|---|---|---|---|---|---|---|---|
| large_high | 1.236 | 0.934 | **0.780** | 0.911 | 1.262 | 2.111 | 6.201 | 1.693 |
| large_low | 0.480 | 0.469 | **0.457** | 0.609 | 0.820 | 1.622 | 3.870 | 1.310 |
| small_high | **0.285** | 0.351 | 0.349 | 0.402 | 0.410 | 0.716 | 3.227 | 0.967 |
| small_low | 0.258 | **0.253** | 0.303 | 0.282 | 0.441 | 0.806 | 2.702 | 0.978 |

| Workload | Best (wall) | Speed-up vs 200 | Best (executor time) |
|---|---|---|---|
| large_high | 4 | 7.95× | 1 |
| large_low | 4 | 8.47× | 1 |
| small_high | 1 | 11.31× | 1 |
| small_low | 2 | 10.66× | 1 |

### Findings

- **F-A1-1 — Default is the worst setting tested.** The static baseline (200) is slowest on
  every workload; the best setting is 7.9–11.3× faster.
- **F-A1-2 — Optimum is tiny and grows with data volume.** Optimum 1–2 partitions for small
  data, 4 for large data — far below the 12 hardware threads. Supports H1 (benefit depends on
  workload characteristics).
- **F-A1-3 — Non-monotonic curve: 400 beats 200 (≈3.6× on large_high).** Mechanism: Spark's
  `spark.shuffle.sort.bypassMergeThreshold` defaults to 200. With ≤ 200 reduce partitions (and
  no map-side aggregation) Spark uses `BypassMergeSortShuffleWriter`, which opens one file per
  reduce partition per map task and then concatenates them — expensive on Windows/NTFS. Above
  200 it switches to the sort-based (unsafe) writer. So the default sits exactly at the most
  expensive point of the writer choice. Paper: explain in Results/Discussion; cite Spark source/docs.
- **F-A1-4 — Executor time prefers 1 partition everywhere**, wall-clock prefers 1–4: extra
  partitions buy parallelism (lower wall-clock) at a higher summed executor cost. Matters for
  the cost model: the cost proxy (wall-clock × cores vs executor time) can change the optimum.
- **F-A1-5 — Shuffle bytes are constant across partition settings within a workload; spill is
  0 everywhere.** The A1 effect is task/file overhead, not data volume or memory pressure.
- Sweep (E2) agrees with E3 (400 < 200; 8 ≈ 12).

### Noise (A1)

5 of 32 cells exceed CV 0.20, all at 1–2 partitions, each caused by one slow block
(e.g. large_high p=1: 2.53, 1.21, 1.13, 1.24, 2.32 s). Sub-second medians; likely hybrid
P-core/E-core scheduling by Windows. Reported as is (outliers disclosed, never dropped).

**Robustness check (`src/robustness_check.py` → `results/processed/a1_outlier_robustness.csv`).**
Dropping the slowest run in each noisy cell (sensitivity analysis only — headline numbers keep
all runs):

| Workload | Best (all runs) | Best (trimmed) | Speed-up all → trimmed | Rank corr. |
|---|---|---|---|---|
| large_high | 4 | 4 | 7.95× → 7.95× | 0.976 |
| large_low | 4 | 2 | 8.47× → 8.64× | 0.976 |
| small_high | 1 | 1 | 11.31× (no noisy cells) | 1.000 |
| small_low | 2 | 2 | 10.66× → 10.74× | 1.000 |

- **F-A1-6 — The optimum is a plateau, not a point.** On large_low, 1, 2 and 4 partitions are
  within 20 ms (medians 0.473 / 0.469 / 0.457 s), so the outlier only decides which member of
  a tie "wins". Rankings are otherwise unchanged (ρ ≥ 0.976) and every speed-up stays ≥ 7.9×.
  Paper: report the optimum as the best setting *and* the set of settings within the noise
  floor; the decision framework must treat differences below the noise floor as ties.

---

## 3. A2 — file compaction

Design (master document §13.3, unchanged): per block, per workload, fixed sequence
fragmented (before) → compaction (`Copt`) → compacted (after); no input caching (fresh
`spark.read` per run, so file listing is timed); shuffle partitions held at 200 (static
baseline) so A2 is not a joint arm; compaction = `coalesce(ceil(size / 128 MB))` → 1 file
at current sizes, written to `data/compacted/<workload>`. Benefit is computed per block
(paired) and summarised by its median.

### E4 pilot (3 blocks) — gate PASSED

| Workload | Files | Before (s) | After (s) | `Copt` (s) | Benefit | Blocks faster | N* (wall) |
|---|---|---|---|---|---|---|---|
| small_low | 1 | 1.069 | 1.102 | 0.141 | −3.2 % | 1/3 | ∞ |
| small_medium | 10 | 2.982 | 1.082 | 0.222 | +63.7 % | 3/3 | 1 |
| small_high | 50 | 3.405 | 1.256 | 0.268 | +66.7 % | 3/3 | 1 |
| large_low | 1 | 5.296 | 5.380 | 1.047 | −0.4 % | 1/3 | ∞ |
| large_medium | 10 | 6.339 | 4.911 | 1.109 | +21.8 % | 3/3 | 1 |
| large_high | 50 | 6.940 | 4.578 | 1.177 | +32.6 % | 3/3 | 1 |

### Findings (pilot; confirm with E5)

- **F-A2-1 — Compaction pays off immediately for fragmented data**: `Copt` (0.2–1.2 s) is
  smaller than the per-run saving (1.4–2.3 s), so N* = 1. For already-compact data (1 file)
  compaction is a no-op rewrite with no benefit → N* = ∞ (framework must say "do nothing").
- **F-A2-2 — Mechanism is scan-task count × shuffle writer, not file-open cost.** With
  `maxPartitionBytes` 128 MB and `openCostInBytes` 4 MB, Spark packs 10 or 50 small files into
  ~12 scan tasks (= default parallelism), whereas 1 file gives 1 scan task. Each map task then
  writes 200 shuffle files under the bypass-merge writer (F-A1-3). Executor run time rises
  ~7× (large: 59 s fragmented vs 8.6 s compacted; small: 31 s vs 4 s) while wall-clock rises
  less because tasks run in parallel. **A2's benefit is therefore partly an interaction with
  the static shuffle setting** — report explicitly; it is how small files hurt in practice
  (more tasks) but its size here depends on 200 partitions.
- **F-A2-3 — Effect saturates between 10 and 50 files** (medium ≈ high, especially for small
  data) because both exceed the 12 scan slots. H1(A2) "monotonic in file count" holds only
  weakly (low < medium ≤ high).
- **F-A2-4 — Relative benefit is larger for small data** (64–67 %) than large (22–33 %):
  fixed per-task overhead dominates small inputs.

### Noise (A2 pilot)

7 of 18 cells exceed CV 0.20 — mostly the sub-second compaction cells and small_* stages
with 3 blocks. Paired per-block benefit is consistent (3/3) where it matters.

### E5 formal (5 blocks, seed 456) — confirms the pilot

| Workload | Files | Before (s) | After (s) | `Copt` (s) | Benefit | Blocks faster | N* (wall) | N* (executor time) |
|---|---|---|---|---|---|---|---|---|
| small_low | 1 | 1.175 | 1.251 | 0.142 | −1.7 % | 1/5 | ∞ | ∞ |
| small_medium | 10 | 3.200 | 1.118 | 0.182 | +66.2 % | 5/5 | 1 | 1 |
| small_high | 50 | 3.541 | 1.154 | 0.312 | +66.4 % | 5/5 | 1 | 1 |
| large_low | 1 | 5.227 | 5.237 | 1.035 | −0.3 % | 2/5 | ∞ | 11 (spurious) |
| large_medium | 10 | 6.390 | 4.920 | 1.256 | +24.6 % | 5/5 | 1 | 1 |
| large_high | 50 | 7.030 | 4.626 | 1.288 | +32.3 % | 5/5 | 1 | 1 |

Median executor run time, fragmented → compacted: large 61–62 s → 9 s; small 31–32 s → 4 s.
Shuffle MB is ~1–1.3 % higher when fragmented (more map outputs); spill 0 everywhere.

- F-A2-1 to F-A2-4 confirmed. Benefit for 1-file workloads is within noise (≤ 2 %, mixed
  direction): compaction of compact data is pure cost.
- **Anomaly:** large_low executor-time N* = 11 comes from a 0.085 s executor-time "benefit"
  that is noise (wall-clock benefit −0.3 %). The decision framework needs a minimum-benefit
  (noise-floor) threshold so it does not recommend compaction on noise.
- **Monotonicity (H1-A2):** large: 0 % → 24.6 % → 32.3 % (monotonic); small: −1.7 % → 66.2 %
  → 66.4 % (saturated after 10 files).

### Noise (A2 formal)

6 of 18 cells exceed CV 0.20: all compaction cells under 0.35 s or 1-file small cells, except
large_high compaction (CV 0.58 from one outlier; IQR only 0.034 s). All benefit cells with
an effect are 5/5 consistent.

---

## 3b. Cost model and break-even (Phase 8)

`src/cost_model.py`; assumptions in `Cost_Model_Assumptions.md`. Price: EC2 on-demand m6i,
Linux, Mumbai (ap-south-1), **USD 0.0505 per vCPU-hour** (accessed 23 Oct 2026; AWS publication
2026-10-22). P1 = wall-clock × 12 vCPUs × p / 3600; P2 = executor run time × p / 3600. Per-run
values are cell medians. Outputs: `results/processed/cost_per_cell.csv`, `breakeven_table.csv`,
`cost_sensitivity.csv`; `results/figures/total_cost_curves_{p1,p2}`.

| Workload | Action | Saving P1 | Saving P2 | Copt / Crun_static (P1) | N* P1 | N* P2 |
|---|---|---|---|---|---|---|
| A1 large_high | A1 search (4 / 1 partitions) | 87.4 % | 95.9 % | 2.44 | 3 | 3 |
| A1 large_low | A1 search (4 / 1) | 88.2 % | 97.4 % | 2.49 | 3 | 3 |
| A1 small_high | A1 search (1 / 1) | 91.2 % | 98.6 % | 2.08 | 3 | 2 |
| A1 small_low | A1 search (2 / 1) | 90.6 % | 98.5 % | 2.23 | 3 | 2 |
| A2 large_low (1 file) | A2 | −0.2 % | 0.9 % | 0.20 | ∞ | 11 (noise) |
| A2 large_medium (10) | A2 | 23.0 % | 85.1 % | 0.20 | 1 | 1 |
| A2 large_high (50) | A2 | 34.2 % | 85.9 % | 0.18 | 1 | 1 |
| A2 small_low (1 file) | A2 | −6.5 % | −6.6 % | 0.12 | ∞ | ∞ |
| A2 small_medium (10) | A2 | 65.1 % | 88.4 % | 0.06 | 1 | 1 |
| A2 small_high (50) | A2 | 67.4 % | 87.4 % | 0.09 | 1 | 1 |

A1 rule-based (Copt = 0): N* = 1 everywhere.

- **F-CM-1 — Even a full search pays off within three runs.** The 8-setting search costs
  2.1–2.5 static runs (P1; 1.8–2.2 under P2) and the chosen setting saves 87–99 % per run, so
  N* = 3 (P1) or 2–3 (P2). At N = 50 total cost falls 82.5–87.0 % (P1) / 91.9–95.1 % (P2).
- **F-CM-2 — Compaction pays off at the first run on fragmented data.** Copt is 6–20 % of one
  fragmented run (P1), < 2 % (P2). Single-file inputs: never (N* = ∞) — the only finite value
  (large_low, P2, N* = 11) is a 0.9 % saving below the static IQR (flagged
  `saving_exceeds_static_iqr = False`). Confirms the need for the Phase 9 noise floor.
- **F-CM-3 — N* is price-invariant.** Price × 0.5 / 1.5 changes no N* and no decision
  (`cost_sensitivity.csv`); absolute savings scale linearly.
- **F-CM-4 — Proxies agree on N* for 11 of 14 pairs** and, after discarding savings within
  noise, on whether to act at every N in {1, 5, 10, 50}. They differ in (i) the A1 setting —
  P2 always picks 1 partition, P1 picks 1–4 (F-A1-4) — and (ii) the size of the A2 saving —
  P2 85–88 % vs P1 23–67 %, because fragmentation multiplies executor task-seconds (F-A2-2)
  more than wall-clock time.
- **Note on A2 numbers.** The cost model uses differences of stage medians; §3 benefit figures
  use the median of per-block paired differences (e.g. large_high 34.2 % vs 32.3 %;
  small_low −6.5 % vs −1.7 %). Same sign everywhere; state which is used in each table.
- **Absolute amounts are tiny** (static run 0.02–0.12 US cents under P1) because the data are
  laptop-scale; report relative savings and N*.

---

## 3c. Decision framework and validation (Phase 9, E6)

Code: `src/decision_framework.py` (rule + validation), `src/run_validation.py` (E6 runner),
`src/workload_generator.py --validation` (unseen workloads), `src/make_figures.py` (paper
figures). Outputs: `results/processed/validation_cell_summary.csv`, `validation_decisions.csv`,
`validation_summary.csv`, `framework_loo.csv`; figures `fig_validation`, `fig_breakeven_map`.

**Rule (decided with the author).** For a new workload, describe it by (log orders, log files,
shuffle fraction), each scaled by its range in the study. For each action take the **nearest
measured study workload** (Euclidean; ties averaged) and copy its saving per run *s* and its
one-time cost ratio *r* = Copt / Crun_static. Predicted total cost in static-run units: do
nothing = N; action = r + N(1 − s). An action is considered only if *s* ≥ **δ = 10 %** of a static
run (noise floor). Pick the lowest predicted cost; ties go to "do nothing". One action at a time
(D3). Baseline "performance-only" picks the largest estimated saving and ignores Copt; the
"oracle" picks the cheapest option using the measured validation costs (hindsight).

**Validation workloads (new seeds 701–704, factor values never used in the study):**

| Workload | Orders | Files | Shuffle fraction | Size |
|---|---|---|---|---|
| V_medium_1f_s85 | 500 K | 1 | 0.85 | 5.7 MB |
| V_medium_25f_s50 | 500 K | 25 | 0.50 | 6.5 MB |
| V_small_25f_s15 | 50 K | 25 | 0.15 | 0.65 MB |
| V_large_5f_s50 | 2 M | 5 | 0.50 | 21.6 MB |

Each block runs every workload with all 8 partition settings, the compaction job and the
compacted run (200 partitions), in shuffled order, fresh read each run (no caching), 1 discarded
warm-up per measured run. Measured "tune" option = search over all 8 settings (Copt = sum) then
the best setting; "compact" = compaction job + compacted run; "nothing" = 200 partitions.
2 of 40 cells have CV > 0.2 (V_medium_1f shuffle8 0.21, V_medium_25f compaction 0.26).

**Results (16 cases per proxy = 4 workloads × N ∈ {1, 5, 10, 50}):**

| Proxy | Strategy | Win / tie / loss vs static | Mean cost reduction (95 % bootstrap CI) | Total-cost reduction | Matches oracle | Max regret |
|---|---|---|---|---|---|---|
| P1 | CAO-DF | 13 / 1 / 2 | 47.3 % (30.5 – 62.3) | 74.1 % | 14 / 16 | 23.9 % |
| P1 | performance-only | 12 / 0 / 4 | 6.5 % (−44.0 – 50.1) | 71.6 % | — | — |
| P2 | CAO-DF | 15 / 1 / 0 | 72.2 % (59.6 – 81.8) | 87.9 % | 14 / 16 | 13.6 % |
| P2 | performance-only | 12 / 0 / 4 | 23.2 % (−23.2 – 62.6) | 83.6 % | — | — |

Oracle mean reduction: 49.7 % (P1), 72.6 % (P2). Win/tie uses |ΔTC| ≤ δ · TC_static as a tie.

- **F-FW-1 — H2 supported.** CAO-DF is no worse than static in 14/16 (P1) and 16/16 (P2)
  cases, lower on average, and the bootstrap CI excludes 0 under both proxies. It is within
  2.4 points (P1) / 0.4 points (P2) of the hindsight oracle on average.
- **F-FW-2 — The value is in the one-time cost.** Performance-only always picks "tune" (largest
  saving) and is 2.4–2.7× the static cost at N = 1 (P1) — the search costs more than one run
  saves. CAO-DF avoids all four of these losses. For N ≥ 5 performance-only also beats static,
  but where cheap compaction is enough CAO-DF saves more (e.g. 50 K / 25 files, N = 5, P1:
  57.4 % vs 41.9 %).
- **F-FW-3 — Choices switch with N as the theory predicts:** compaction (cheap, smaller saving)
  for few runs, tuning (expensive search, larger saving) for many; "do nothing" on the 1-file
  workload at N = 1.
- **F-FW-4 — Tuning estimates transfer well; compaction estimates under P1 do not.** Estimated
  vs measured tuning saving differs by ≤ 3.4 points on every unseen workload. Compaction under
  P2 is over-estimated by 6–17 points but keeps the right sign. Under P1, compaction on the
  500 K / 25-file and 2 M / 5-file workloads saved −1.7 % and 3.3 % of wall-clock time
  (estimated 34 % and 23 %), so CAO-DF's two P1 losses (N = 1, regret 14.5 % and 23.9 %) both
  come from compaction. On the 50 K / 25-file workload compaction saved 59 % (estimated 67 %).
  Interpretation: compaction always cuts executor task time (P2) but whether that shows up as
  wall-clock time depends on how much of the run it leaves idle cores — not captured by the
  three features. Report as an observed limit, not an explanation.
- **F-FW-5 — Best partition count on unseen data is again small:** 2 (three workloads) or 4
  (wall-clock); 1 for executor time on all four — consistent with F-A1-4.
- **F-FW-6 — Leave-one-out on the study workloads: 80 % agreement (64/80).** All 16
  disagreements are the two 1-file A2 workloads, whose nearest neighbour (10 files) gains from
  compaction — the rule cannot extrapolate to the edge of the study range. On the unseen 1-file
  workload the A2 neighbour is a 1-file workload and compaction was correctly rejected.
- **F-FW-7 — Robust to δ.** δ ∈ {0, 5 %, 20 %} gives identical choices; only the win/tie label
  of one P1 case changes at 20 %.
- **Rule-based tuning (Copt = 0, neighbour's partition setting):** 82–87 % (P1) / 94–98 % (P2)
  cost reduction at every N — reported as the lower-bound sensitivity, as in Phase 8.

---

## 4. Limitations register (for the Threats to Validity section)

| # | Limitation | Affects | Mitigation / wording |
|---|---|---|---|
| L1 | Single local machine (`local[*]`), not a cluster; no network shuffle | External validity | State explicitly; relative effects and break-even logic transfer, absolute times do not |
| L2 | Windows/NTFS file-creation cost amplifies bypass-merge-writer overhead | A1 magnitude, A2 mechanism | Report mechanism; expect smaller (not reversed) effect on Linux/HDFS/S3 |
| L3 | Hybrid P/E-core scheduling → sporadic slow runs at 1–2 partitions | A1 noise (5 cells) | Medians, IQR, randomized blocks, outliers disclosed; trimmed sensitivity check (F-A1-6) leaves conclusions unchanged |
| L4 | Small datasets (≤ 28 MB on disk, ≤ 35 MB shuffled) | Generality | Optimum partition count will grow with data; framework uses measured/estimated costs, not fixed values |
| L5 | OS file cache warm in A2 (warm-up runs) | A2 read-side cost | Acknowledged in design (§13.3); compaction benefit is a lower bound on cold reads |
| L6 | A2 benefit partly mediated by 200 shuffle partitions (F-A2-2) | A2 interpretation | Report as interaction; no joint arm by design (D3) → future work |
| L7 | AQE disabled (would coalesce partitions automatically) | Practical relevance of A1 | Justified for control; AQE arm = future work |
| L8 | Synthetic data, one seed for formal workloads | Generality | Validation workloads use different seeds |
| L9 | Costs are estimated from published pricing, never billed | Cost model | Label consistently; sensitivity ±50 % |
| L10 | Framework validated on only 4 unseen workloads × 3 blocks (16 cases per proxy) and learned from 4 (A1) / 6 (A2) study workloads | Strength of H2 evidence | Bootstrap CI, LOO check, δ sensitivity; call it a first validation |
| L11 | Nearest-neighbour estimate cannot extrapolate beyond the study range (LOO misses at 1 file) and misses wall-clock compaction savings (F-FW-4) | Framework accuracy | Report both failure cases and the regret; richer features / more training workloads = future work |
| L12 | A1 study ratios come from runs with cached input; validation runs read fresh each time | Transfer of A1 estimates | Part of the generalisation test; estimates still within 3.4 points (F-FW-4) |
| L13 | The measured "tune" option assumes a full 8-setting search each time; a cheaper search (or AQE) would lower Copt | A1 cost | Rule-based lower bound reported alongside |

---

## 5. Decisions log (study-machine phase)

| Date | Decision | Rationale |
|---|---|---|
| 2026-10-20 | Formal A1 grid {1, 2, 4, 8, 16, 50, 200, 400} | Span under-parallel → default → over-partitioned |
| 2026-10-22 | Keep the 5 noisy A1 cells; do not rerun *(recommended; final say with the author)* | Single-run outliers; trimmed sensitivity check changes no conclusion (F-A1-6); protocol says disclose, not drop |
| 2026-10-22 | Literature matrix: fix [20], [22]–[24]; add [25]–[29] | Metadata aligned with verified Appendix A; DOIs checked on Crossref |
| 2026-10-21 | A2 shuffle partitions held at 200 | Static baseline; keeps A2 separate from A1 (D3) |
| 2026-10-21 | Compaction via `coalesce`, not `repartition` | Matches bin-pack compaction (no full shuffle) → realistic `Copt` |
| 2026-10-21 | No extra 500-file level | Pilot gate passed; master plan says add only if it fails |
| 2026-10-21 | Same GitHub repository for all phases | Single reproducible artifact; tags per milestone |

## 6. Open questions

- ~~Cloud pricing basis~~ — resolved 2026-10-22/23 (EC2 m6i, Mumbai).
- Cost proxy: wall-clock × cores vs executor time (F-A1-4 shows they disagree on the A1 optimum).
- ~~A1 `Copt` definition~~ — resolved 2026-10-23 (search primary, rule sensitivity).
- Licence; co-authorship of the guide.

- **2026-10-22 — Pricing basis:** AWS EC2 on-demand vCPU-hour chosen by the author (simplest, most transparent; Glue/DBU may appear only as discussion).
- **2026-10-23 — A1 Copt definition:** primary = search-based (one measured run of each candidate shuffle-partition setting, i.e. the cost of discovering the optimum); sensitivity = rule-based lower bound (Copt ~ 0, N* = 1). Chosen by the author; mirrors A2 where Copt is the measured compaction job.
- **2026-10-23 — Reference verification (paper/references.bib):** every entry checked against Crossref, arXiv or the publisher page. Corrected: [3] LOCAT pp. 674–684 (not 1345–1358); [5] Lyu et al. vol. 17 no. 11 pp. 3565–3579 (not no. 12, –3578); [19] OptEx DOI 10.1109/CCGrid.2016.10 (.111 belongs to an unrelated paper). Added missing DOIs/pages for [1], [4], [6], [8], [9], [15], [22], [24]. [16] (Gilad blog) could not be located and was removed from the paper. Appendix A updated accordingly.
- **2026-10-23 — Paper draft:** IEEEtran skeleton in `paper/`; Sections I–V and VIII drafted; Results, Discussion and Conclusion are placeholders until Phases 8–10. Compiles cleanly (tested with Tectonic 0.17).
- **2026-10-23 — Phase 9 design (author):** nearest measured workload for estimates; noise floor δ = 10 % of a static run; validation on 4 unseen workloads × 3 blocks; paper limited to 8 pages including references, plain language, more figures.
- **2026-10-23 — Phase 9 complete:** E6 run (120 runs), framework validation, H2 supported (F-FW-1). Paper figures now produced by `src/make_figures.py`.
- **2026-10-23 — Phase 8 complete:** region/instance for pricing = m6i, Asia Pacific (Mumbai), chosen by the author; P1 charges 12 vCPUs (the study machine's task slots). Cost model, assumptions file, break-even table, sensitivity and total-cost figures produced; Results §VI-C drafted.
