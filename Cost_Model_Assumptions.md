# Cost Model Assumptions (Phase 8)

All costs in this project are **estimated from published cloud pricing; nothing was billed.**
Implementation: `src/cost_model.py`. Outputs: `results/processed/cost_per_cell.csv`,
`breakeven_table.csv`, `cost_sensitivity.csv`, figures `results/figures/total_cost_curves_{p1,p2}`.

## 1. Price

| Item | Value |
|---|---|
| Provider / product | AWS EC2, On-Demand |
| Instance family | m6i (general purpose, Intel x86), Linux, shared tenancy |
| Region | Asia Pacific (Mumbai), `ap-south-1` |
| Price per vCPU-hour, `p` | **USD 0.0505** (identical for every m6i size, e.g. m6i.large 2 vCPU USD 0.101/h, m6i.xlarge 4 vCPU USD 0.202/h, m6i.4xlarge 16 vCPU USD 0.808/h) |
| Source | Amazon EC2 On-Demand Pricing, <https://aws.amazon.com/ec2/pricing/on-demand/> (values read from the public price file that backs this page; AWS publication timestamp 2026-10-22T23:14:53Z) |
| Accessed | 23 October 2026 |

Chosen by the author (research log, 2026-10-22 and 2026-10-23). Glue DPU-hour or Databricks DBU
pricing may be mentioned in the discussion only.

## 2. Cost proxies

| Proxy | Formula | Meaning |
|---|---|---|
| **P1, time-based** | `C = wall_clock_s × v × p / 3600`, with `v = 12` | Renting a machine with the study machine's 12 task slots for as long as the job runs |
| **P2, resource-based** | `C = executor_run_time_s × p / 3600` | Paying only for the core time used by tasks (closer to per-task / serverless billing) |

`v = 12` because Spark `local[*]` on the study machine (Intel i5-1335U) runs 12 task slots, one
per hardware thread, and one AWS vCPU is one hardware thread. The study machine is *not* an
m6i instance; the price is a transparent scalar for converting time into money, not a claim
that the laptop's speed equals the cloud instance's.

## 3. Action costs

| Action | `Copt` | `Crun(a)` | `Crun_static` |
|---|---|---|---|
| **A1_search** (primary) | Sum of one run at each of the K = 8 candidate settings {1, 2, 4, 8, 16, 50, 200, 400}, counted as pure overhead (conservative: those runs also produce output) | Run at the setting with the lowest median cost *under the same proxy* | 200 partitions |
| **A1_rule** (sensitivity, lower bound) | 0 (setting chosen from workload features without search) | As above | 200 partitions |
| **A2** | Measured compaction job (read fragmented data, write 1 compacted file) | Run on compacted data | Run on fragmented data, 200 partitions |

## 4. Formulas

```
TC(a, N)  = Copt(a) + N × Crun(a)          TC(none, N) = N × Crun_static
N*(a)     = max(1, ceil(Copt(a) / (Crun_static − Crun(a))))   if Crun_static > Crun(a), else ∞
```

Every per-run quantity is the **median** over the measured repetitions of its cell (5 per cell).
For A2 the cost model uses the difference of stage medians; the research-log benefit figures use
the median of per-block paired differences. The two agree in sign and order of magnitude
(e.g. large_high 34.2 % vs 32.3 %).

## 5. Assumptions and limitations

1. Workload characteristics stay stable over the N future runs, so a measured saving recurs.
2. N is known or estimable from the schedule; it is a model parameter, not physically executed.
   Reported for N ∈ {1, 5, 10, 50}.
3. Compacted data is not re-fragmented by new writes during the horizon.
4. The cost of evaluating the decision rule is negligible.
5. Only compute is priced: no storage, data transfer, licence or idle-cluster cost.
6. Absolute dollar values reflect laptop-scale data (≤ 28 MB) and are tiny (fractions of a
   US cent per run); the **relative** savings and `N*` are the results of interest.
7. Because `Copt` and `Crun` use the same proxy, `N*` is independent of `p` (and of `v` under P1).

## 6. Sensitivity analysis

* Price × {0.5, 1.0, 1.5}: `N*` and every decision are unchanged (verified in
  `cost_sensitivity.csv`); absolute savings scale linearly.
* Both proxies reported for every workload and action.
* A1 one-time cost: search (primary) vs rule (lower bound).
* Noise check: `saving_exceeds_static_iqr` flags savings smaller than the interquartile range
  of the static configuration's cost. This is indicative only; the decision framework's
  noise-floor threshold is defined in Phase 9.
