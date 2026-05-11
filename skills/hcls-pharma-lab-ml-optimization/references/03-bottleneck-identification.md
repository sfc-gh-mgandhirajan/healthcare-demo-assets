# Phase 3 Reference — Bottleneck Identification

Rank steps by where time actually goes, so ML effort lands on the steps with leverage.

## What to Measure per Step

For each step from Phase 1, capture:

| Measure | Why it matters |
|---------|----------------|
| Median cycle time | Typical case |
| P95 cycle time | Long tail — usually the operational pain |
| Wait time before step starts | Often dominates total elapsed time |
| Variance / stdev | High variance → high ML opportunity |
| Throughput cap | Capacity ceiling of this step |
| Failure / rerun rate | Steps with high rerun loops are bottlenecks even if their cycle time is short |

## Ranking Heuristic

```
bottleneck_score = wait_time_p95 + step_cycle_time_p95
                   * (1 + rerun_rate)
```

Sort descending. Top 1-3 are the bottlenecks worth modeling.

## Diagnostic Patterns

| Pattern | What it usually means | Where to look |
|---------|-----------------------|---------------|
| High wait time, low cycle time | Capacity / queueing problem | Scheduling, staffing, instrument count |
| Low wait, high cycle time | Slow process | Method, automation, manual steps |
| High variance | Non-deterministic step | Sample type mix, manual judgment, reagent lot effects |
| Bimodal distribution | Hidden sub-population | Often two routing branches collapsed into one step |
| Diurnal / shift cycles | Staffing or cutoff effects | Shift schedule, batch cutoffs |
| Long tail on rerun | QC failure cascade | Method robustness, calibration |

## SQL Sketch (assumes LIMS event log)

```sql
WITH step_events AS (
  SELECT
    sample_id,
    step_name,
    step_start_ts,
    step_end_ts,
    DATEDIFF('second', step_start_ts, step_end_ts) AS cycle_seconds,
    DATEDIFF('second', LAG(step_end_ts) OVER (PARTITION BY sample_id ORDER BY step_start_ts), step_start_ts) AS wait_seconds
  FROM lims_step_events
  WHERE step_start_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())
)
SELECT
  step_name,
  COUNT(*)                                              AS n,
  APPROX_PERCENTILE(cycle_seconds, 0.5)                 AS cycle_p50,
  APPROX_PERCENTILE(cycle_seconds, 0.95)                AS cycle_p95,
  APPROX_PERCENTILE(wait_seconds,  0.5)                 AS wait_p50,
  APPROX_PERCENTILE(wait_seconds,  0.95)                AS wait_p95,
  STDDEV(cycle_seconds)                                 AS cycle_stdev,
  AVG(IFF(rerun_flag, 1.0, 0.0))                        AS rerun_rate
FROM step_events
GROUP BY step_name
ORDER BY (wait_p95 + cycle_p95) * (1 + rerun_rate) DESC;
```

## Capture Template

```
Bottleneck rank: <1, 2, 3>
Step:                 <name>
Median cycle time:    <X>
P95 cycle time:       <X>
Median wait time:     <X>
P95 wait time:        <X>
Variance / stdev:     <X>
Rerun rate:           <%>
Suspected cause:      <queueing / slow process / variance / rerun cascade>
```

## Anti-Patterns

- **Using mean only.** Tail behavior is what hurts SLAs.
- **Ignoring wait time.** A 30-second analyzer step with a 4-hour wait queue is a queueing bottleneck, not an analyzer problem.
- **Picking bottlenecks by feel.** Always back the choice with cycle/wait stats.
- **Modeling the longest step instead of the slowest-to-recover step.** Sometimes a frequently-failing 1-minute step beats a steady 10-minute one.
