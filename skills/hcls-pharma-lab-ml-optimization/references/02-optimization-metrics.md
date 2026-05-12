# Phase 2 Reference — Optimization Metrics

Decide what "optimized" means before deciding how to optimize it. Optimizing the wrong metric is the most common ML-for-ops failure mode.

## Metric Tree Pattern

```
PRIMARY KPI                                   <- one number leadership cares about
  ├─ supporting metric A                      <- must move with the KPI
  ├─ supporting metric B                      <- must move with the KPI
  └─ supporting metric C                      <- must not regress
GUARDRAIL CONSTRAINTS                         <- hard limits, never violated
  ├─ regulatory / accreditation
  ├─ quality (false-result rate, reanalysis rate)
  └─ safety / compliance
```

## Canonical Lab KPIs

| KPI | Definition | Typical Unit | Common Source |
|-----|------------|--------------|---------------|
| Turnaround Time (TAT) | order-to-result elapsed time | minutes / hours | LIMS timestamps |
| Therapeutic TAT | collection-to-result time on stat orders | minutes | LIMS + HL7 |
| Throughput | samples processed per unit time | samples / hour | LIMS / instrument |
| First-Pass Yield | % samples passing without rerun | % | LIMS rerun flags |
| On-Time Delivery | % results released within SLA | % | LIMS + SLA table |
| Cost per Sample | total cost / samples processed | USD / sample | ERP + LIMS |
| Instrument Utilization | run hours / available hours | % | Instrument logs |
| Auto-Verification Rate | % results auto-released with no tech review | % | LIMS |
| QC Pass Rate | % QC events passing | % | Instrument / LIMS |
| Tech Idle Time | % shift time with no active task | % | Staffing / LIMS |
| Reagent Waste | reagent expired / total used | % | Inventory |

## Choosing the Primary KPI

Use the lab's stated business pain to pick exactly one:

| If the user says... | Primary KPI |
|---------------------|-------------|
| "Results take too long" | TAT (median or P95) |
| "We're falling behind" | Throughput or Backlog |
| "We're missing SLAs" | On-Time Delivery |
| "Costs are too high" | Cost per Sample |
| "Instruments sit idle / break down" | Utilization or MTBF |
| "Too many manual reviews" | Auto-Verification Rate |
| "Too many reruns" | First-Pass Yield |

## Guardrail Constraints (never optimize past these)

- **Regulatory TAT** — e.g., CAP requires routine chemistry results within stated timeframes.
- **Reanalysis rate cap** — reducing TAT by skipping QC will increase reanalysis. Set a max.
- **False result rate** — e.g., autoverification expansion must not increase critical-value misses.
- **Accreditation requirements** — CAP, CLIA, ISO 15189, GxP for manufacturing.
- **Headcount / budget** — practical operational ceiling.

## Anti-Patterns

- **Optimizing average instead of P95.** Patients and customers feel the tail, not the mean.
- **Optimizing TAT by deferring QC.** TAT drops, reanalysis rate explodes — you've made things worse.
- **Mixing scoped and unscoped metrics.** "TAT" without specifying *which* test type and *which* priority class is meaningless.
- **Vanity metrics.** "Volume" without "first-pass yield" lets quality regress invisibly.
- **No numeric target.** "Faster" is not a metric. "P95 routine chemistry TAT < 60 min" is.

## Capture Template

```
Primary KPI: <name>
  current value: <number> <unit>
  target value:  <number> <unit>
  measurement:   <how it is measured today; data source; cadence>
  trustworthy?:  <yes / known issues>

Supporting metrics:
  - <name>   current=<X>  must move to=<Y>
  - <name>   current=<X>  must not regress past=<Y>

Guardrails:
  - <constraint>   threshold=<value>   source=<authority>
```
