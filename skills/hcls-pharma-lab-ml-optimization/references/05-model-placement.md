# Phase 5 Reference — Model Placement

A predictive model only adds value if its output lands *before* the bottleneck and changes a decision. Predictions made after the bottleneck have no leverage.

## Placement Rule

```
[upstream features available] → [PREDICTION] → [DECISION] → [bottleneck step] → [outcome]
```

If features for the prediction aren't available before the decision point, the placement is invalid.

## Model Contract Template

For every bottleneck from Phase 3:

```
Bottleneck step:        <name>
Target variable:        <what you predict>
Prediction unit:        <sample / batch / instrument-hour / shift>
Decision driven:        <route / prioritize / pre-empt / schedule>
Prediction window:      <how far in advance must the prediction land>
Feature scope:          <Phase 4 features available at prediction time>
Input schema:           <columns + types>
Output schema:          <columns + types>
Refresh cadence:        <streaming / hourly / daily>
Owner:                  <role>
Decision threshold:     <how the prediction is consumed operationally>
Success metric:         <how we measure that placement worked>
```

## Canonical Placements

### 1. Rerun Prediction (placed before analyzer run)

- **Target:** binary — will this sample need a rerun?
- **Decision:** if predicted-rerun probability is high, route to a more robust method, request fresh sample, or pre-allocate redundant capacity.
- **Features:** sample type, priority, draw conditions, prior delta-checks, reagent lot history, instrument calibration age, queue depth.
- **Window:** seconds (before the analyzer run is queued).

### 2. TAT Prediction (placed before queue assignment)

- **Target:** regression — predicted minutes until result release.
- **Decision:** prioritize / route stat samples that would otherwise miss SLA.
- **Features:** current backlog, shift staffing, sample type mix, instrument status, time-of-day.
- **Window:** seconds.

### 3. QC Failure Prediction (placed before next maintenance window)

- **Target:** binary — will this instrument fail QC in the next H hours?
- **Decision:** schedule pre-emptive calibration, swap reagent lot, tag instrument for service.
- **Features:** QC drift trend, calibration age, reagent-lot age, last PM date, error-code history, environmental sensors.
- **Window:** hours to days.

### 4. Predictive Maintenance (placed before failure)

- **Target:** time-to-failure or failure-in-window.
- **Decision:** schedule maintenance during low-utilization window.
- **Features:** error codes, run hours since last PM, vibration/temp telemetry, age, parts-replacement history.
- **Window:** days.

### 5. Autoverification Confidence (placed at result generation)

- **Target:** classification — autoverify-eligible? probability of agreement with tech reviewer.
- **Decision:** auto-release vs. queue for tech review.
- **Features:** flags, delta-check hits, prior result history, instrument QC status, sample type.
- **Window:** seconds.

### 6. Tech Review Time Prediction (placed at result entry to review queue)

- **Target:** regression — minutes to complete review.
- **Decision:** load-balance review queue across reviewers / shifts.
- **Features:** flag count, sample complexity, reviewer skill, shift, queue depth.
- **Window:** seconds.

## Diagram Convention

When sketching the workflow with model insertion points, mark:

```
[STEP A] → [feature snapshot] → ⟦MODEL: target=X⟧ → [decision: route/prioritize] → [STEP B (bottleneck)] → [STEP C]
```

The `⟦MODEL⟧` block always appears *before* the bottleneck and *after* a feature snapshot.

## Anti-Patterns

- **Predicting at the bottleneck instead of before it.** No leverage.
- **Using leaky features.** A feature that exists only after the bottleneck cannot be a model input.
- **No decision attached.** "Predict TAT" with no operational consumer is theater.
- **Wrong prediction unit.** Per-sample prediction can't drive a per-batch decision (and vice versa).
- **One model for many bottlenecks.** Each bottleneck deserves its own contract; multi-output models are usually a misalignment.
