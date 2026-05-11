# Phase 6 Reference — Model Execution on Snowflake

Phase 5 produced model contracts. Phase 6 stands the models up. Delegate platform mechanics to `machine-learning`, `dynamic-tables`, `cortex-ai-functions`, and `data-quality`.

## Reference Architecture

```
[ Source systems: LIMS, instrument middleware, ERP, sensors ]
              │
              ▼  (Snowpipe / connector / Streams)
       RAW.* tables
              │
              ▼  (Dynamic Table feature pipeline)
       FEATURES.LAB_FEATURES   <- one row per prediction unit
              │
              ▼  (Snowpark ML / external)  registered in
       Snowflake Model Registry
              │
              ▼  (UDF / SQL inference / Dynamic Table)
       PREDICTIONS.LAB_PREDICTIONS
              │
              ▼
       PREDICTIONS.LAB_PREDICTION_EXPLANATIONS
              │
              ▼
       Streamlit dashboard / LIMS callback
```

## Feature Pipeline

Use a **Dynamic Table** keyed by the prediction unit. One row per sample, batch, or instrument-hour.

```sql
CREATE OR REPLACE DYNAMIC TABLE FEATURES.LAB_FEATURES
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_ML_WH
AS
SELECT
  s.sample_id                                AS prediction_unit_id,
  'SAMPLE'                                   AS prediction_unit_type,
  s.sample_type,
  s.priority,
  s.draw_to_accession_seconds,
  q.queue_depth_at_analyzer,
  i.calibrator_age_hours,
  i.reagent_lot_age_days,
  i.last_qc_drift,
  st.shift_role_count,
  CURRENT_TIMESTAMP()                        AS feature_snapshot_ts
FROM RAW.LIMS_SAMPLES s
LEFT JOIN RAW.LIMS_QUEUES q
  ON q.analyzer_id = s.assigned_analyzer_id AND q.snapshot_ts = s.queued_ts
LEFT JOIN RAW.INSTRUMENT_STATE i
  ON i.instrument_id = s.assigned_analyzer_id AND i.snapshot_ts = s.queued_ts
LEFT JOIN RAW.STAFFING st
  ON st.shift_id = s.shift_id;
```

## Training (delegate to `machine-learning` skill)

The pattern:

1. Snapshot historical features into a training table.
2. Join to outcome labels (rerun flag, actual TAT, actual QC pass/fail).
3. Train with Snowpark ML (or external) — XGBoost / LightGBM / logistic regression are usually enough; do not reach for deep learning unless tabular models clearly underperform.
4. Register in **Snowflake Model Registry** with name `LAB_ML.<bottleneck>_<target>`, version-tagged.

## Inference

### Option A — UDF on Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE PREDICTIONS.LAB_PREDICTIONS
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_ML_WH
AS
SELECT
  UUID_STRING()                                                    AS prediction_id,
  prediction_unit_type,
  prediction_unit_id,
  'LAB_ML.RERUN_PREDICTOR'                                         AS model_name,
  '2026-04-01-v3'                                                  AS model_version,
  'rerun_probability'                                              AS target_variable,
  TO_VARIANT(MODEL_REGISTRY.RERUN_PREDICTOR_V3!PREDICT(OBJECT_CONSTRUCT(*))) AS predicted_value,
  CURRENT_TIMESTAMP()                                              AS prediction_ts,
  DATEADD('minute', 30, CURRENT_TIMESTAMP())                       AS decision_window_end_ts,
  feature_snapshot_id,
  data_quality_status
FROM FEATURES.LAB_FEATURES
WHERE data_quality_status = 'OK';
```

### Option B — Streamlit / LIMS callback

Service-level prediction by calling the model from a Streamlit app or LIMS webhook into a Snowflake Service Function.

## Output Table

```sql
CREATE TABLE IF NOT EXISTS PREDICTIONS.LAB_PREDICTIONS (
  prediction_id            STRING,
  prediction_unit_type     STRING,
  prediction_unit_id       STRING,
  model_name               STRING,
  model_version            STRING,
  target_variable          STRING,
  predicted_value          VARIANT,
  prediction_ts            TIMESTAMP_NTZ,
  decision_window_end_ts   TIMESTAMP_NTZ,
  feature_snapshot_id      STRING,
  data_quality_status      STRING
);
```

## Data Quality Gate (delegate to `data-quality`)

Before scoring, validate:

- All required features non-null
- Numeric features within expected ranges
- Feature freshness within model's tolerance

Set `data_quality_status` to `OK`, `STALE`, `OUT_OF_RANGE`, or `MISSING_FEATURE`. Predictions on non-`OK` rows should be flagged, not silently consumed by downstream decisions.

## Monitoring

Track three layers:

| Layer | Metric | Source |
|-------|--------|--------|
| Model quality | calibration, AUC, MAE; prediction-vs-actual drift | join `LAB_PREDICTIONS` to outcomes table |
| Feature drift | PSI / KS-test on each feature vs. training distribution | snapshot features over time |
| Operational impact | did the Phase 2 KPI actually move? | metric tree |

Schedule monitoring as a **Snowflake Task** that writes to `PREDICTIONS.LAB_MODEL_MONITORING`.

## Anti-Patterns

- **Training without a holdout aligned to deployment time.** Random splits leak future info.
- **Skipping the data-quality gate.** Silent scoring on stale features causes silent harm.
- **No outcome join.** If you can't join predictions back to outcomes, you can't measure model quality — and you have a feedback-loop problem.
- **Ignoring concept drift.** Reagent lots, instrument firmware, staffing rotations all break stationarity. Plan retraining cadence.
