# Phase 7 Reference — Explainability and Key Drivers

A prediction without an explanation does not change behavior. Lab leads need to know *why* — at the population level (where to invest), the cohort level (which slices are at risk), and the per-prediction level (what to do with this sample / batch / instrument right now).

## Three Layers of Explanation

| Layer | Audience | Purpose | Method |
|-------|----------|---------|--------|
| Per-prediction | bench tech, supervisor | "Why is this sample flagged?" | SHAP, feature contributions |
| Cohort | lab manager | "Which slices are at risk?" | grouped feature importance |
| Global | lab director, ops | "Where to invest?" | global feature importance, partial dependence |

## SHAP on Snowpark ML

```python
import shap, joblib
from snowflake.ml.registry import Registry

registry = Registry(session=session, database_name="LAB_ML", schema_name="MODELS")
model = registry.get_model("RERUN_PREDICTOR").default

booster = model._packager.model.booster_   # XGBoost / LightGBM example
explainer = shap.TreeExplainer(booster)

shap_values = explainer.shap_values(features_pdf)
```

Persist per-prediction explanations to `LAB_PREDICTION_EXPLANATIONS`:

```sql
CREATE TABLE IF NOT EXISTS PREDICTIONS.LAB_PREDICTION_EXPLANATIONS (
  prediction_id              STRING,
  feature_name               STRING,
  feature_value              VARIANT,
  contribution               FLOAT,    -- SHAP value (signed)
  rank                       INTEGER,  -- 1 = top driver
  explanation_method         STRING,
  generated_ts               TIMESTAMP_NTZ
);
```

Keep top-K (typically 5) features per prediction to control storage.

## Cohort Analysis

For each model, compute mean |SHAP| per feature, grouped by operationally meaningful slices:

- instrument_id
- shift
- sample_type
- ordering_location
- reagent_lot

Surface as a heatmap: rows = cohort, columns = features, cells = mean contribution. Cohort outliers point at process issues that ML alone cannot fix (a single instrument or shift over-driving rerun risk usually means recalibration, training, or hardware service — not a model retrain).

## Global Feature Importance

Refresh on each retrain. Publish to `PREDICTIONS.LAB_GLOBAL_FEATURE_IMPORTANCE` with model name, version, feature, importance, and rank. Use this to prioritize source-system investment in Phase 4.

## Narrative Explanation via AI_COMPLETE

Convert top-K SHAP rows into a one-sentence operational explanation using `cortex-ai-functions`:

```sql
SELECT
  prediction_id,
  AI_COMPLETE(
    'mistral-large2',
    PROMPT(
      $$You are a lab operations assistant. Given the top drivers for a rerun-risk prediction, write ONE sentence (max 25 words) for a bench tech explaining why this sample is flagged and the recommended action. Drivers (feature: value, contribution): {0}$$,
      ARRAY_AGG(feature_name || ': ' || TO_VARCHAR(feature_value) || ' (shap=' || TO_VARCHAR(contribution) || ')') WITHIN GROUP (ORDER BY rank)
    )
  ) AS narrative
FROM PREDICTIONS.LAB_PREDICTION_EXPLANATIONS
WHERE prediction_id = ?
GROUP BY prediction_id;
```

## Action Loop

Every explanation must map to a concrete operational action. Maintain a lookup of `(top_driver, recommended_action)` pairs:

| Top Driver | Recommended Action |
|------------|--------------------|
| reagent_lot_age_days high | Switch to fresh lot; verify QC |
| calibrator_age_hours high | Run calibration before next batch |
| queue_depth_at_analyzer high | Re-route to alternate analyzer |
| prior_delta_check_hits | Repeat sample; verify draw integrity |
| shift_role_count low | Surface for staffing review |

Persist as `PREDICTIONS.LAB_ACTION_RECOMMENDATIONS` and join to `LAB_PREDICTIONS` so the consumer (Streamlit dashboard, LIMS callback) sees prediction + driver + action together.

## Streamlit Surface (delegate to `developing-with-streamlit`)

Minimal panels:

1. **Today's predictions** — filterable by sample type, instrument, shift.
2. **Top global drivers** — bar chart of feature importance.
3. **Cohort heatmap** — to spot a misbehaving instrument / shift.
4. **Per-prediction drill-down** — top-5 SHAP, narrative, recommended action.
5. **Outcome reconciliation** — actual vs. predicted, model quality trend.

## Anti-Patterns

- **Showing a number with no driver.** Operationally useless.
- **SHAP values without sign / direction.** Lab staff need to know which features pushed the prediction up vs. down.
- **Skipping cohort analysis.** Global importance hides the instrument-specific or shift-specific patterns that are most actionable.
- **Narrative without action.** A nice sentence that doesn't tell the tech what to do is filler.
- **Static recommendations.** When drivers shift, the action lookup must shift with them — review on each retrain.
