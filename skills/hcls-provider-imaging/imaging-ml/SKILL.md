---
name: imaging-ml
description: "Machine learning for medical imaging on Snowflake. Feature engineering from DICOM metadata, embedding generation, model training and registry, vector similarity search, model monitoring, and SPCS deployment for imaging AI. Use when: imaging model, train imaging, imaging classification ML, pathology model, radiology AI, deploy imaging model, imaging inference, image embedding, similar study search."
parent_skill: hcls-provider-imaging
---

# Medical Imaging ML Models

## When to Load

Parent router routes here on **ML** intent: imaging model, train imaging, radiology AI, deploy imaging model, image embedding, similar study search.

## Prerequisites

- DICOM metadata in `{database}.{schema}` (core tables populated)
- `snowflake-ml-python` for model training/registry
- Cortex AI functions (`SNOWFLAKE.CORTEX.EMBED_TEXT_1024`, `SNOWFLAKE.CORTEX.COMPLETE`)
- For GPU: SPCS with GPU compute pools

## Workflow

### Step 1: Feature Engineering

**Ask** user which ML task: (1) Classification, (2) Anomaly detection, (3) Regression, (4) Embedding generation, (5) Vector similarity search, (6) Full pipeline.

**MANDATORY STOPPING POINT:** Confirm ML task before creating feature tables.

**Study-level features with complexity scoring:**
```sql
CREATE OR REPLACE VIEW {database}.{schema}.V_IMAGING_ML_FEATURES AS
SELECT s.STUDY_KEY, s.STUDY_INSTANCE_UID, ser.MODALITY, ser.BODY_PART_EXAMINED,
    e.INSTITUTION_NAME,
    s.NUMBER_OF_SERIES, s.NUMBER_OF_INSTANCES,
    s.STUDY_DATE AS study_date_parsed,
    DATEDIFF('year', p.PATIENT_BIRTH_DATE, s.STUDY_DATE) AS patient_age_at_study,
    p.PATIENT_SEX,
    ROUND(s.NUMBER_OF_SERIES * LOG(10, GREATEST(s.NUMBER_OF_INSTANCES, 1)) +
          CASE WHEN ser.MODALITY IN ('CT','MR','PT') THEN 2 ELSE 0 END, 2) AS study_complexity_score,
    CASE WHEN s.NUMBER_OF_SERIES > 5 AND s.NUMBER_OF_INSTANCES > 100 THEN 1 ELSE 0 END AS is_complex_study,
    DAYOFWEEK(s.STUDY_DATE) AS study_day_of_week,
    HOUR(s.STUDY_TIME) AS study_hour,
    CASE WHEN HOUR(s.STUDY_TIME) NOT BETWEEN 7 AND 21 THEN 1 ELSE 0 END AS is_off_hours
FROM {database}.{schema}.DICOM_STUDY s
JOIN {database}.{schema}.DICOM_PATIENT p ON s.PATIENT_KEY = p.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY
WHERE ser.MODALITY IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.STUDY_KEY ORDER BY ser.SERIES_KEY) = 1;
```

**Multi-modality patient features:**
```sql
CREATE OR REPLACE VIEW {database}.{schema}.V_PATIENT_MODALITY_FEATURES AS
SELECT p.PATIENT_KEY, p.PATIENT_ID, COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS total_studies,
    COUNT(DISTINCT ser.MODALITY) AS distinct_modalities,
    LISTAGG(DISTINCT ser.MODALITY, ',') WITHIN GROUP (ORDER BY ser.MODALITY) AS modality_profile,
    DATEDIFF('day', MIN(s.STUDY_DATE), MAX(s.STUDY_DATE)) AS imaging_span_days
FROM {database}.{schema}.DICOM_PATIENT p
JOIN {database}.{schema}.DICOM_STUDY s ON p.PATIENT_KEY = s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
GROUP BY p.PATIENT_KEY, p.PATIENT_ID;
```

### Step 2: Embedding Generation

**Goal:** Text embeddings of radiology reports for similarity search.

```sql
INSERT INTO {database}.{schema}.EMBEDDING_MODEL
    (MODEL_NAME, MODEL_VERSION, MODALITY_SCOPE, TASK_SCOPE, DIMENSIONALITY, TRAINING_DATA_SUMMARY)
VALUES ('snowflake-arctic-embed-l-v2.0', 'v2.0', ARRAY_CONSTRUCT('ALL'),
    ARRAY_CONSTRUCT('report_similarity','clinical_search'), 1024, 'Snowflake Arctic Embed Large');

INSERT INTO {database}.{schema}.IMAGE_EMBEDDING
    (INSTANCE_KEY, EMBEDDING_VECTOR, MODEL_KEY, REPRESENTATION_SCOPE, REPRESENTATION_VERSION)
SELECT i.INSTANCE_KEY,
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', r.REPORT_TEXT)::ARRAY,
    (SELECT MODEL_KEY FROM EMBEDDING_MODEL WHERE MODEL_NAME = 'snowflake-arctic-embed-l-v2.0' LIMIT 1),
    'radiology_report', 'v1'
FROM {database}.{schema}.RADIOLOGY_REPORTS r
JOIN {database}.{schema}.DICOM_STUDY s ON r.STUDY_INSTANCE_UID = s.STUDY_INSTANCE_UID
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
JOIN {database}.{schema}.DICOM_INSTANCE i ON ser.SERIES_KEY = i.SERIES_KEY
WHERE r.REPORT_TEXT IS NOT NULL AND LENGTH(r.REPORT_TEXT) > 10
  AND NOT EXISTS (SELECT 1 FROM IMAGE_EMBEDDING emb WHERE emb.INSTANCE_KEY = i.INSTANCE_KEY AND emb.REPRESENTATION_SCOPE = 'radiology_report');
```

**MANDATORY STOPPING POINT:** Verify embedding count and coverage before proceeding.

### Step 3: Model Training

**Train/test split:**
```python
from snowflake.snowpark import Session
import os
session = Session.builder.config("connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default").create()
feature_df = session.table("{database}.{schema}.V_IMAGING_ML_FEATURES")
train_df, test_df = feature_df.random_split([0.8, 0.2], seed=42)
train_df.write.save_as_table("{database}.{schema}.ML_TRAIN", mode="overwrite")
test_df.write.save_as_table("{database}.{schema}.ML_TEST", mode="overwrite")
```

**Classification:**
```python
from snowflake.ml.modeling.ensemble import RandomForestClassifier
clf = RandomForestClassifier(
    input_cols=["NUMBER_OF_SERIES","NUMBER_OF_INSTANCES","STUDY_COMPLEXITY_SCORE","PATIENT_AGE_AT_STUDY","IS_OFF_HOURS","STUDY_DAY_OF_WEEK"],
    label_cols=["IS_COMPLEX_STUDY"], output_cols=["PREDICTED_COMPLEX"])
clf.fit(train_df)
predictions = clf.predict(test_df)
accuracy = predictions.filter(predictions["IS_COMPLEX_STUDY"] == predictions["PREDICTED_COMPLEX"]).count() / predictions.count()
```

**Anomaly detection (dose outliers):**
```python
from snowflake.ml.modeling.ensemble import IsolationForest
dose_df = session.sql("SELECT CTDI_VOL, DOSE_LENGTH_PRODUCT, EXPOSURE_TIME, KVP, XRAY_TUBE_CURRENT FROM {database}.{schema}.DICOM_DOSE_SUMMARY WHERE CTDI_VOL IS NOT NULL")
iso = IsolationForest(input_cols=["CTDI_VOL","DOSE_LENGTH_PRODUCT","EXPOSURE_TIME","KVP","XRAY_TUBE_CURRENT"], output_cols=["ANOMALY_SCORE"], contamination=0.05)
iso.fit(dose_df)
```

**Regression (report turnaround time):**
```python
from snowflake.ml.modeling.ensemble import GradientBoostingRegressor
tat_df = session.sql("""SELECT NUMBER_OF_SERIES, NUMBER_OF_INSTANCES, STUDY_COMPLEXITY_SCORE, IS_OFF_HOURS, STUDY_DAY_OF_WEEK, PATIENT_AGE_AT_STUDY,
    DATEDIFF('hour', s.STUDY_DATE, r.REPORT_DATE) AS TAT_HOURS
FROM V_IMAGING_ML_FEATURES f JOIN DICOM_STUDY s ON f.STUDY_KEY=s.STUDY_KEY JOIN RADIOLOGY_REPORTS r ON s.STUDY_INSTANCE_UID=r.STUDY_INSTANCE_UID WHERE r.REPORT_DATE IS NOT NULL""")
reg = GradientBoostingRegressor(input_cols=["NUMBER_OF_SERIES","NUMBER_OF_INSTANCES","STUDY_COMPLEXITY_SCORE","IS_OFF_HOURS","STUDY_DAY_OF_WEEK","PATIENT_AGE_AT_STUDY"],
    label_cols=["TAT_HOURS"], output_cols=["PREDICTED_TAT_HOURS"])
reg.fit(tat_df)
```

**MANDATORY STOPPING POINT:** Review model metrics before registry.

### Step 4: Model Registry

```python
from snowflake.ml.registry import Registry
registry = Registry(session=session, database_name="{database}", schema_name="{schema}")
mv = registry.log_model(model=clf, model_name="imaging_complexity_classifier", version_name="v1",
    sample_input_data=train_df.select("NUMBER_OF_SERIES","NUMBER_OF_INSTANCES","STUDY_COMPLEXITY_SCORE","PATIENT_AGE_AT_STUDY","IS_OFF_HOURS","STUDY_DAY_OF_WEEK").limit(10),
    metrics={"accuracy": float(accuracy)}, comment="Predicts study complexity from DICOM metadata")
```

### Step 5: Vector Similarity Search

```sql
WITH qemb AS (SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0',
    'chest CT bilateral pulmonary nodules suspicious metastatic') AS qvec)
SELECT r.STUDY_INSTANCE_UID, r.REPORT_TEXT, ser.MODALITY, ser.BODY_PART_EXAMINED,
    VECTOR_COSINE_SIMILARITY(emb.EMBEDDING_VECTOR::VECTOR(FLOAT,1024), qe.qvec::VECTOR(FLOAT,1024)) AS similarity
FROM {database}.{schema}.IMAGE_EMBEDDING emb
JOIN qemb qe ON 1=1
JOIN DICOM_INSTANCE i ON emb.INSTANCE_KEY = i.INSTANCE_KEY
JOIN DICOM_SERIES ser ON i.SERIES_KEY = ser.SERIES_KEY
JOIN DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
JOIN RADIOLOGY_REPORTS r ON s.STUDY_INSTANCE_UID = r.STUDY_INSTANCE_UID
WHERE emb.REPRESENTATION_SCOPE = 'radiology_report'
ORDER BY similarity DESC LIMIT 10;
```

### Step 6: SQL Inference

```sql
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_ML_PREDICTIONS
    TARGET_LAG = '1 hour' WAREHOUSE = {warehouse}
AS SELECT f.STUDY_KEY, f.STUDY_INSTANCE_UID, f.MODALITY, f.BODY_PART_EXAMINED, f.STUDY_COMPLEXITY_SCORE,
    {database}.{schema}.imaging_complexity_classifier!PREDICT(
        f.NUMBER_OF_SERIES, f.NUMBER_OF_INSTANCES, f.STUDY_COMPLEXITY_SCORE,
        f.PATIENT_AGE_AT_STUDY, f.IS_OFF_HOURS, f.STUDY_DAY_OF_WEEK
    ):PREDICTED_COMPLEX::INTEGER AS predicted_complex, CURRENT_TIMESTAMP() AS predicted_at
FROM {database}.{schema}.V_IMAGING_ML_FEATURES f;
```

**MANDATORY STOPPING POINT:** Verify inference results on known data before deploying continuous predictions.

### Step 7: Model Monitoring

```sql
CREATE OR REPLACE TABLE {database}.{schema}.ML_MODEL_MONITORING (
    monitoring_key INTEGER AUTOINCREMENT PRIMARY KEY, model_name VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL, monitoring_date DATE DEFAULT CURRENT_DATE(),
    total_predictions INTEGER, prediction_distribution VARIANT, feature_stats VARIANT,
    drift_detected BOOLEAN DEFAULT FALSE, _created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP());

CREATE OR REPLACE TASK {database}.{schema}.TASK_ML_MONITORING
    WAREHOUSE = {warehouse} SCHEDULE = 'USING CRON 0 6 * * * America/Los_Angeles'
AS INSERT INTO ML_MODEL_MONITORING (model_name, model_version, total_predictions, prediction_distribution, drift_detected)
SELECT 'imaging_complexity_classifier', 'v1', COUNT(*),
    OBJECT_CONSTRUCT('predicted_complex_pct', ROUND(100.0*SUM(CASE WHEN predicted_complex=1 THEN 1 ELSE 0 END)/COUNT(*),2)),
    CASE WHEN ABS((SUM(CASE WHEN predicted_complex=1 THEN 1 ELSE 0 END)::FLOAT/COUNT(*)) -
        (SELECT AVG(prediction_distribution:predicted_complex_pct::FLOAT/100) FROM ML_MODEL_MONITORING
         WHERE model_name='imaging_complexity_classifier' AND monitoring_date >= DATEADD('day',-30,CURRENT_DATE()))) > 0.10
    THEN TRUE ELSE FALSE END
FROM DT_ML_PREDICTIONS WHERE predicted_at >= DATEADD('day',-1,CURRENT_TIMESTAMP());

ALTER TASK {database}.{schema}.TASK_ML_MONITORING RESUME;
```

### Step 8: SPCS Deployment

**Service spec for GPU-based imaging model:**
```yaml
spec:
  containers:
    - name: imaging-model-server
      image: /{database}/{schema}/IMAGE_REPO/imaging-model:latest
      resources:
        requests: { memory: 8Gi, cpu: 4, "nvidia.com/gpu": 1 }
      env: { MODEL_PATH: /app/model/ }
  endpoints:
    - name: inference
      port: 8080
      public: false
```

```sql
CREATE COMPUTE POOL IF NOT EXISTS IMAGING_GPU_POOL MIN_NODES=1 MAX_NODES=2 INSTANCE_FAMILY=GPU_NV_S;
CREATE SERVICE {database}.{schema}.IMAGING_MODEL_SVC
    IN COMPUTE POOL IMAGING_GPU_POOL FROM @SERVICE_SPECS SPECIFICATION_FILE='imaging_model_spec.yaml';
```

**MANDATORY STOPPING POINT:** GPU compute pools incur cost. Confirm before creation.

## Stopping Points

- After Step 1: Confirm ML task and features
- After Step 2: Verify embedding coverage
- After Step 3: Review model metrics before registry
- After Step 6: Validate inference on known data
- After Step 8: GPU cost approval

## Output

| Object | Type | Purpose |
|--------|------|---------|
| `V_IMAGING_ML_FEATURES` | View | Study-level ML features |
| `V_PATIENT_MODALITY_FEATURES` | View | Patient multi-modality features |
| `IMAGE_EMBEDDING` rows | Table data | Report embeddings |
| Registered model | ML Registry | Versioned model with metrics |
| `DT_ML_PREDICTIONS` | Dynamic Table | Continuous inference |
| `ML_MODEL_MONITORING` + Task | Table + Task | Drift detection |
| `IMAGING_MODEL_SVC` | SPCS Service | GPU model serving (optional) |
