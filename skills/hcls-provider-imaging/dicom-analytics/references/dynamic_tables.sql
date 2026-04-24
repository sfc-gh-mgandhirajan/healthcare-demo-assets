-- =============================================================================
-- DICOM Analytics Dynamic Tables
-- All functionally tested against POLARIS1 (2026-04-13)
-- Database: {database}.{schema}
-- =============================================================================

-- DT_IMAGING_COHORTS: Cross-modality patient cohort profiles
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_IMAGING_COHORTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = {warehouse}
AS
SELECT
    p.PATIENT_KEY, p.PATIENT_ID, p.PATIENT_SEX, p.PATIENT_BIRTH_DATE,
    LISTAGG(DISTINCT ser.MODALITY, ',') WITHIN GROUP (ORDER BY ser.MODALITY) AS modality_profile,
    LISTAGG(DISTINCT ser.BODY_PART_EXAMINED, ',') WITHIN GROUP (ORDER BY ser.BODY_PART_EXAMINED) AS body_parts_imaged,
    COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS total_studies,
    COUNT(DISTINCT ser.MODALITY) AS distinct_modalities,
    COUNT(DISTINCT ser.BODY_PART_EXAMINED) AS distinct_body_parts,
    COUNT(DISTINCT CASE WHEN ser.MODALITY='CT' THEN s.STUDY_INSTANCE_UID END) AS ct_count,
    COUNT(DISTINCT CASE WHEN ser.MODALITY='MR' THEN s.STUDY_INSTANCE_UID END) AS mr_count,
    COUNT(DISTINCT CASE WHEN ser.MODALITY='CR' THEN s.STUDY_INSTANCE_UID END) AS cr_count,
    COUNT(DISTINCT CASE WHEN ser.MODALITY='US' THEN s.STUDY_INSTANCE_UID END) AS us_count,
    COUNT(DISTINCT CASE WHEN ser.MODALITY='DX' THEN s.STUDY_INSTANCE_UID END) AS dx_count,
    MIN(s.STUDY_DATE) AS first_study_date,
    MAX(s.STUDY_DATE) AS last_study_date,
    DATEDIFF('day', MIN(s.STUDY_DATE), MAX(s.STUDY_DATE)) AS imaging_span_days
FROM {database}.{schema}.DICOM_PATIENT p
JOIN {database}.{schema}.DICOM_STUDY s ON p.PATIENT_KEY = s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
GROUP BY p.PATIENT_KEY, p.PATIENT_ID, p.PATIENT_SEX, p.PATIENT_BIRTH_DATE;

-- DT_EQUIPMENT_UTILIZATION_DEEP: Scanner throughput by hour/day/modality
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_EQUIPMENT_UTILIZATION_DEEP
    TARGET_LAG = '1 hour'
    WAREHOUSE = {warehouse}
AS
SELECT
    e.EQUIPMENT_KEY, e.STATION_NAME, e.MANUFACTURER, e.MANUFACTURER_MODEL_NAME,
    e.SOFTWARE_VERSIONS, e.DEVICE_SERIAL_NUMBER, e.INSTITUTION_NAME,
    s.STUDY_DATE AS study_day,
    DAYOFWEEK(s.STUDY_DATE) AS day_of_week_num,
    HOUR(s.STUDY_TIME) AS hour_of_day,
    ser.MODALITY,
    COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS studies_performed,
    COUNT(DISTINCT s.PATIENT_KEY) AS unique_patients,
    SUM(s.NUMBER_OF_SERIES) AS total_series,
    SUM(s.NUMBER_OF_INSTANCES) AS total_images,
    AVG(s.NUMBER_OF_INSTANCES) AS avg_images_per_study
FROM {database}.{schema}.DICOM_EQUIPMENT e
JOIN {database}.{schema}.DICOM_SERIES ser ON e.SERIES_KEY = ser.SERIES_KEY
JOIN {database}.{schema}.DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
GROUP BY e.EQUIPMENT_KEY, e.STATION_NAME, e.MANUFACTURER, e.MANUFACTURER_MODEL_NAME,
    e.SOFTWARE_VERSIONS, e.DEVICE_SERIAL_NUMBER, e.INSTITUTION_NAME,
    s.STUDY_DATE, DAYOFWEEK(s.STUDY_DATE),
    HOUR(s.STUDY_TIME), ser.MODALITY;

-- DT_PATIENT_IMAGING_TIMELINE: Longitudinal timelines with follow-up/repeat detection
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_PATIENT_IMAGING_TIMELINE
    TARGET_LAG = '1 hour'
    WAREHOUSE = {warehouse}
AS
SELECT
    p.PATIENT_KEY, p.PATIENT_ID, s.STUDY_KEY, s.STUDY_INSTANCE_UID,
    ser.MODALITY, ser.BODY_PART_EXAMINED, s.STUDY_DESCRIPTION, e.INSTITUTION_NAME, s.REFERRING_PHYSICIAN,
    s.STUDY_DATE AS study_date,
    ROW_NUMBER() OVER (PARTITION BY p.PATIENT_ID ORDER BY s.STUDY_DATE, s.STUDY_TIME) AS study_sequence,
    LAG(s.STUDY_DATE)
        OVER (PARTITION BY p.PATIENT_ID ORDER BY s.STUDY_DATE, s.STUDY_TIME) AS prev_study_date,
    LAG(ser.MODALITY) OVER (PARTITION BY p.PATIENT_ID ORDER BY s.STUDY_DATE, s.STUDY_TIME) AS prev_modality,
    DATEDIFF('day',
        LAG(s.STUDY_DATE) OVER (PARTITION BY p.PATIENT_ID ORDER BY s.STUDY_DATE, s.STUDY_TIME),
        s.STUDY_DATE
    ) AS days_since_prev_study,
    LAG(s.STUDY_DATE)
        OVER (PARTITION BY p.PATIENT_ID, ser.BODY_PART_EXAMINED ORDER BY s.STUDY_DATE, s.STUDY_TIME) AS prev_same_part_date,
    DATEDIFF('day',
        LAG(s.STUDY_DATE) OVER (PARTITION BY p.PATIENT_ID, ser.BODY_PART_EXAMINED ORDER BY s.STUDY_DATE, s.STUDY_TIME),
        s.STUDY_DATE
    ) AS days_since_same_part,
    CASE WHEN DATEDIFF('day',
        LAG(s.STUDY_DATE) OVER (PARTITION BY p.PATIENT_ID, ser.MODALITY, ser.BODY_PART_EXAMINED ORDER BY s.STUDY_DATE),
        s.STUDY_DATE) <= 7 THEN TRUE ELSE FALSE END AS is_repeat_study
FROM {database}.{schema}.DICOM_PATIENT p
JOIN {database}.{schema}.DICOM_STUDY s ON p.PATIENT_KEY = s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY;

-- DT_IMAGING_QUALITY_SCORECARD: Metadata completeness and anomaly flags
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_IMAGING_QUALITY_SCORECARD
    TARGET_LAG = '1 hour'
    WAREHOUSE = {warehouse}
AS
SELECT
    s.STUDY_KEY, s.STUDY_INSTANCE_UID, ser.MODALITY, ser.BODY_PART_EXAMINED, e.INSTITUTION_NAME,
    s.STUDY_DATE AS study_date,
    s.NUMBER_OF_SERIES AS expected_series, COUNT(DISTINCT ser.SERIES_KEY) AS actual_series,
    s.NUMBER_OF_INSTANCES AS expected_instances, COUNT(DISTINCT i.INSTANCE_KEY) AS actual_instances,
    CASE WHEN s.NUMBER_OF_SERIES > 0 THEN ROUND(COUNT(DISTINCT ser.SERIES_KEY)/s.NUMBER_OF_SERIES*100,1) ELSE NULL END AS series_completeness_pct,
    CASE WHEN s.NUMBER_OF_INSTANCES > 0 THEN ROUND(COUNT(DISTINCT i.INSTANCE_KEY)/s.NUMBER_OF_INSTANCES*100,1) ELSE NULL END AS instance_completeness_pct,
    (CASE WHEN s.STUDY_DATE IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN s.STUDY_TIME IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN s.STUDY_DESCRIPTION IS NOT NULL AND s.STUDY_DESCRIPTION != '' THEN 1 ELSE 0 END
     + CASE WHEN s.REFERRING_PHYSICIAN IS NOT NULL AND s.REFERRING_PHYSICIAN != '' THEN 1 ELSE 0 END
     + CASE WHEN s.ACCESSION_NUMBER IS NOT NULL AND s.ACCESSION_NUMBER != '' THEN 1 ELSE 0 END
     + CASE WHEN ser.MODALITY IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN ser.BODY_PART_EXAMINED IS NOT NULL AND ser.BODY_PART_EXAMINED != '' THEN 1 ELSE 0 END
     + CASE WHEN e.INSTITUTION_NAME IS NOT NULL AND e.INSTITUTION_NAME != '' THEN 1 ELSE 0 END
     + CASE WHEN e.INSTITUTIONAL_DEPT_NAME IS NOT NULL AND e.INSTITUTIONAL_DEPT_NAME != '' THEN 1 ELSE 0 END
     + CASE WHEN s.NUMBER_OF_SERIES > 0 THEN 1 ELSE 0 END
    ) / 10.0 * 100 AS metadata_completeness_pct,
    CASE WHEN s.STUDY_DATE > CURRENT_DATE() THEN TRUE ELSE FALSE END AS has_future_date,
    CASE WHEN ser.MODALITY IS NULL THEN TRUE ELSE FALSE END AS missing_modality,
    CASE WHEN ser.BODY_PART_EXAMINED IS NULL OR ser.BODY_PART_EXAMINED = '' THEN TRUE ELSE FALSE END AS missing_body_part
FROM {database}.{schema}.DICOM_STUDY s
LEFT JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_INSTANCE i ON ser.SERIES_KEY = i.SERIES_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY
GROUP BY s.STUDY_KEY, s.STUDY_INSTANCE_UID, ser.MODALITY, ser.BODY_PART_EXAMINED, e.INSTITUTION_NAME,
    s.STUDY_DATE, s.STUDY_TIME, s.STUDY_DESCRIPTION, s.REFERRING_PHYSICIAN,
    s.ACCESSION_NUMBER, e.INSTITUTIONAL_DEPT_NAME, s.NUMBER_OF_SERIES, s.NUMBER_OF_INSTANCES;

-- DT_RADIOLOGY_FINDINGS: NLP-extracted findings via Cortex Complete
CREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_RADIOLOGY_FINDINGS
    TARGET_LAG = '2 hours'
    WAREHOUSE = {warehouse}
AS
SELECT
    r.REPORT_KEY, r.STUDY_INSTANCE_UID, r.PATIENT_ID, r.RADIOLOGIST_NAME, r.REPORT_DATETIME, r.REPORT_TEXT,
    TRY_PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', CONCAT(
        'Extract structured information from this radiology report. ',
        'Return ONLY valid JSON: ',
        '{"findings":"key findings","impression":"clinical impression",',
        '"recommendations":"follow-up or empty string",',
        '"critical_flags":["urgent findings or empty array"],',
        '"body_part_mentioned":["body parts referenced"],',
        '"modality_mentioned":"modality or empty string","abnormal":true/false}',
        '\n\nRadiology Report:\n', r.REPORT_TEXT
    ))) AS extracted,
    extracted:findings::STRING AS findings,
    extracted:impression::STRING AS impression,
    extracted:recommendations::STRING AS recommendations,
    extracted:critical_flags AS critical_flags,
    extracted:body_part_mentioned AS body_part_mentioned,
    extracted:modality_mentioned::STRING AS modality_mentioned,
    extracted:abnormal::BOOLEAN AS is_abnormal
FROM {database}.{schema}.RADIOLOGY_REPORTS r
WHERE r.REPORT_TEXT IS NOT NULL AND LENGTH(r.REPORT_TEXT) > 10;
