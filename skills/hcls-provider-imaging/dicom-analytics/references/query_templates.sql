-- =============================================================================
-- DICOM Analytics: Ad-hoc Query Templates
-- All functionally tested against POLARIS1 (2026-04-13)
-- Database: {database}.{schema}
-- =============================================================================

-- Cross-modality cohort: Patients with CT + MRI of same body part
SELECT p.PATIENT_ID, ct_ser.BODY_PART_EXAMINED, ct_s.STUDY_DATE AS ct_date, mr_s.STUDY_DATE AS mr_date,
    DATEDIFF('day', ct_s.STUDY_DATE, mr_s.STUDY_DATE) AS days_between
FROM {database}.{schema}.DICOM_PATIENT p
JOIN {database}.{schema}.DICOM_STUDY ct_s ON p.PATIENT_KEY = ct_s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ct_ser ON ct_s.STUDY_KEY = ct_ser.STUDY_KEY AND ct_ser.MODALITY = 'CT'
JOIN {database}.{schema}.DICOM_STUDY mr_s ON p.PATIENT_KEY = mr_s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES mr_ser ON mr_s.STUDY_KEY = mr_ser.STUDY_KEY AND mr_ser.MODALITY = 'MR' AND ct_ser.BODY_PART_EXAMINED = mr_ser.BODY_PART_EXAMINED
ORDER BY p.PATIENT_ID;

-- Frequency-based cohort: Patients with >N studies of a modality in M months
SELECT p.PATIENT_ID, ser.MODALITY, COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS study_count,
    MIN(s.STUDY_DATE) AS period_start, MAX(s.STUDY_DATE) AS period_end
FROM {database}.{schema}.DICOM_PATIENT p
JOIN {database}.{schema}.DICOM_STUDY s ON p.PATIENT_KEY = s.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
WHERE s.STUDY_DATE >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY p.PATIENT_ID, ser.MODALITY HAVING COUNT(DISTINCT s.STUDY_INSTANCE_UID) > 3 ORDER BY study_count DESC;

-- Equipment downtime detection (gaps >4 hours per scanner on same day)
WITH scanner_timeline AS (
    SELECT e.STATION_NAME, e.MANUFACTURER, e.MANUFACTURER_MODEL_NAME,
        TIMESTAMP_FROM_PARTS(s.STUDY_DATE, s.STUDY_TIME) AS study_ts,
        LAG(TIMESTAMP_FROM_PARTS(s.STUDY_DATE, s.STUDY_TIME))
            OVER (PARTITION BY e.STATION_NAME ORDER BY s.STUDY_DATE, s.STUDY_TIME) AS prev_ts
    FROM {database}.{schema}.DICOM_EQUIPMENT e
    JOIN {database}.{schema}.DICOM_SERIES ser ON e.SERIES_KEY = ser.SERIES_KEY
    JOIN {database}.{schema}.DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
    WHERE s.STUDY_DATE IS NOT NULL AND s.STUDY_TIME IS NOT NULL
)
SELECT STATION_NAME, MANUFACTURER, MANUFACTURER_MODEL_NAME, prev_ts AS gap_start, study_ts AS gap_end,
    DATEDIFF('minute', prev_ts, study_ts) AS gap_minutes
FROM scanner_timeline
WHERE prev_ts IS NOT NULL AND DATEDIFF('minute', prev_ts, study_ts) > 240 AND TO_DATE(prev_ts) = TO_DATE(study_ts)
ORDER BY gap_minutes DESC;

-- Comparative equipment performance
SELECT e.STATION_NAME, e.MANUFACTURER, e.MANUFACTURER_MODEL_NAME,
    COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS total_studies, COUNT(DISTINCT s.PATIENT_KEY) AS unique_patients,
    AVG(s.NUMBER_OF_INSTANCES) AS avg_images_per_study,
    COUNT(DISTINCT s.STUDY_DATE) AS active_days,
    ROUND(COUNT(DISTINCT s.STUDY_INSTANCE_UID)/NULLIF(COUNT(DISTINCT s.STUDY_DATE),0),1) AS avg_studies_per_day
FROM {database}.{schema}.DICOM_EQUIPMENT e
JOIN {database}.{schema}.DICOM_SERIES ser ON e.SERIES_KEY = ser.SERIES_KEY
JOIN {database}.{schema}.DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
GROUP BY e.STATION_NAME, e.MANUFACTURER, e.MANUFACTURER_MODEL_NAME ORDER BY total_studies DESC;

-- Repeat study detection (same modality + body part within 7 days)
WITH ordered AS (
    SELECT p.PATIENT_ID, s.STUDY_INSTANCE_UID, ser.MODALITY, ser.BODY_PART_EXAMINED,
        s.STUDY_DATE AS study_date,
        LAG(s.STUDY_DATE)
            OVER (PARTITION BY p.PATIENT_ID, ser.MODALITY, ser.BODY_PART_EXAMINED ORDER BY s.STUDY_DATE) AS prev_date
    FROM {database}.{schema}.DICOM_PATIENT p
    JOIN {database}.{schema}.DICOM_STUDY s ON p.PATIENT_KEY = s.PATIENT_KEY
    JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
)
SELECT *, DATEDIFF('day', prev_date, study_date) AS days_between_repeats
FROM ordered WHERE prev_date IS NOT NULL AND DATEDIFF('day', prev_date, study_date) <= 7;

-- Population-level temporal trends (monthly volume)
SELECT DATE_TRUNC('month', s.STUDY_DATE) AS study_month,
    ser.MODALITY, e.INSTITUTION_NAME,
    COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS study_count, COUNT(DISTINCT s.PATIENT_KEY) AS patient_count,
    SUM(s.NUMBER_OF_INSTANCES) AS total_images
FROM {database}.{schema}.DICOM_STUDY s
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY
WHERE s.STUDY_DATE IS NOT NULL
GROUP BY study_month, ser.MODALITY, e.INSTITUTION_NAME ORDER BY study_month DESC, study_count DESC;

-- Metadata anomaly detection
SELECT 'Missing Study Date' AS anomaly_type, COUNT(*) AS count FROM {database}.{schema}.DICOM_STUDY WHERE STUDY_DATE IS NULL
UNION ALL SELECT 'Future Study Date', COUNT(*) FROM {database}.{schema}.DICOM_STUDY WHERE STUDY_DATE > CURRENT_DATE()
UNION ALL SELECT 'Missing Modality', COUNT(*) FROM {database}.{schema}.DICOM_SERIES WHERE MODALITY IS NULL
UNION ALL SELECT 'Missing Body Part', COUNT(*) FROM {database}.{schema}.DICOM_SERIES WHERE BODY_PART_EXAMINED IS NULL OR BODY_PART_EXAMINED = ''
UNION ALL SELECT 'Duplicate Study UID', COUNT(*) FROM (SELECT STUDY_INSTANCE_UID FROM {database}.{schema}.DICOM_STUDY GROUP BY STUDY_INSTANCE_UID HAVING COUNT(*) > 1)
UNION ALL SELECT 'Zero-Instance Series', COUNT(*) FROM {database}.{schema}.DICOM_SERIES ser LEFT JOIN {database}.{schema}.DICOM_INSTANCE i ON ser.SERIES_KEY = i.SERIES_KEY WHERE i.INSTANCE_KEY IS NULL
ORDER BY count DESC;

-- Turnaround-time analytics (report date vs study date)
SELECT ser.MODALITY, e.INSTITUTION_NAME, COUNT(*) AS report_count,
    AVG(DATEDIFF('day', s.STUDY_DATE, r.REPORT_DATETIME::DATE)) AS avg_tat_days,
    MEDIAN(DATEDIFF('day', s.STUDY_DATE, r.REPORT_DATETIME::DATE)) AS median_tat_days,
    MAX(DATEDIFF('day', s.STUDY_DATE, r.REPORT_DATETIME::DATE)) AS max_tat_days
FROM {database}.{schema}.RADIOLOGY_REPORTS r
JOIN {database}.{schema}.DICOM_STUDY s ON r.STUDY_INSTANCE_UID = s.STUDY_INSTANCE_UID
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY
GROUP BY ser.MODALITY, e.INSTITUTION_NAME ORDER BY avg_tat_days DESC;

-- Cortex Search: Semantic search
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '{database}.{schema}.IMAGING_SEARCH_SVC',
    '{"query":"chest CT with pulmonary nodule","columns":["STUDY_INSTANCE_UID","MODALITY","BODY_PART_EXAMINED","STUDY_DESCRIPTION"],"limit":10}'
));

-- Cortex Search: Faceted search (single filter)
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '{database}.{schema}.IMAGING_SEARCH_SVC',
    '{"query":"lung mass suspicious for malignancy","columns":["STUDY_INSTANCE_UID","MODALITY","BODY_PART_EXAMINED","REPORT_TEXT"],
      "filter":{"@eq":{"MODALITY":"CT"}},"limit":10}'
));

-- Cortex Search: Faceted search (combined AND filters)
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '{database}.{schema}.IMAGING_SEARCH_SVC',
    '{"query":"fracture displacement","columns":["STUDY_INSTANCE_UID","MODALITY","BODY_PART_EXAMINED","INSTITUTION_NAME"],
      "filter":{"@and":[{"@eq":{"MODALITY":"CR"}},{"@eq":{"BODY_PART_EXAMINED":"EXTREMITY"}}]},"limit":10}'
));

-- Cortex Agent: Example prompts
SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    '{database}.{schema}.IMAGING_ANALYTICS_AGENT',
    $${"messages": [{"role": "user", "content": [{"type": "text", "text": "How many CT studies were performed per institution last quarter?"}]}]}$$
  )
) AS resp;

SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    '{database}.{schema}.IMAGING_ANALYTICS_AGENT',
    $${"messages": [{"role": "user", "content": [{"type": "text", "text": "Find all studies mentioning pulmonary embolism in radiology reports"}]}]}$$
  )
) AS resp;
