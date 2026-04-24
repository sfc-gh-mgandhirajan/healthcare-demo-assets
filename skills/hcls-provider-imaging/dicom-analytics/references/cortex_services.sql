-- =============================================================================
-- DICOM Analytics: Cortex Search, Semantic View, and Cortex Agent
-- All functionally tested against POLARIS1 (2026-04-13)
-- Database: {database}.{schema}
-- =============================================================================

-- IMAGING_SEARCH_CORPUS: Denormalized search corpus
CREATE OR REPLACE TABLE {database}.{schema}.IMAGING_SEARCH_CORPUS AS
SELECT s.STUDY_INSTANCE_UID AS CORPUS_ID, s.STUDY_INSTANCE_UID, p.PATIENT_ID,
    ser.MODALITY, ser.BODY_PART_EXAMINED, e.INSTITUTION_NAME,
    s.STUDY_DATE AS STUDY_DATE_PARSED, s.STUDY_DESCRIPTION, r.REPORT_TEXT,
    CONCAT('Study: ',COALESCE(s.STUDY_DESCRIPTION,''),' | Modality: ',COALESCE(ser.MODALITY,''),
        ' | Body Part: ',COALESCE(ser.BODY_PART_EXAMINED,''),' | Institution: ',COALESCE(e.INSTITUTION_NAME,''),
        ' | Report: ',COALESCE(r.REPORT_TEXT,'')) AS SEARCH_TEXT
FROM {database}.{schema}.DICOM_STUDY s
JOIN {database}.{schema}.DICOM_PATIENT p ON s.PATIENT_KEY = p.PATIENT_KEY
JOIN {database}.{schema}.DICOM_SERIES ser ON s.STUDY_KEY = ser.STUDY_KEY
LEFT JOIN {database}.{schema}.DICOM_EQUIPMENT e ON ser.SERIES_KEY = e.SERIES_KEY
LEFT JOIN {database}.{schema}.RADIOLOGY_REPORTS r ON s.STUDY_INSTANCE_UID = r.STUDY_INSTANCE_UID;

-- IMAGING_SEARCH_SVC: Cortex Search Service with faceted attributes
CREATE OR REPLACE CORTEX SEARCH SERVICE {database}.{schema}.IMAGING_SEARCH_SVC
    ON SEARCH_TEXT
    ATTRIBUTES MODALITY, BODY_PART_EXAMINED, INSTITUTION_NAME
    WAREHOUSE = {warehouse}
    TARGET_LAG = '1 hour'
AS (
    SELECT SEARCH_TEXT, STUDY_INSTANCE_UID, PATIENT_ID, MODALITY, BODY_PART_EXAMINED,
        INSTITUTION_NAME, STUDY_DATE_PARSED, STUDY_DESCRIPTION, REPORT_TEXT
    FROM {database}.{schema}.IMAGING_SEARCH_CORPUS
);

-- DICOM_ANALYTICS_SV: Semantic View for natural-language analytics
CREATE OR REPLACE SEMANTIC VIEW {database}.{schema}.DICOM_ANALYTICS_SV
    TABLES (
        {database}.{schema}.DICOM_STUDY,
        {database}.{schema}.DICOM_SERIES,
        {database}.{schema}.DICOM_PATIENT,
        {database}.{schema}.DICOM_EQUIPMENT
    )
    FACTS (
        DICOM_STUDY.NUMBER_OF_SERIES AS NUMBER_OF_SERIES,
        DICOM_STUDY.NUMBER_OF_INSTANCES AS NUMBER_OF_INSTANCES
    )
    DIMENSIONS (
        DICOM_STUDY.STUDY_INSTANCE_UID AS STUDY_INSTANCE_UID,
        DICOM_STUDY.REFERRING_PHYSICIAN AS REFERRING_PHYSICIAN,
        DICOM_STUDY.STUDY_DESCRIPTION AS STUDY_DESCRIPTION,
        DICOM_STUDY.STUDY_DATE AS STUDY_DATE,
        DICOM_SERIES.MODALITY AS MODALITY,
        DICOM_SERIES.BODY_PART_EXAMINED AS BODY_PART_EXAMINED,
        DICOM_PATIENT.PATIENT_ID AS PATIENT_ID,
        DICOM_PATIENT.PATIENT_NAME AS PATIENT_NAME,
        DICOM_PATIENT.PATIENT_SEX AS PATIENT_SEX,
        DICOM_PATIENT.PATIENT_BIRTH_DATE AS PATIENT_BIRTH_DATE,
        DICOM_EQUIPMENT.MANUFACTURER AS MANUFACTURER,
        DICOM_EQUIPMENT.MANUFACTURER_MODEL_NAME AS MANUFACTURER_MODEL_NAME,
        DICOM_EQUIPMENT.STATION_NAME AS STATION_NAME,
        DICOM_EQUIPMENT.INSTITUTION_NAME AS INSTITUTION_NAME,
        DICOM_EQUIPMENT.INSTITUTIONAL_DEPT_NAME AS INSTITUTIONAL_DEPT_NAME
    );

-- IMAGING_ANALYTICS_AGENT: Cortex Agent (Semantic View + Cortex Search)
CREATE OR REPLACE AGENT {database}.{schema}.IMAGING_ANALYTICS_AGENT
  COMMENT = 'Conversational DICOM analytics: structured queries via Semantic View + report search via Cortex Search'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-4-sonnet

  instructions:
    system: "You are a radiology analytics assistant helping clinical and operational users analyze DICOM imaging metadata, study volumes, equipment utilization, and radiology findings. Use the Analyst tool for structured queries about volumes, modalities, equipment, and demographics. Use the Search tool for finding studies by clinical content or report findings."
    response: "Provide clear, data-backed answers with modality/institution context."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "ImagingAnalyst"
        description: "Converts natural language questions about imaging studies into SQL"
    - tool_spec:
        type: "cortex_search"
        name: "ReportSearch"
        description: "Searches radiology reports for clinical findings"

  tool_resources:
    ImagingAnalyst:
      semantic_view: "{database}.{schema}.DICOM_ANALYTICS_SV"
      execution_warehouse: "{warehouse}"
    ReportSearch:
      name: "{database}.{schema}.IMAGING_SEARCH_SVC"
      max_results: "10"
  $$;
