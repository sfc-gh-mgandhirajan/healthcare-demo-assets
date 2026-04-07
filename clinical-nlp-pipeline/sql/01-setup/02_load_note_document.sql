USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

-- ============================================================
-- POPULATE NOTE_DOCUMENT FROM DEDUP SOURCE
-- ============================================================
INSERT INTO NOTE_DOCUMENT (
    document_id, patient_id, encounter_id, note_type, source_system, created_datetime, author, raw_text
)
SELECT
    MD5(RELATIVE_PATH) AS document_id,
    MD5(COALESCE(PATIENT_NAME, 'UNKNOWN') || COALESCE(TO_VARCHAR(PATIENT_DOB), '')) AS patient_id,
    NULL AS encounter_id,
    CASE 
        WHEN LOWER(SPECIALTY) LIKE '%radiology%' THEN 'RADIOLOGY_REPORT'
        WHEN LOWER(SPECIALTY) LIKE '%pathology%' THEN 'PATHOLOGY_REPORT'
        WHEN LOWER(SPECIALTY) LIKE '%consult%' THEN 'CONSULT_NOTE'
        WHEN LOWER(SPECIALTY) LIKE '%surgery%' OR LOWER(SPECIALTY) LIKE '%orthop%' THEN 'H_AND_P'
        WHEN LOWER(SPECIALTY) LIKE '%discharge%' THEN 'DISCHARGE_SUMMARY'
        WHEN LOWER(SPECIALTY) LIKE '%progress%' OR LOWER(SPECIALTY) LIKE '%soap%' THEN 'PROGRESS'
        WHEN LOWER(SPECIALTY) LIKE '%emergency%' OR LOWER(SPECIALTY) LIKE '%er %' THEN 'ED_NOTE'
        ELSE 'H_AND_P'
    END AS note_type,
    'MED_TRANSCRIPTS' AS source_system,
    CURRENT_TIMESTAMP() AS created_datetime,
    NULL AS author,
    FULL_TEXT AS raw_text
FROM NOTE_DOCUMENT_SOURCE src
WHERE NOT EXISTS (
    SELECT 1 FROM NOTE_DOCUMENT nd WHERE nd.document_id = MD5(src.RELATIVE_PATH)
);
