-- =============================================================================
-- 06: Cortex Search Service
-- Enables semantic (vector) search over DICOM study metadata.
-- Powers the Cortex Agent chat interface and natural-language queries.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

CREATE OR REPLACE CORTEX SEARCH SERVICE DICOM_STUDY_SEARCH
    ON SEARCH_TEXT
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT
            SERIES_INSTANCE_UID,
            SERIES_LABEL || ' | ' || COALESCE(STUDY_DESCRIPTION, '')
                || ' | ' || COALESCE(SERIES_DESCRIPTION, '')
                || ' | ' || COALESCE(PROTOCOL_NAME, '')
                || ' | ' || COALESCE(MANUFACTURER, '') AS SEARCH_TEXT,
            COLLECTION_NAME,
            MODALITY,
            MANUFACTURER,
            BODY_PART_EXAMINED,
            PATIENT_ID
        FROM EW_IMAGING_DB.EXPLORER.DICOM_CARDIAC_METADATA
        GROUP BY ALL
    );
