USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

-- ============================================================
-- CORTEX SEARCH SERVICE OVER EXTRACTED CONDITIONS
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_ENTITY_SEARCH
    ON display
    ATTRIBUTES category, code_system, code, is_negated, temporality, certainty
    WAREHOUSE = BI_WH
    TARGET_LAG = '1 day'
AS (
    SELECT
        condition_id AS id,
        'CONDITION' AS entity_type,
        display,
        category,
        code,
        code_system,
        is_negated::VARCHAR AS is_negated,
        temporality,
        certainty,
        severity_display AS severity,
        body_site_display AS body_site,
        laterality,
        evidence_text
    FROM CONDITION
    WHERE source = 'GENAI_NLP_NOTE'

    UNION ALL

    SELECT
        medication_request_id,
        'MEDICATION',
        medication_display,
        NULL,
        medication_code,
        medication_system,
        is_negated::VARCHAR,
        temporality,
        NULL,
        NULL,
        NULL,
        NULL,
        evidence_text
    FROM MEDICATION_REQUEST
    WHERE source = 'GENAI_NLP_NOTE'

    UNION ALL

    SELECT
        observation_id,
        'OBSERVATION',
        display,
        category,
        code,
        code_system,
        is_negated::VARCHAR,
        temporality,
        certainty,
        NULL,
        body_site_display,
        laterality,
        evidence_text
    FROM OBSERVATION
    WHERE source = 'GENAI_NLP_NOTE'

    UNION ALL

    SELECT
        allergy_id,
        'ALLERGY',
        substance_display,
        NULL,
        substance_code,
        substance_system,
        is_negated::VARCHAR,
        NULL,
        NULL,
        severity,
        NULL,
        NULL,
        evidence_text
    FROM ALLERGY_INTOLERANCE
    WHERE source = 'GENAI_NLP_NOTE'

    UNION ALL

    SELECT
        adverse_event_id,
        'ADVERSE_EVENT',
        event_display,
        NULL,
        event_code,
        event_system,
        NULL,
        NULL,
        NULL,
        severity,
        NULL,
        NULL,
        evidence_text
    FROM ADVERSE_EVENT
    WHERE source = 'GENAI_NLP_NOTE'
);
