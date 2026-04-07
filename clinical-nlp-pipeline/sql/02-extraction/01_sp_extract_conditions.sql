USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_CONDITIONS(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_total_docs INT;
    v_batch_num INT DEFAULT 0;
    v_batch_offset INT DEFAULT 0;
    v_inserted INT DEFAULT 0;
    v_total_inserted INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-COND';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM CONDITION c WHERE c.provenance_document_id = nd.document_id AND c.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_CONDITIONS', 1, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        INSERT INTO CONDITION (
            condition_id, patient_id, encounter_id, display, category, clinical_status,
            severity_display, body_site_display, laterality, is_negated, temporality,
            certainty, evidence_text, extraction_confidence, provenance_document_id, source
        )
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM CONDITION c WHERE c.provenance_document_id = nd.document_id AND c.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        ),
        extraction AS (
            SELECT
                document_id, patient_id,
                TRY_PARSE_JSON(
                    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                        'Extract ALL medical conditions, diagnoses, symptoms, and risk factors from this clinical note. Return ONLY a valid JSON array. Each object must have these fields:
- display: condition name (string)
- category: one of PROBLEM_LIST_ITEM, ENCOUNTER_DIAGNOSIS, SYMPTOM, RISK_FACTOR, HISTORY_OF
- clinical_status: one of active, recurrence, resolved, inactive, unknown
- severity_display: one of mild, moderate, severe, or null
- body_site_display: anatomical location or null
- laterality: one of LEFT, RIGHT, BILATERAL, or null
- is_negated: true if the condition is explicitly denied/absent, false otherwise
- temporality: one of CURRENT, HISTORICAL, FUTURE
- certainty: one of CONFIRMED, PROBABLE, POSSIBLE, UNLIKELY, RULED_OUT
- evidence_text: exact quote from note supporting this finding (max 200 chars)
- extraction_confidence: 0.0 to 1.0

If no conditions found, return []. Return ONLY the JSON array, no other text.

Clinical note:
' || LEFT(raw_text, 6000))
                ) AS conditions_json
            FROM batch_docs
        )
        SELECT
            MD5(e.document_id || '-COND-' || f.index::VARCHAR) AS condition_id,
            e.patient_id,
            NULL,
            f.value:display::VARCHAR,
            f.value:category::VARCHAR,
            f.value:clinical_status::VARCHAR,
            f.value:severity_display::VARCHAR,
            f.value:body_site_display::VARCHAR,
            f.value:laterality::VARCHAR,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:temporality::VARCHAR,
            f.value:certainty::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            e.document_id,
            'GENAI_NLP_NOTE'
        FROM extraction e, LATERAL FLATTEN(input => e.conditions_json) f
        WHERE e.conditions_json IS NOT NULL AND f.value:display::VARCHAR IS NOT NULL AND TRIM(f.value:display::VARCHAR) != '';

        v_inserted := SQLROWCOUNT;
        v_total_inserted := v_total_inserted + v_inserted;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_CONDITIONS_BATCH', 1, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), :BATCH_SIZE, :v_inserted, NULL, :v_batch_num, :BATCH_SIZE, NULL;

        v_batch_offset := v_batch_offset + :BATCH_SIZE;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(), rows_inserted = :v_total_inserted
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_CONDITIONS';

    RETURN 'CONDITIONS: ' || v_total_inserted || ' entities extracted from ' || v_total_docs || ' docs in ' || v_batch_num || ' batches';
END;
$$;
