USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_OBSERVATIONS(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_total_docs INT;
    v_batch_num INT DEFAULT 0;
    v_batch_offset INT DEFAULT 0;
    v_total_inserted INT DEFAULT 0;
    v_inserted INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-OBS';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM OBSERVATION o WHERE o.provenance_document_id = nd.document_id AND o.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_OBSERVATIONS', 3, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        INSERT INTO OBSERVATION (
            observation_id, patient_id, display, category, value_quantity, value_unit,
            value_string, interpretation, method, body_site_display, laterality,
            is_negated, temporality, certainty, evidence_text, extraction_confidence,
            provenance_document_id, source
        )
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM OBSERVATION o WHERE o.provenance_document_id = nd.document_id AND o.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        ),
        extraction AS (
            SELECT
                document_id, patient_id,
                TRY_PARSE_JSON(
                    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                        'Extract ALL clinical observations from this note: vital signs, lab results, imaging findings, physical exam findings, and clinical scores. Return ONLY a valid JSON array. Each object:
- display: observation name (e.g. "Blood Pressure", "Hemoglobin", "Chest X-ray")
- category: one of VITAL_SIGNS, LAB, IMAGING, EXAM, SCORE, OTHER
- value_quantity: numeric value or null
- value_unit: unit of measurement or null
- value_string: text value for non-numeric (e.g. "normal", "clear to auscultation")
- interpretation: one of HIGH, LOW, ABNORMAL, NORMAL, CRITICAL_HIGH, CRITICAL_LOW, or null
- method: measurement method if mentioned, or null
- body_site_display: anatomical location or null
- laterality: LEFT, RIGHT, BILATERAL, or null
- is_negated: true if explicitly absent
- temporality: CURRENT or HISTORICAL
- certainty: CONFIRMED, PROBABLE, POSSIBLE, or UNLIKELY
- evidence_text: exact quote from note (max 200 chars)
- extraction_confidence: 0.0 to 1.0

If no observations found, return []. Return ONLY the JSON array.

Clinical note:
' || LEFT(raw_text, 6000))
                ) AS obs_json
            FROM batch_docs
        )
        SELECT
            MD5(e.document_id || '-OBS-' || f.index::VARCHAR),
            e.patient_id,
            f.value:display::VARCHAR,
            f.value:category::VARCHAR,
            TRY_CAST(f.value:value_quantity::VARCHAR AS NUMBER),
            f.value:value_unit::VARCHAR,
            f.value:value_string::VARCHAR,
            f.value:interpretation::VARCHAR,
            f.value:method::VARCHAR,
            f.value:body_site_display::VARCHAR,
            f.value:laterality::VARCHAR,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:temporality::VARCHAR,
            f.value:certainty::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            e.document_id,
            'GENAI_NLP_NOTE'
        FROM extraction e, LATERAL FLATTEN(input => e.obs_json) f
        WHERE e.obs_json IS NOT NULL AND f.value:display::VARCHAR IS NOT NULL AND TRIM(f.value:display::VARCHAR) != '';

        v_inserted := SQLROWCOUNT;
        v_total_inserted := v_total_inserted + v_inserted;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_OBSERVATIONS_BATCH', 3, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, :v_inserted, NULL, :v_batch_num, :BATCH_SIZE, NULL;

        v_batch_offset := v_batch_offset + :BATCH_SIZE;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(), rows_inserted = :v_total_inserted
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_OBSERVATIONS';

    RETURN 'OBSERVATIONS: ' || v_total_inserted || ' entities from ' || v_total_docs || ' docs in ' || v_batch_num || ' batches';
END;
$$;
