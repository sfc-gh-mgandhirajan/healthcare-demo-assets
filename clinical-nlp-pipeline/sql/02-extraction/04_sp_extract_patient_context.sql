USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_PATIENT_CONTEXT(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_total_docs INT;
    v_batch_num INT DEFAULT 0;
    v_batch_offset INT DEFAULT 0;
    v_total_social INT DEFAULT 0;
    v_total_family INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-PCTX';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM SOCIAL_HISTORY_OBSERVATION s WHERE s.provenance_document_id = nd.document_id AND s.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_PATIENT_CONTEXT', 4, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        CREATE OR REPLACE TEMPORARY TABLE _PCTX_BATCH AS
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM SOCIAL_HISTORY_OBSERVATION s WHERE s.provenance_document_id = nd.document_id AND s.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        )
        SELECT
            document_id, patient_id,
            TRY_PARSE_JSON(
                SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                    'Extract ALL social history and family history from this clinical note. Return ONLY a valid JSON object:
{
  "social_history": [{"display":"","sdoh_domain":"TOBACCO|ALCOHOL|SUBSTANCE_USE|EMPLOYMENT|HOUSING|FOOD_INSECURITY|TRANSPORTATION|FINANCIAL_STRAIN|EDUCATION|SOCIAL_ISOLATION|SAFETY|VETERAN_STATUS|OTHER","status":"ACTIVE|RESOLVED|UNKNOWN","value_quantity":null,"value_unit":"","value_string":"","evidence_text":"","extraction_confidence":0.9}],
  "family_history": [{"relationship_code":"MOTHER|FATHER|SIBLING|CHILD|OTHER","relationship_display":"","condition_display":"","onset_age":null,"deceased_flag":false,"is_negated":false,"evidence_text":"","extraction_confidence":0.9}]
}
If none, use empty arrays. Return ONLY JSON.

Clinical note:
' || LEFT(raw_text, 6000))
            ) AS result_json
        FROM batch_docs;

        INSERT INTO SOCIAL_HISTORY_OBSERVATION (
            social_history_id, patient_id, display, sdoh_domain, status,
            value_quantity, value_unit, value_string, evidence_text, extraction_confidence,
            provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-SOC-' || f.index::VARCHAR),
            b.patient_id,
            f.value:display::VARCHAR,
            f.value:sdoh_domain::VARCHAR,
            COALESCE(f.value:status::VARCHAR, 'UNKNOWN'),
            TRY_CAST(f.value:value_quantity::VARCHAR AS NUMBER),
            f.value:value_unit::VARCHAR,
            f.value:value_string::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _PCTX_BATCH b, LATERAL FLATTEN(input => b.result_json:social_history) f
        WHERE b.result_json IS NOT NULL AND f.value:display::VARCHAR IS NOT NULL AND TRIM(f.value:display::VARCHAR) != '';
        v_total_social := v_total_social + SQLROWCOUNT;

        INSERT INTO FAMILY_MEMBER_HISTORY (
            family_history_id, patient_id, relationship_code, relationship_display,
            condition_display, onset_age, deceased_flag, is_negated,
            evidence_text, extraction_confidence, provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-FAM-' || f.index::VARCHAR),
            b.patient_id,
            COALESCE(f.value:relationship_code::VARCHAR, 'OTHER'),
            COALESCE(f.value:relationship_display::VARCHAR, f.value:relationship_code::VARCHAR),
            f.value:condition_display::VARCHAR,
            TRY_CAST(f.value:onset_age::VARCHAR AS INT),
            f.value:deceased_flag::BOOLEAN,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _PCTX_BATCH b, LATERAL FLATTEN(input => b.result_json:family_history) f
        WHERE b.result_json IS NOT NULL AND f.value:condition_display::VARCHAR IS NOT NULL AND TRIM(f.value:condition_display::VARCHAR) != '';
        v_total_family := v_total_family + SQLROWCOUNT;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_PATIENT_CONTEXT_BATCH', 4, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
               :v_total_social + :v_total_family, NULL, :v_batch_num, :BATCH_SIZE,
               OBJECT_CONSTRUCT('social', :v_total_social, 'family', :v_total_family);

        v_batch_offset := v_batch_offset + :BATCH_SIZE;
        DROP TABLE IF EXISTS _PCTX_BATCH;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(),
        rows_inserted = :v_total_social + :v_total_family
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_PATIENT_CONTEXT';

    RETURN 'PATIENT CONTEXT: ' || v_total_social || ' social, ' || v_total_family || ' family from ' || v_total_docs || ' docs';
END;
$$;
