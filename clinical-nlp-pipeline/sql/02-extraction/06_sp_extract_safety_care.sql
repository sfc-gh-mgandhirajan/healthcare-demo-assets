USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_SAFETY_CARE(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_total_docs INT;
    v_batch_num INT DEFAULT 0;
    v_batch_offset INT DEFAULT 0;
    v_total_adverse INT DEFAULT 0;
    v_total_care INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-SAFE';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM ADVERSE_EVENT ae WHERE ae.provenance_document_id = nd.document_id AND ae.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_SAFETY_CARE', 6, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        CREATE OR REPLACE TEMPORARY TABLE _SAFE_BATCH AS
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM ADVERSE_EVENT ae WHERE ae.provenance_document_id = nd.document_id AND ae.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        )
        SELECT
            document_id, patient_id,
            TRY_PARSE_JSON(
                SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                    'Extract ALL adverse events and care plan items from this clinical note. Return ONLY a valid JSON object:
{
  "adverse_events": [{"event_display":"","seriousness":"SERIOUS|NON_SERIOUS|UNKNOWN","severity":"MILD|MODERATE|SEVERE|LIFE_THREATENING","outcome":"RECOVERED|RECOVERING|FATAL|UNKNOWN|OTHER","evidence_text":"","extraction_confidence":0.9}],
  "care_plan_items": [{"item_type":"GOAL|ACTION|REFERRAL|FOLLOW_UP|EDUCATION|MONITORING","description":"","status":"PLANNED|IN_PROGRESS|COMPLETED|CANCELLED|UNKNOWN","evidence_text":"","extraction_confidence":0.9}]
}
Adverse events include drug reactions, procedure complications, falls, injuries, infections. Care plan items include goals, referrals, follow-ups, patient education, monitoring plans.
If none, use empty arrays. Return ONLY JSON.

Clinical note:
' || LEFT(raw_text, 6000))
            ) AS result_json
        FROM batch_docs;

        INSERT INTO ADVERSE_EVENT (
            adverse_event_id, patient_id, event_display, seriousness, severity,
            outcome, evidence_text, extraction_confidence, provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-AE-' || f.index::VARCHAR),
            b.patient_id,
            f.value:event_display::VARCHAR,
            f.value:seriousness::VARCHAR,
            f.value:severity::VARCHAR,
            f.value:outcome::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _SAFE_BATCH b, LATERAL FLATTEN(input => b.result_json:adverse_events) f
        WHERE b.result_json IS NOT NULL AND f.value:event_display::VARCHAR IS NOT NULL AND TRIM(f.value:event_display::VARCHAR) != '';
        v_total_adverse := v_total_adverse + SQLROWCOUNT;

        INSERT INTO CARE_PLAN_ITEM (
            care_plan_item_id, patient_id, item_type, description, status,
            evidence_text, extraction_confidence, provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-CP-' || f.index::VARCHAR),
            b.patient_id,
            COALESCE(f.value:item_type::VARCHAR, 'ACTION'),
            f.value:description::VARCHAR,
            f.value:status::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _SAFE_BATCH b, LATERAL FLATTEN(input => b.result_json:care_plan_items) f
        WHERE b.result_json IS NOT NULL AND f.value:description::VARCHAR IS NOT NULL AND TRIM(f.value:description::VARCHAR) != '';
        v_total_care := v_total_care + SQLROWCOUNT;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_SAFETY_CARE_BATCH', 6, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
               :v_total_adverse + :v_total_care, NULL, :v_batch_num, :BATCH_SIZE,
               OBJECT_CONSTRUCT('adverse', :v_total_adverse, 'care_plan', :v_total_care);

        v_batch_offset := v_batch_offset + :BATCH_SIZE;
        DROP TABLE IF EXISTS _SAFE_BATCH;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(),
        rows_inserted = :v_total_adverse + :v_total_care
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_SAFETY_CARE';

    RETURN 'SAFETY/CARE: ' || v_total_adverse || ' adverse events, ' || v_total_care || ' care plan items from ' || v_total_docs || ' docs';
END;
$$;
