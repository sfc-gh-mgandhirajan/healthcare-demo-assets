USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_ONCOLOGY(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
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
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-ONC';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM TUMOR_EPISODE t WHERE t.provenance_document_id = nd.document_id AND t.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_ONCOLOGY', 5, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        INSERT INTO TUMOR_EPISODE (
            tumor_episode_id, patient_id, primary_condition_id, primary_site_display,
            histology_display, stage_group, tnm_t, tnm_n, tnm_m, grade,
            performance_status_scale, performance_status_value, response_status,
            certainty, evidence_text, extraction_confidence, provenance_document_id, source
        )
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM TUMOR_EPISODE t WHERE t.provenance_document_id = nd.document_id AND t.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        ),
        extraction AS (
            SELECT
                document_id, patient_id,
                TRY_PARSE_JSON(
                    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                        'Extract ALL tumor/cancer episodes from this clinical note. Only extract if the note contains cancer/tumor/malignancy information. Return ONLY a valid JSON array. Each object:
- primary_site_display: anatomical site of primary tumor (e.g. "lung", "breast", "colon")
- histology_display: histological type (e.g. "adenocarcinoma", "squamous cell carcinoma")
- stage_group: overall stage (e.g. "IIA", "IIIB", "IV")
- tnm_t: T component (e.g. "T2", "T3a")
- tnm_n: N component (e.g. "N0", "N1")
- tnm_m: M component (e.g. "M0", "M1")
- grade: tumor grade (e.g. "G1", "well differentiated")
- performance_status_scale: ECOG or KARNOFSKY, or null
- performance_status_value: numeric value or null
- response_status: CR, PR, SD, PD, UNKNOWN, or null
- certainty: CONFIRMED, PROBABLE, POSSIBLE
- evidence_text: exact quote (max 200 chars)
- extraction_confidence: 0.0 to 1.0

If no cancer/tumor info found, return []. Return ONLY the JSON array.

Clinical note:
' || LEFT(raw_text, 6000))
                ) AS onc_json
            FROM batch_docs
        )
        SELECT
            MD5(e.document_id || '-ONC-' || f.index::VARCHAR),
            e.patient_id,
            MD5(e.document_id || '-COND-ONC-' || f.index::VARCHAR),
            f.value:primary_site_display::VARCHAR,
            f.value:histology_display::VARCHAR,
            f.value:stage_group::VARCHAR,
            f.value:tnm_t::VARCHAR,
            f.value:tnm_n::VARCHAR,
            f.value:tnm_m::VARCHAR,
            f.value:grade::VARCHAR,
            f.value:performance_status_scale::VARCHAR,
            f.value:performance_status_value::VARCHAR,
            f.value:response_status::VARCHAR,
            f.value:certainty::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            e.document_id,
            'GENAI_NLP_NOTE'
        FROM extraction e, LATERAL FLATTEN(input => e.onc_json) f
        WHERE e.onc_json IS NOT NULL AND f.value:primary_site_display::VARCHAR IS NOT NULL AND TRIM(f.value:primary_site_display::VARCHAR) != '';

        v_inserted := SQLROWCOUNT;
        v_total_inserted := v_total_inserted + v_inserted;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_ONCOLOGY_BATCH', 5, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, :v_inserted, NULL, :v_batch_num, :BATCH_SIZE, NULL;

        v_batch_offset := v_batch_offset + :BATCH_SIZE;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(), rows_inserted = :v_total_inserted
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_ONCOLOGY';

    RETURN 'ONCOLOGY: ' || v_total_inserted || ' tumor episodes from ' || v_total_docs || ' docs in ' || v_batch_num || ' batches';
END;
$$;
