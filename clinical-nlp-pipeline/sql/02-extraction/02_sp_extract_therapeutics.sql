USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_EXTRACT_THERAPEUTICS(BATCH_SIZE INT DEFAULT 500, SAMPLE_LIMIT INT DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_total_docs INT;
    v_batch_num INT DEFAULT 0;
    v_batch_offset INT DEFAULT 0;
    v_total_meds INT DEFAULT 0;
    v_total_procs INT DEFAULT 0;
    v_total_allergies INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-THER';

    SELECT COUNT(*) INTO v_total_docs
    FROM NOTE_DOCUMENT nd
    WHERE NOT EXISTS (
        SELECT 1 FROM MEDICATION_REQUEST m WHERE m.provenance_document_id = nd.document_id AND m.source = 'GENAI_NLP_NOTE'
    );

    IF (:SAMPLE_LIMIT IS NOT NULL AND :SAMPLE_LIMIT < v_total_docs) THEN
        v_total_docs := :SAMPLE_LIMIT;
    END IF;

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'EXTRACT_THERAPEUTICS', 2, 'RUNNING', CURRENT_TIMESTAMP(), NULL, :v_total_docs, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('batch_size', :BATCH_SIZE, 'total_docs', :v_total_docs);

    WHILE (v_batch_offset < v_total_docs) DO
        v_batch_num := v_batch_num + 1;

        -- Extract medications, procedures, allergies in one Cortex call per doc
        CREATE OR REPLACE TEMPORARY TABLE _THER_BATCH AS
        WITH batch_docs AS (
            SELECT nd.document_id, nd.patient_id, nd.raw_text
            FROM NOTE_DOCUMENT nd
            WHERE NOT EXISTS (
                SELECT 1 FROM MEDICATION_REQUEST m WHERE m.provenance_document_id = nd.document_id AND m.source = 'GENAI_NLP_NOTE'
            )
            ORDER BY nd.document_id
            LIMIT :BATCH_SIZE OFFSET :v_batch_offset
        )
        SELECT
            document_id, patient_id,
            TRY_PARSE_JSON(
                SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                    'Extract ALL medications, procedures, and allergies from this clinical note. Return ONLY a valid JSON object with three arrays:
{
  "medications": [{"medication_display":"","dosage_text":"","dose":null,"dose_unit":"","route_display":"","frequency_text":"","status":"ACTIVE|COMPLETED|STOPPED|UNKNOWN","intent":"ORDER|PLAN|PROPOSAL","is_negated":false,"temporality":"CURRENT|HISTORICAL|FUTURE","evidence_text":"","extraction_confidence":0.9}],
  "procedures": [{"display":"","category":"SURGICAL|DIAGNOSTIC|IMAGING|THERAPEUTIC|OTHER","status":"COMPLETED|IN_PROGRESS|NOT_DONE|UNKNOWN","body_site_display":"","laterality":"LEFT|RIGHT|BILATERAL|null","is_negated":false,"temporality":"CURRENT|HISTORICAL|FUTURE","evidence_text":"","extraction_confidence":0.9}],
  "allergies": [{"substance_display":"","criticality":"LOW|HIGH|UNKNOWN","severity":"MILD|MODERATE|SEVERE|UNKNOWN","reaction_display":"","is_negated":false,"evidence_text":"","extraction_confidence":0.9}]
}
If none found for a category, use empty array. Return ONLY the JSON, no other text.

Clinical note:
' || LEFT(raw_text, 6000))
            ) AS result_json
        FROM batch_docs;

        -- Insert medications
        INSERT INTO MEDICATION_REQUEST (
            medication_request_id, patient_id, medication_display, dosage_text, dose, dose_unit,
            route_display, frequency_text, status, intent, is_negated, temporality,
            evidence_text, extraction_confidence, provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-MED-' || f.index::VARCHAR),
            b.patient_id,
            f.value:medication_display::VARCHAR,
            f.value:dosage_text::VARCHAR,
            TRY_CAST(f.value:dose::VARCHAR AS NUMBER),
            f.value:dose_unit::VARCHAR,
            f.value:route_display::VARCHAR,
            f.value:frequency_text::VARCHAR,
            f.value:status::VARCHAR,
            f.value:intent::VARCHAR,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:temporality::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _THER_BATCH b, LATERAL FLATTEN(input => b.result_json:medications) f
        WHERE b.result_json IS NOT NULL AND f.value:medication_display::VARCHAR IS NOT NULL AND TRIM(f.value:medication_display::VARCHAR) != '';
        v_total_meds := v_total_meds + SQLROWCOUNT;

        -- Insert procedures
        INSERT INTO "PROCEDURE" (
            procedure_id, patient_id, encounter_id, display, category, status,
            body_site_display, laterality, is_negated, temporality,
            evidence_text, extraction_confidence, provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-PROC-' || f.index::VARCHAR),
            b.patient_id,
            'UNKNOWN',
            f.value:display::VARCHAR,
            f.value:category::VARCHAR,
            f.value:status::VARCHAR,
            f.value:body_site_display::VARCHAR,
            f.value:laterality::VARCHAR,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:temporality::VARCHAR,
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _THER_BATCH b, LATERAL FLATTEN(input => b.result_json:procedures) f
        WHERE b.result_json IS NOT NULL AND f.value:display::VARCHAR IS NOT NULL AND TRIM(f.value:display::VARCHAR) != '';
        v_total_procs := v_total_procs + SQLROWCOUNT;

        -- Insert allergies
        INSERT INTO ALLERGY_INTOLERANCE (
            allergy_id, patient_id, substance_display, criticality, severity,
            reaction_display, is_negated, evidence_text, extraction_confidence,
            provenance_document_id, source
        )
        SELECT
            MD5(b.document_id || '-ALLG-' || f.index::VARCHAR),
            b.patient_id,
            f.value:substance_display::VARCHAR,
            f.value:criticality::VARCHAR,
            f.value:severity::VARCHAR,
            f.value:reaction_display::VARCHAR,
            COALESCE(f.value:is_negated::BOOLEAN, FALSE),
            f.value:evidence_text::VARCHAR,
            f.value:extraction_confidence::NUMBER(5,4),
            b.document_id,
            'GENAI_NLP_NOTE'
        FROM _THER_BATCH b, LATERAL FLATTEN(input => b.result_json:allergies) f
        WHERE b.result_json IS NOT NULL AND f.value:substance_display::VARCHAR IS NOT NULL AND TRIM(f.value:substance_display::VARCHAR) != '';
        v_total_allergies := v_total_allergies + SQLROWCOUNT;

        INSERT INTO PIPELINE_RUN_LOG
        SELECT :v_run_id, 'EXTRACT_THERAPEUTICS_BATCH', 2, 'COMPLETED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
               :v_total_meds + :v_total_procs + :v_total_allergies, NULL, :v_batch_num, :BATCH_SIZE,
               OBJECT_CONSTRUCT('meds', :v_total_meds, 'procs', :v_total_procs, 'allergies', :v_total_allergies);

        v_batch_offset := v_batch_offset + :BATCH_SIZE;

        DROP TABLE IF EXISTS _THER_BATCH;
    END WHILE;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(),
        rows_inserted = :v_total_meds + :v_total_procs + :v_total_allergies
    WHERE run_id = :v_run_id AND step_name = 'EXTRACT_THERAPEUTICS';

    RETURN 'THERAPEUTICS: ' || v_total_meds || ' meds, ' || v_total_procs || ' procs, ' || v_total_allergies || ' allergies from ' || v_total_docs || ' docs';
END;
$$;
