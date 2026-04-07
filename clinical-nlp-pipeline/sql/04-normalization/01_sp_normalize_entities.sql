USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

CREATE OR REPLACE PROCEDURE SP_NORMALIZE_ENTITIES(BATCH_SIZE INT DEFAULT 500)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_exact_conditions INT DEFAULT 0; v_fuzzy_conditions INT DEFAULT 0;
    v_exact_meds INT DEFAULT 0; v_fuzzy_meds INT DEFAULT 0;
    v_exact_obs INT DEFAULT 0; v_fuzzy_obs INT DEFAULT 0;
    v_exact_procs INT DEFAULT 0; v_fuzzy_procs INT DEFAULT 0;
    v_exact_tumors INT DEFAULT 0; v_fuzzy_tumors INT DEFAULT 0;
    v_exact_allergies INT DEFAULT 0; v_fuzzy_allergies INT DEFAULT 0;
BEGIN
    v_run_id := 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') || '-NORM';

    INSERT INTO PIPELINE_RUN_LOG
    SELECT :v_run_id, 'NORMALIZE_ALL', 7, 'RUNNING', CURRENT_TIMESTAMP(), NULL, NULL, 0, NULL, NULL, NULL,
           OBJECT_CONSTRUCT('code_systems', 'ALL');

    -- ================================================================
    -- STEP 1: EXACT MATCH — CONDITIONS → ICD-10-CM / SNOMED CT
    -- ================================================================
    UPDATE CONDITION c
    SET c.code = cd.code,
        c.code_system = CASE cs.name WHEN 'ICD-10-CM' THEN 'ICD10CM' WHEN 'SNOMED CT' THEN 'SNOMED_CT' ELSE cs.name END
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE c.code IS NULL
      AND c.source = 'GENAI_NLP_NOTE'
      AND LOWER(TRIM(c.display)) = LOWER(TRIM(cd.display))
      AND cs.name IN ('ICD-10-CM', 'SNOMED CT')
      AND cd.semantic_group IN ('DISEASE', 'SYMPTOM');
    v_exact_conditions := SQLROWCOUNT;

    -- ================================================================
    -- STEP 2: EXACT MATCH — MEDICATIONS → RxNorm
    -- ================================================================
    UPDATE MEDICATION_REQUEST m
    SET m.medication_code = cd.code,
        m.medication_system = 'RXNORM'
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE m.medication_code IS NULL
      AND m.source = 'GENAI_NLP_NOTE'
      AND LOWER(TRIM(m.medication_display)) = LOWER(TRIM(cd.display))
      AND cs.name = 'RxNorm'
      AND cd.semantic_group = 'MEDICATION';
    v_exact_meds := SQLROWCOUNT;

    -- ================================================================
    -- STEP 3: EXACT MATCH — OBSERVATIONS → LOINC / SNOMED CT
    -- ================================================================
    UPDATE OBSERVATION o
    SET o.code = cd.code,
        o.code_system = CASE cs.name WHEN 'LOINC' THEN 'LOINC' WHEN 'SNOMED CT' THEN 'SNOMED_CT' ELSE cs.name END
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE o.code IS NULL
      AND o.source = 'GENAI_NLP_NOTE'
      AND LOWER(TRIM(o.display)) = LOWER(TRIM(cd.display))
      AND cs.name IN ('LOINC', 'SNOMED CT')
      AND cd.semantic_group IN ('LAB', 'SCORE', 'DISEASE', 'SYMPTOM');
    v_exact_obs := SQLROWCOUNT;

    -- ================================================================
    -- STEP 4: EXACT MATCH — PROCEDURES → SNOMED CT / HCPCS / ICD-10-PCS
    -- ================================================================
    UPDATE "PROCEDURE" p
    SET p.code = cd.code,
        p.code_system = CASE cs.name WHEN 'SNOMED CT' THEN 'SNOMED_CT' WHEN 'ICD-10-PCS' THEN 'ICD10PCS' ELSE cs.name END
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE p.code IS NULL
      AND p.source = 'GENAI_NLP_NOTE'
      AND LOWER(TRIM(p.display)) = LOWER(TRIM(cd.display))
      AND cd.semantic_group = 'PROCEDURE';
    v_exact_procs := SQLROWCOUNT;

    -- ================================================================
    -- STEP 5: EXACT MATCH — TUMORS → ICD-O-3
    -- ================================================================
    UPDATE TUMOR_EPISODE t
    SET t.primary_site_code = cd.code,
        t.primary_site_system = 'ICD-O-3'
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE t.primary_site_code IS NULL
      AND t.source = 'GENAI_NLP_NOTE'
      AND (LOWER(TRIM(t.primary_site_display)) = LOWER(TRIM(cd.display))
           OR LOWER(TRIM(t.histology_display)) = LOWER(TRIM(cd.display)))
      AND cd.semantic_group = 'TUMOR';
    v_exact_tumors := SQLROWCOUNT;

    -- ================================================================
    -- STEP 6: EXACT MATCH — ALLERGIES → RxNorm
    -- ================================================================
    UPDATE ALLERGY_INTOLERANCE a
    SET a.substance_code = cd.code,
        a.substance_system = 'RXNORM'
    FROM UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION cd
    JOIN UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CODE_SYSTEM cs ON cd.code_system_id = cs.code_system_id
    WHERE a.substance_code IS NULL
      AND a.source = 'GENAI_NLP_NOTE'
      AND LOWER(TRIM(a.substance_display)) = LOWER(TRIM(cd.display))
      AND cs.name = 'RxNorm'
      AND cd.semantic_group = 'MEDICATION';
    v_exact_allergies := SQLROWCOUNT;

    -- ================================================================
    -- STEP 7: DETERMINISTIC VITAL SIGNS → LOINC
    -- ================================================================
    UPDATE OBSERVATION
    SET code = CASE LOWER(TRIM(display))
            WHEN 'blood pressure' THEN '85354-9'
            WHEN 'systolic blood pressure' THEN '8480-6'
            WHEN 'diastolic blood pressure' THEN '8462-4'
            WHEN 'heart rate' THEN '8867-4'
            WHEN 'pulse' THEN '8867-4'
            WHEN 'respiratory rate' THEN '9279-1'
            WHEN 'temperature' THEN '8310-5'
            WHEN 'body temperature' THEN '8310-5'
            WHEN 'oxygen saturation' THEN '2708-6'
            WHEN 'spo2' THEN '2708-6'
            WHEN 'body weight' THEN '29463-7'
            WHEN 'weight' THEN '29463-7'
            WHEN 'body height' THEN '8302-2'
            WHEN 'height' THEN '8302-2'
            WHEN 'bmi' THEN '39156-5'
            WHEN 'body mass index' THEN '39156-5'
            ELSE code
        END,
        code_system = CASE WHEN LOWER(TRIM(display)) IN (
            'blood pressure','systolic blood pressure','diastolic blood pressure',
            'heart rate','pulse','respiratory rate','temperature','body temperature',
            'oxygen saturation','spo2','body weight','weight','body height','height',
            'bmi','body mass index'
        ) THEN 'LOINC' ELSE code_system END
    WHERE code IS NULL
      AND source = 'GENAI_NLP_NOTE'
      AND category = 'VITAL_SIGNS';

    -- ================================================================
    -- STEP 8: LLM-ASSISTED FUZZY MATCH — CONDITIONS → ICD-10-CM
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_CONDITIONS AS
    SELECT condition_id, display, category, severity_display, body_site_display, laterality, certainty, evidence_text
    FROM CONDITION
    WHERE code IS NULL AND source = 'GENAI_NLP_NOTE'
    LIMIT :BATCH_SIZE;

    UPDATE CONDITION c
    SET c.code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical condition to the most specific ICD-10-CM code. Return ONLY JSON: {"code":"X00.0","display":"exact ICD-10-CM description"}. If uncertain return {"code":null,"display":null}.
Condition: ' || u.display ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.laterality IS NOT NULL THEN ' | Laterality: ' || u.laterality ELSE '' END ||
                CASE WHEN u.severity_display IS NOT NULL THEN ' | Severity: ' || u.severity_display ELSE '' END
            )
        ):code::VARCHAR,
        c.code_system = 'ICD10CM'
    FROM _UNMATCHED_CONDITIONS u
    WHERE c.condition_id = u.condition_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical condition to the most specific ICD-10-CM code. Return ONLY JSON: {"code":"X00.0","display":"exact ICD-10-CM description"}. If uncertain return {"code":null,"display":null}.
Condition: ' || u.display ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.laterality IS NOT NULL THEN ' | Laterality: ' || u.laterality ELSE '' END ||
                CASE WHEN u.severity_display IS NOT NULL THEN ' | Severity: ' || u.severity_display ELSE '' END
            )
        ):code IS NOT NULL;
    v_fuzzy_conditions := SQLROWCOUNT;

    -- ================================================================
    -- STEP 9: LLM-ASSISTED FUZZY MATCH — MEDICATIONS → RxNorm
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_MEDS AS
    SELECT medication_request_id, medication_display, dosage_text, dose, dose_unit, route_display, evidence_text
    FROM MEDICATION_REQUEST
    WHERE medication_code IS NULL AND source = 'GENAI_NLP_NOTE'
    LIMIT :BATCH_SIZE;

    UPDATE MEDICATION_REQUEST m
    SET m.medication_code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this medication to the best RxNorm code. Target SCD (Semantic Clinical Drug) when dose+route known, IN (Ingredient) when only drug name. Return ONLY JSON: {"code":"123456","display":"RxNorm description"}. If uncertain return {"code":null,"display":null}.
Medication: ' || u.medication_display ||
                CASE WHEN u.dosage_text IS NOT NULL THEN ' | Dosage: ' || u.dosage_text ELSE '' END ||
                CASE WHEN u.route_display IS NOT NULL THEN ' | Route: ' || u.route_display ELSE '' END
            )
        ):code::VARCHAR,
        m.medication_system = 'RXNORM'
    FROM _UNMATCHED_MEDS u
    WHERE m.medication_request_id = u.medication_request_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this medication to the best RxNorm code. Target SCD (Semantic Clinical Drug) when dose+route known, IN (Ingredient) when only drug name. Return ONLY JSON: {"code":"123456","display":"RxNorm description"}. If uncertain return {"code":null,"display":null}.
Medication: ' || u.medication_display ||
                CASE WHEN u.dosage_text IS NOT NULL THEN ' | Dosage: ' || u.dosage_text ELSE '' END ||
                CASE WHEN u.route_display IS NOT NULL THEN ' | Route: ' || u.route_display ELSE '' END
            )
        ):code IS NOT NULL;
    v_fuzzy_meds := SQLROWCOUNT;

    -- ================================================================
    -- STEP 10: LLM-ASSISTED FUZZY MATCH — OBSERVATIONS → LOINC
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_OBS AS
    SELECT observation_id, display, category, value_quantity, value_unit, method, body_site_display, evidence_text
    FROM OBSERVATION
    WHERE code IS NULL AND source = 'GENAI_NLP_NOTE' AND category != 'VITAL_SIGNS'
    LIMIT :BATCH_SIZE;

    UPDATE OBSERVATION o
    SET o.code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical observation to the best LOINC code. Consider the 6 LOINC axes: component, property, timing, system, scale, method. Return ONLY JSON: {"code":"12345-6","display":"LOINC description"}. If uncertain return {"code":null,"display":null}.
Observation: ' || u.display ||
                CASE WHEN u.category IS NOT NULL THEN ' | Category: ' || u.category ELSE '' END ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.method IS NOT NULL THEN ' | Method: ' || u.method ELSE '' END
            )
        ):code::VARCHAR,
        o.code_system = 'LOINC'
    FROM _UNMATCHED_OBS u
    WHERE o.observation_id = u.observation_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical observation to the best LOINC code. Consider the 6 LOINC axes: component, property, timing, system, scale, method. Return ONLY JSON: {"code":"12345-6","display":"LOINC description"}. If uncertain return {"code":null,"display":null}.
Observation: ' || u.display ||
                CASE WHEN u.category IS NOT NULL THEN ' | Category: ' || u.category ELSE '' END ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.method IS NOT NULL THEN ' | Method: ' || u.method ELSE '' END
            )
        ):code IS NOT NULL;
    v_fuzzy_obs := SQLROWCOUNT;

    -- ================================================================
    -- STEP 11: LLM-ASSISTED FUZZY MATCH — PROCEDURES → ICD-10-PCS / CPT
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_PROCS AS
    SELECT procedure_id, display, category, body_site_display, laterality
    FROM "PROCEDURE"
    WHERE code IS NULL AND source = 'GENAI_NLP_NOTE'
    LIMIT :BATCH_SIZE;

    UPDATE "PROCEDURE" p
    SET p.code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical procedure to the most specific ICD-10-PCS or CPT code. Prefer CPT for common procedures and ICD-10-PCS for surgical procedures. Return ONLY JSON: {"code":"0DBJ4ZZ","system":"ICD10PCS","display":"name"} or {"code":"99213","system":"CPT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Procedure: ' || u.display ||
                CASE WHEN u.category IS NOT NULL THEN ' | Category: ' || u.category ELSE '' END ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.laterality IS NOT NULL THEN ' | Laterality: ' || u.laterality ELSE '' END
            )
        ):code::VARCHAR,
        p.code_system = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical procedure to the most specific ICD-10-PCS or CPT code. Prefer CPT for common procedures and ICD-10-PCS for surgical procedures. Return ONLY JSON: {"code":"0DBJ4ZZ","system":"ICD10PCS","display":"name"} or {"code":"99213","system":"CPT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Procedure: ' || u.display ||
                CASE WHEN u.category IS NOT NULL THEN ' | Category: ' || u.category ELSE '' END ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.laterality IS NOT NULL THEN ' | Laterality: ' || u.laterality ELSE '' END
            )
        ):system::VARCHAR
    FROM _UNMATCHED_PROCS u
    WHERE p.procedure_id = u.procedure_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this clinical procedure to the most specific ICD-10-PCS or CPT code. Prefer CPT for common procedures and ICD-10-PCS for surgical procedures. Return ONLY JSON: {"code":"0DBJ4ZZ","system":"ICD10PCS","display":"name"} or {"code":"99213","system":"CPT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Procedure: ' || u.display ||
                CASE WHEN u.category IS NOT NULL THEN ' | Category: ' || u.category ELSE '' END ||
                CASE WHEN u.body_site_display IS NOT NULL THEN ' | Site: ' || u.body_site_display ELSE '' END ||
                CASE WHEN u.laterality IS NOT NULL THEN ' | Laterality: ' || u.laterality ELSE '' END
            )
        ):code IS NOT NULL;
    v_fuzzy_procs := SQLROWCOUNT;

    -- ================================================================
    -- STEP 12: LLM-ASSISTED FUZZY MATCH — TUMORS → ICD-O-3
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_TUMORS AS
    SELECT tumor_episode_id, primary_site_display, histology_display, stage_group
    FROM TUMOR_EPISODE
    WHERE primary_site_code IS NULL AND source = 'GENAI_NLP_NOTE'
    LIMIT :BATCH_SIZE;

    UPDATE TUMOR_EPISODE t
    SET t.primary_site_code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this tumor to the most specific ICD-O-3 topography code (C code). Return ONLY JSON: {"site_code":"C50.9","histology_code":"8500/3"}. If no match for a field use null.
Primary site: ' || COALESCE(u.primary_site_display, 'unknown') ||
                ' | Histology: ' || COALESCE(u.histology_display, 'unknown') ||
                CASE WHEN u.stage_group IS NOT NULL THEN ' | Stage: ' || u.stage_group ELSE '' END
            )
        ):site_code::VARCHAR,
        t.primary_site_system = 'ICD-O-3',
        t.histology_code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this tumor to the most specific ICD-O-3 topography code (C code). Return ONLY JSON: {"site_code":"C50.9","histology_code":"8500/3"}. If no match for a field use null.
Primary site: ' || COALESCE(u.primary_site_display, 'unknown') ||
                ' | Histology: ' || COALESCE(u.histology_display, 'unknown') ||
                CASE WHEN u.stage_group IS NOT NULL THEN ' | Stage: ' || u.stage_group ELSE '' END
            )
        ):histology_code::VARCHAR,
        t.histology_system = 'ICD-O-3'
    FROM _UNMATCHED_TUMORS u
    WHERE t.tumor_episode_id = u.tumor_episode_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this tumor to the most specific ICD-O-3 topography code (C code). Return ONLY JSON: {"site_code":"C50.9","histology_code":"8500/3"}. If no match for a field use null.
Primary site: ' || COALESCE(u.primary_site_display, 'unknown') ||
                ' | Histology: ' || COALESCE(u.histology_display, 'unknown') ||
                CASE WHEN u.stage_group IS NOT NULL THEN ' | Stage: ' || u.stage_group ELSE '' END
            )
        ):site_code IS NOT NULL;
    v_fuzzy_tumors := SQLROWCOUNT;

    -- ================================================================
    -- STEP 13: LLM-ASSISTED FUZZY MATCH — ALLERGIES → RxNorm / SNOMED
    -- ================================================================
    CREATE OR REPLACE TEMPORARY TABLE _UNMATCHED_ALLERGIES AS
    SELECT allergy_id, substance_display, reaction_display
    FROM ALLERGY_INTOLERANCE
    WHERE substance_code IS NULL AND source = 'GENAI_NLP_NOTE'
      AND substance_display IS NOT NULL
      AND TRIM(substance_display) NOT IN ('', 'NONE', 'NKDA', 'No known drug allergies', 'No Known Drug Allergies', 'none', 'no known allergies')
    LIMIT :BATCH_SIZE;

    UPDATE ALLERGY_INTOLERANCE a
    SET a.substance_code = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this allergy substance to the most specific RxNorm or SNOMED CT code. Return ONLY JSON: {"code":"12345","system":"RXNORM","display":"name"} or {"code":"12345","system":"SNOMED_CT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Substance: ' || u.substance_display ||
                CASE WHEN u.reaction_display IS NOT NULL THEN ' | Reaction: ' || u.reaction_display ELSE '' END
            )
        ):code::VARCHAR,
        a.substance_system = TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this allergy substance to the most specific RxNorm or SNOMED CT code. Return ONLY JSON: {"code":"12345","system":"RXNORM","display":"name"} or {"code":"12345","system":"SNOMED_CT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Substance: ' || u.substance_display ||
                CASE WHEN u.reaction_display IS NOT NULL THEN ' | Reaction: ' || u.reaction_display ELSE '' END
            )
        ):system::VARCHAR
    FROM _UNMATCHED_ALLERGIES u
    WHERE a.allergy_id = u.allergy_id
      AND TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b',
                'Map this allergy substance to the most specific RxNorm or SNOMED CT code. Return ONLY JSON: {"code":"12345","system":"RXNORM","display":"name"} or {"code":"12345","system":"SNOMED_CT","display":"name"}. If no match return {"code":null,"system":null,"display":null}.
Substance: ' || u.substance_display ||
                CASE WHEN u.reaction_display IS NOT NULL THEN ' | Reaction: ' || u.reaction_display ELSE '' END
            )
        ):code IS NOT NULL;
    v_fuzzy_allergies := SQLROWCOUNT;

    DROP TABLE IF EXISTS _UNMATCHED_CONDITIONS;
    DROP TABLE IF EXISTS _UNMATCHED_MEDS;
    DROP TABLE IF EXISTS _UNMATCHED_OBS;
    DROP TABLE IF EXISTS _UNMATCHED_PROCS;
    DROP TABLE IF EXISTS _UNMATCHED_TUMORS;
    DROP TABLE IF EXISTS _UNMATCHED_ALLERGIES;

    UPDATE PIPELINE_RUN_LOG SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(),
        rows_inserted = :v_exact_conditions + :v_fuzzy_conditions + :v_exact_meds + :v_fuzzy_meds + :v_exact_obs + :v_fuzzy_obs + :v_exact_procs + :v_fuzzy_procs + :v_exact_tumors + :v_fuzzy_tumors + :v_exact_allergies + :v_fuzzy_allergies,
        metadata = OBJECT_CONSTRUCT(
            'exact_conditions', :v_exact_conditions, 'fuzzy_conditions', :v_fuzzy_conditions,
            'exact_meds', :v_exact_meds, 'fuzzy_meds', :v_fuzzy_meds,
            'exact_obs', :v_exact_obs, 'fuzzy_obs', :v_fuzzy_obs,
            'exact_procs', :v_exact_procs, 'fuzzy_procs', :v_fuzzy_procs,
            'exact_tumors', :v_exact_tumors, 'fuzzy_tumors', :v_fuzzy_tumors,
            'exact_allergies', :v_exact_allergies, 'fuzzy_allergies', :v_fuzzy_allergies
        )
    WHERE run_id = :v_run_id AND step_name = 'NORMALIZE_ALL';

    RETURN 'NORMALIZATION: Cond(' || v_exact_conditions || ' exact, ' || v_fuzzy_conditions || ' fuzzy) | Meds(' || v_exact_meds || ' exact, ' || v_fuzzy_meds || ' fuzzy) | Obs(' || v_exact_obs || ' exact, ' || v_fuzzy_obs || ' fuzzy) | Procs(' || v_exact_procs || ' exact, ' || v_fuzzy_procs || ' fuzzy) | Tumors(' || v_exact_tumors || ' exact, ' || v_fuzzy_tumors || ' fuzzy) | Allergies(' || v_exact_allergies || ' exact, ' || v_fuzzy_allergies || ' fuzzy)';
END;
$$;
