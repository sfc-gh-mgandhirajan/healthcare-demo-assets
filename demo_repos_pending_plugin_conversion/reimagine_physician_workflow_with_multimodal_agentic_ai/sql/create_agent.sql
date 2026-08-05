-- ============================================================
-- Step 5: Create the Cortex Agent
--
-- Run this AFTER:
--   1. sql/deploy_medgemma.sql  (compute pool, EAI, secrets, MedGemma on SPCS)
--   2. sql/setup_data.sql       (tables, data, MEDGEMMA_MEDICAL_INTERPRETER proc)
--   3. SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML  (HIMSS_PATIENT_SEMANTIC_VIEW)
--
-- All identifiers below are rewritten for your environment by:
--   python3 configure.py
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- The agent is created in the agent database/schema below (default
-- SNOWFLAKE_INTELLIGENCE.AGENTS) so Snowsight / CoWork can discover it.
-- Both are created if they don't already exist.
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- ============================================================
-- Agent definition
--
-- Two tools, matching the demo architecture:
--   PATIENT_ANALYST              -> Cortex Analyst over the semantic view (structured)
--   MEDGEMMA_MEDICAL_INTERPRETER -> stored procedure calling MedGemma 4B on SPCS (imaging)
--
-- Both tool_resources pin an execution_environment. This is REQUIRED: without a
-- pinned warehouse the agent fails with 399504 ("missing an execution
-- environment") whenever it is called by a client that has no default
-- warehouse, which includes any SPCS service or app using an owner's-rights
-- service role. query_timeout is capped at 600 seconds by Cortex; larger
-- values are rejected.
-- ============================================================

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HIMSS_PHYSICIAN_AGENT
  COMMENT = 'PhysicianAssist: routes clinical questions to structured patient data (Cortex Analyst) or medical image interpretation (MedGemma 4B on SPCS).'
  PROFILE = '{"display_name": "PhysicianAssist", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  instructions:
    response: |
      You support licensed clinicians reviewing their own patients. Be concise and
      clinical. Lead with the answer, then the supporting values. Always include
      units and the observation date for any vital, lab, or medication dose.
      Never speculate beyond the retrieved data -- if a value is absent, say so
      rather than estimating. Surface drug interactions and abnormal values
      explicitly instead of burying them in prose. You are decision support, not
      a diagnosis: do not state a definitive diagnosis or issue orders.
    orchestration: |
      Use PATIENT_ANALYST for anything answerable from the patient record:
      demographics, conditions, medications, vitals, encounters, and image
      metadata. Use MEDGEMMA_MEDICAL_INTERPRETER only to interpret the pixel
      content of a medical image (ECG, chest X-ray, echo, angiogram, MRI,
      Holter).

      When a question needs both -- for example "read this ECG in the context of
      his medications" -- call PATIENT_ANALYST first to resolve the image ID and
      the clinical context, then pass that context to
      MEDGEMMA_MEDICAL_INTERPRETER as P_QUESTION.

      To interpret an image, set P_MODE='image' and pass the IMAGE_ID from
      MEDICAL_IMAGES as P_IMAGE_ID. Use P_MODE='text' only for narrative
      clinical text with no image. Never invent an IMAGE_ID -- look it up first.
    sample_questions:
      - question: "What medications is patient P-1001 on, and are there any interactions?"
      - question: "Show me the vitals trend for P-1002 over the last 48 hours."
      - question: "Interpret the admission ECG for P-1001."
      - question: "Which of my patients had a STEMI this admission?"

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "PATIENT_ANALYST"
        description: |
          Queries the structured cardiology patient record: patients, conditions,
          medications, vitals, encounters, and medical image metadata (including
          IMAGE_ID). Use for any question about what is recorded for a patient,
          for cohort questions across patients, and to look up an IMAGE_ID before
          calling MEDGEMMA_MEDICAL_INTERPRETER. Do NOT use it to interpret the
          content of an image -- it reads metadata only, not pixels.
    - tool_spec:
        type: "generic"
        name: "MEDGEMMA_MEDICAL_INTERPRETER"
        description: |
          Interprets medical images using MedGemma 4B, a medical vision-language
          model running on Snowflake GPU compute (SPCS). Handles ECG, chest
          X-ray, echocardiogram, angiogram, cardiac MRI, and Holter images.
          Returns a JSON string with the interpretation. Use ONLY when the
          clinician wants the image itself read. Requires a valid IMAGE_ID from
          MEDICAL_IMAGES -- resolve it with PATIENT_ANALYST first.
        input_schema:
          type: "object"
          properties:
            P_MODE:
              type: "string"
              description: "'image' to interpret an image (requires P_IMAGE_ID), or 'text' to interpret clinical narrative text (requires P_TEXT). Defaults to 'text'."
            P_IMAGE_ID:
              type: "string"
              description: "IMAGE_ID from the MEDICAL_IMAGES table, e.g. 'IMG-1001-ECG-01'. Required when P_MODE='image'. Must be looked up via PATIENT_ANALYST -- never guessed."
            P_TEXT:
              type: "string"
              description: "Clinical narrative text to interpret. Used only when P_MODE='text'."
            P_QUESTION:
              type: "string"
              description: "The specific clinical question to answer about the image or text, including any relevant patient context (age, presentation, current medications)."
          required:
            - "P_MODE"

  tool_resources:
    PATIENT_ANALYST:
      semantic_view: "DEMO_DB.HIMSS_DEMO.HIMSS_PATIENT_SEMANTIC_VIEW"
      execution_environment:
        type: "warehouse"
        warehouse: "HIMSS_INTERACTIVE_WH"
        query_timeout: 600
    MEDGEMMA_MEDICAL_INTERPRETER:
      type: "procedure"
      identifier: "DEMO_DB.HIMSS_DEMO.MEDGEMMA_MEDICAL_INTERPRETER"
      execution_environment:
        type: "warehouse"
        warehouse: "HIMSS_INTERACTIVE_WH"
        query_timeout: 600
  $$;

-- ============================================================
-- Verify
-- ============================================================

DESCRIBE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HIMSS_PHYSICIAN_AGENT;

-- Optional smoke test without leaving SQL. Errors surface INSIDE the returned
-- JSON (code 399504) rather than as a thrown SQL error, so parse the response.
-- SELECT TRY_PARSE_JSON(
--          SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
--            'SNOWFLAKE_INTELLIGENCE.AGENTS.HIMSS_PHYSICIAN_AGENT',
--            {'messages': [{'role': 'user',
--                           'content': [{'type': 'text',
--                                        'text': 'What medications is patient P-1001 on?'}]}]}
--          )
--        ) AS response;

-- ============================================================
-- Grant access
--
-- The frontend authenticates with a PAT bound to a user whose role needs USAGE
-- on the agent, plus USAGE on the tools the agent calls. Replace <YOUR_ROLE>.
-- ============================================================

-- GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE <YOUR_ROLE>;
-- GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE <YOUR_ROLE>;
-- GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HIMSS_PHYSICIAN_AGENT TO ROLE <YOUR_ROLE>;
-- GRANT USAGE ON PROCEDURE DEMO_DB.HIMSS_DEMO.MEDGEMMA_MEDICAL_INTERPRETER(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE <YOUR_ROLE>;
