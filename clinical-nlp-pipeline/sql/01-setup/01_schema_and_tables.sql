USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BI_WH;

CREATE DATABASE IF NOT EXISTS UNSTRUCTURED_HEALTHDATA;
CREATE SCHEMA IF NOT EXISTS UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

USE SCHEMA UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP;

-- ============================================================
-- PIPELINE PROGRESS TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    run_id VARCHAR NOT NULL,
    step_name VARCHAR NOT NULL,
    step_order INT NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'PENDING',
    started_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ,
    rows_processed INT DEFAULT 0,
    rows_inserted INT DEFAULT 0,
    error_message VARCHAR,
    batch_number INT,
    batch_size INT,
    metadata VARIANT
);

-- ============================================================
-- SOURCE DEDUP VIEW
-- ============================================================
CREATE OR REPLACE VIEW NOTE_DOCUMENT_SOURCE AS
SELECT
    RELATIVE_PATH,
    SIZE,
    FILE_URL,
    FULL_TEXT,
    PATIENT_FIRST_NAME,
    PATIENT_LAST_NAME,
    PATIENT_DOB,
    GENDER,
    ZIP_CODE,
    REPORT,
    SPECIALTY,
    PATIENT_NAME
FROM UNSTRUCTURED_HEALTHDATA.MED_TRANSCRIPTS.DOC_CATEGORIZED_AND_CHUNKED_2A
QUALIFY ROW_NUMBER() OVER (PARTITION BY RELATIVE_PATH ORDER BY SIZE DESC) = 1;

-- ============================================================
-- CONTEXT TABLES
-- ============================================================
CREATE TABLE IF NOT EXISTS NOTE_DOCUMENT (
    document_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    note_type VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    created_datetime TIMESTAMP_NTZ NOT NULL,
    author VARCHAR,
    raw_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS NOTE_SECTION (
    section_id VARCHAR NOT NULL PRIMARY KEY,
    document_id VARCHAR NOT NULL,
    section_name VARCHAR NOT NULL,
    start_offset INT NOT NULL,
    end_offset INT NOT NULL
);

-- ============================================================
-- CLINICAL ENTITY TABLES (10 tables)
-- ============================================================
CREATE TABLE IF NOT EXISTS CONDITION (
    condition_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    display VARCHAR NOT NULL,
    code VARCHAR,
    code_system VARCHAR,
    category VARCHAR,
    clinical_status VARCHAR,
    verification_status VARCHAR,
    severity_display VARCHAR,
    severity_code VARCHAR,
    body_site_display VARCHAR,
    body_site_code VARCHAR,
    laterality VARCHAR,
    onset_datetime TIMESTAMP_NTZ,
    abatement_datetime TIMESTAMP_NTZ,
    recorded_date DATE,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    temporality VARCHAR,
    certainty VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS MEDICATION_REQUEST (
    medication_request_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    medication_display VARCHAR NOT NULL,
    medication_code VARCHAR,
    medication_system VARCHAR,
    status VARCHAR,
    intent VARCHAR,
    dosage_text VARCHAR,
    dose NUMBER,
    dose_unit VARCHAR,
    route_display VARCHAR,
    route_code VARCHAR,
    frequency_text VARCHAR,
    frequency_norm VARCHAR,
    duration_value NUMBER,
    duration_unit VARCHAR,
    authored_on TIMESTAMP_NTZ,
    requester VARCHAR,
    indication_condition_id VARCHAR,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    temporality VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS "PROCEDURE" (
    procedure_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR NOT NULL,
    display VARCHAR NOT NULL,
    code VARCHAR,
    code_system VARCHAR,
    category VARCHAR,
    status VARCHAR,
    performed_start TIMESTAMP_NTZ,
    performed_end TIMESTAMP_NTZ,
    body_site_display VARCHAR,
    body_site_code VARCHAR,
    laterality VARCHAR,
    reason_condition_id VARCHAR,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    temporality VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS OBSERVATION (
    observation_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    display VARCHAR NOT NULL,
    code VARCHAR,
    code_system VARCHAR,
    category VARCHAR,
    status VARCHAR,
    effective_datetime TIMESTAMP_NTZ,
    value_quantity NUMBER,
    value_unit VARCHAR,
    value_string VARCHAR,
    value_code VARCHAR,
    value_code_system VARCHAR,
    value_display VARCHAR,
    interpretation VARCHAR,
    method VARCHAR,
    body_site_display VARCHAR,
    body_site_code VARCHAR,
    laterality VARCHAR,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    temporality VARCHAR,
    certainty VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ALLERGY_INTOLERANCE (
    allergy_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    substance_display VARCHAR NOT NULL,
    substance_code VARCHAR,
    substance_system VARCHAR,
    verification_status VARCHAR,
    criticality VARCHAR,
    severity VARCHAR,
    onset_datetime TIMESTAMP_NTZ,
    reaction_display VARCHAR,
    reaction_code VARCHAR,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ADVERSE_EVENT (
    adverse_event_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    event_display VARCHAR NOT NULL,
    event_code VARCHAR,
    event_system VARCHAR,
    seriousness VARCHAR,
    severity VARCHAR,
    outcome VARCHAR,
    onset_datetime TIMESTAMP_NTZ,
    resolution_datetime TIMESTAMP_NTZ,
    suspect_medication_request_id VARCHAR,
    suspect_device_id VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS SOCIAL_HISTORY_OBSERVATION (
    social_history_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    display VARCHAR NOT NULL,
    code VARCHAR,
    code_system VARCHAR,
    sdoh_domain VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'UNKNOWN',
    screening_instrument VARCHAR,
    value_quantity NUMBER,
    value_unit VARCHAR,
    value_string VARCHAR,
    effective_period_start TIMESTAMP_NTZ,
    effective_period_end TIMESTAMP_NTZ,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS FAMILY_MEMBER_HISTORY (
    family_history_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    relationship_code VARCHAR NOT NULL,
    relationship_display VARCHAR NOT NULL,
    condition_display VARCHAR NOT NULL,
    condition_code VARCHAR,
    condition_system VARCHAR,
    onset_age INT,
    deceased_flag BOOLEAN,
    is_negated BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS CARE_PLAN_ITEM (
    care_plan_item_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    encounter_id VARCHAR,
    item_type VARCHAR NOT NULL,
    description TEXT NOT NULL,
    status VARCHAR,
    due_datetime TIMESTAMP_NTZ,
    target_condition_id VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS TUMOR_EPISODE (
    tumor_episode_id VARCHAR NOT NULL PRIMARY KEY,
    patient_id VARCHAR NOT NULL,
    primary_condition_id VARCHAR NOT NULL,
    primary_site_display VARCHAR,
    primary_site_code VARCHAR,
    primary_site_system VARCHAR,
    histology_display VARCHAR,
    histology_code VARCHAR,
    histology_system VARCHAR,
    stage_group VARCHAR,
    tnm_t VARCHAR,
    tnm_n VARCHAR,
    tnm_m VARCHAR,
    grade VARCHAR,
    performance_status_scale VARCHAR,
    performance_status_value VARCHAR,
    response_status VARCHAR,
    certainty VARCHAR,
    evidence_text VARCHAR,
    extraction_confidence NUMBER(5,4),
    provenance_document_id VARCHAR,
    source VARCHAR NOT NULL
);
