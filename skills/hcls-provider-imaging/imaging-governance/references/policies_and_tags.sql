-- =============================================================================
-- Imaging Governance: Tags, Masking Policies, Row Access, Aggregation
-- Database: {database}.{schema}
-- Functionally tested against POLARIS1 (2026-04-13)
-- =============================================================================

-- ===== TAGS =====

CREATE TAG IF NOT EXISTS {database}.{schema}.PHI_TYPE
    ALLOWED_VALUES 'NAME','MRN','DOB','PHYSICIAN','DEVICE_ID','ACCESSION','FREE_TEXT','FILE_PATH','UNIQUE_ID','VARIANT_PHI'
    COMMENT = 'HIPAA PHI identifier type for DICOM imaging columns';

CREATE TAG IF NOT EXISTS {database}.{schema}.SENSITIVITY_LEVEL
    ALLOWED_VALUES 'HIGH', 'MEDIUM', 'LOW'
    COMMENT = 'Data sensitivity classification level';

CREATE TAG IF NOT EXISTS {database}.{schema}.HIPAA_SAFE_HARBOR
    ALLOWED_VALUES 'DIRECT_IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE_ATTRIBUTE'
    COMMENT = 'HIPAA Safe Harbor de-identification category';

-- ===== TAG APPLICATION =====

ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_name
    SET TAG {database}.{schema}.PHI_TYPE = 'NAME',
        {database}.{schema}.SENSITIVITY_LEVEL = 'HIGH',
        {database}.{schema}.HIPAA_SAFE_HARBOR = 'DIRECT_IDENTIFIER';
ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_id
    SET TAG {database}.{schema}.PHI_TYPE = 'MRN',
        {database}.{schema}.SENSITIVITY_LEVEL = 'HIGH',
        {database}.{schema}.HIPAA_SAFE_HARBOR = 'DIRECT_IDENTIFIER';
ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_birth_date
    SET TAG {database}.{schema}.PHI_TYPE = 'DOB',
        {database}.{schema}.SENSITIVITY_LEVEL = 'HIGH',
        {database}.{schema}.HIPAA_SAFE_HARBOR = 'DIRECT_IDENTIFIER';

-- Remaining PHI columns: apply same pattern for
-- DICOM_STUDY: referring_physician (PHYSICIAN/HIGH), accession_number (ACCESSION/HIGH)
-- DICOM_EQUIPMENT: device_serial_number (DEVICE_ID/MEDIUM/QUASI), station_name (DEVICE_ID/MEDIUM/QUASI)
-- DICOM_PROCEDURE_STEP: performing_physician (PHYSICIAN/HIGH)
-- DICOM_FILE_LOCATION: storage_uri (FILE_PATH/MEDIUM/QUASI)
-- DICOM_RAW: metadata (VARIANT_PHI/HIGH)
-- RADIOLOGY_REPORTS: patient_id (MRN/HIGH), radiologist_name (PHYSICIAN/HIGH), report_text (FREE_TEXT/HIGH/SENSITIVE_ATTRIBUTE)

-- ===== MASKING POLICIES =====

CREATE OR REPLACE MASKING POLICY {database}.{schema}.PHI_STRING_MASK
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
        WHEN IS_ROLE_IN_SESSION('IMAGING_ADMIN') THEN val
        ELSE '***MASKED***'
    END;

CREATE OR REPLACE MASKING POLICY {database}.{schema}.PHI_DATE_MASK
    AS (val DATE) RETURNS DATE ->
    CASE
        WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
        WHEN IS_ROLE_IN_SESSION('IMAGING_ADMIN') THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
    END;

CREATE OR REPLACE MASKING POLICY {database}.{schema}.PHI_NUMBER_MASK
    AS (val NUMBER) RETURNS NUMBER ->
    CASE
        WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
        WHEN IS_ROLE_IN_SESSION('IMAGING_ADMIN') THEN val
        ELSE 0
    END;

CREATE OR REPLACE MASKING POLICY {database}.{schema}.PHI_VARIANT_MASK
    AS (val VARIANT) RETURNS VARIANT ->
    CASE
        WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN val
        WHEN IS_ROLE_IN_SESSION('IMAGING_ADMIN') THEN val
        ELSE OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(
             OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(OBJECT_DELETE(val,
             'PatientName'),'PatientID'),'PatientBirthDate'),'ReferringPhysicianName'),
             'InstitutionName'),'InstitutionAddress'),'PerformingPhysicianName'),
             'OperatorsName'),'PatientAddress'),'PatientTelephoneNumbers'),
             'DeviceSerialNumber'),'StationName')
    END;

-- ===== MASKING POLICY APPLICATION =====

ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_name SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_id SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_PATIENT MODIFY COLUMN patient_birth_date SET MASKING POLICY {database}.{schema}.PHI_DATE_MASK;
ALTER TABLE {database}.{schema}.DICOM_STUDY MODIFY COLUMN referring_physician SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_STUDY MODIFY COLUMN accession_number SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_EQUIPMENT MODIFY COLUMN device_serial_number SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_EQUIPMENT MODIFY COLUMN station_name SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_PROCEDURE_STEP MODIFY COLUMN performing_physician SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_FILE_LOCATION MODIFY COLUMN storage_uri SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.DICOM_RAW MODIFY COLUMN metadata SET MASKING POLICY {database}.{schema}.PHI_VARIANT_MASK;
ALTER TABLE {database}.{schema}.RADIOLOGY_REPORTS MODIFY COLUMN patient_id SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.RADIOLOGY_REPORTS MODIFY COLUMN radiologist_name SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;
ALTER TABLE {database}.{schema}.RADIOLOGY_REPORTS MODIFY COLUMN report_text SET MASKING POLICY {database}.{schema}.PHI_STRING_MASK;

-- ===== ROW ACCESS POLICIES =====

CREATE OR REPLACE ROW ACCESS POLICY {database}.{schema}.IMAGING_INSTITUTION_RAP
    AS (institution_val VARCHAR) RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('IMAGING_ADMIN') OR IS_ROLE_IN_SESSION('PHI_AUTHORIZED')
    OR institution_val IN (
        SELECT institution FROM {database}.{schema}.ROLE_INSTITUTION_MAPPING
        WHERE IS_ROLE_IN_SESSION(role_name));

CREATE OR REPLACE ROW ACCESS POLICY {database}.{schema}.IMAGING_DEPARTMENT_RAP
    AS (department_val VARCHAR) RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('IMAGING_ADMIN')
    OR department_val IN (
        SELECT department FROM {database}.{schema}.ROLE_DEPARTMENT_MAPPING
        WHERE IS_ROLE_IN_SESSION(role_name));

-- Consent-based RAP for research
CREATE TABLE IF NOT EXISTS {database}.{schema}.PATIENT_CONSENT_REGISTRY (
    patient_id STRING, consent_type STRING, consent_granted BOOLEAN,
    consent_date DATE, expiration_date DATE);

CREATE OR REPLACE ROW ACCESS POLICY {database}.{schema}.IMAGING_CONSENT_RAP
    AS (patient_id_val VARCHAR) RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('IMAGING_ADMIN') OR IS_ROLE_IN_SESSION('PHI_AUTHORIZED')
    OR (IS_ROLE_IN_SESSION('RESEARCH_ANALYST') AND EXISTS (
        SELECT 1 FROM {database}.{schema}.PATIENT_CONSENT_REGISTRY
        WHERE patient_id = patient_id_val AND consent_type = 'RESEARCH'
        AND consent_granted = TRUE
        AND (expiration_date IS NULL OR expiration_date >= CURRENT_DATE())));

-- ===== AGGREGATION POLICY =====

CREATE OR REPLACE AGGREGATION POLICY {database}.{schema}.IMAGING_MIN_CELL_SIZE
    AS () RETURNS AGGREGATION_CONSTRAINT ->
    CASE
        WHEN IS_ROLE_IN_SESSION('IMAGING_ADMIN') THEN NO_AGGREGATION_CONSTRAINT()
        WHEN IS_ROLE_IN_SESSION('PHI_AUTHORIZED') THEN NO_AGGREGATION_CONSTRAINT()
        ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)
    END;

ALTER TABLE {database}.{schema}.DICOM_STUDY SET AGGREGATION POLICY {database}.{schema}.IMAGING_MIN_CELL_SIZE;
ALTER TABLE {database}.{schema}.DICOM_PATIENT SET AGGREGATION POLICY {database}.{schema}.IMAGING_MIN_CELL_SIZE;
ALTER TABLE {database}.{schema}.RADIOLOGY_REPORTS SET AGGREGATION POLICY {database}.{schema}.IMAGING_MIN_CELL_SIZE;
