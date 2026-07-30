-- =============================================================================
-- 05: Masking Policies (PHI Governance)
-- Protects Protected Health Information (PHI) using Snowflake's native
-- column-level masking. Only privileged roles see actual patient identifiers.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- Create the masking policy
CREATE OR REPLACE MASKING POLICY PHI_MASK AS (VAL VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN VAL
        ELSE '***MASKED***'
    END;

-- Apply masking policy to PHI columns
ALTER TABLE DICOM_CARDIAC_METADATA
    MODIFY COLUMN PATIENT_ID SET MASKING POLICY PHI_MASK;

ALTER TABLE DICOM_CARDIAC_METADATA
    MODIFY COLUMN PATIENT_NAME SET MASKING POLICY PHI_MASK;
