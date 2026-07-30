-- =============================================================================
-- 04: Tables
-- Core metadata table.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- -----------------------------------------------------------------------------
-- Table 1: DICOM_CARDIAC_METADATA
-- Stores parsed DICOM metadata from all 10 series (2,848 files).
-- Columns are extracted from the VARIANT output of EXTRACT_DICOM_METADATA.
-- PATIENT_ID and PATIENT_NAME have masking policies applied (see 05_masking_policies.sql).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DICOM_CARDIAC_METADATA (
    FILE_NAME           VARCHAR,
    FILE_SIZE           NUMBER(38,0),
    RAW_METADATA        VARIANT,
    PATIENT_ID          VARCHAR,
    PATIENT_NAME        VARCHAR,
    PATIENT_SEX         VARCHAR,
    PATIENT_AGE         VARCHAR,
    STUDY_INSTANCE_UID  VARCHAR,
    STUDY_DATE          VARCHAR,
    STUDY_TIME          VARCHAR,
    STUDY_DESCRIPTION   VARCHAR,
    SERIES_INSTANCE_UID VARCHAR,
    SERIES_NUMBER       VARCHAR,
    SERIES_DESCRIPTION  VARCHAR,
    SOP_INSTANCE_UID    VARCHAR,
    SOP_CLASS_UID       VARCHAR,
    MODALITY            VARCHAR,
    MANUFACTURER        VARCHAR,
    INSTITUTION_NAME    VARCHAR,
    BODY_PART_EXAMINED  VARCHAR,
    IMAGE_ROWS          NUMBER(38,0),
    IMAGE_COLUMNS       NUMBER(38,0),
    BITS_ALLOCATED      NUMBER(38,0),
    BITS_STORED         NUMBER(38,0),
    PIXEL_SPACING       VARCHAR,
    SLICE_THICKNESS     VARCHAR,
    SLICE_LOCATION      VARCHAR,
    IMAGE_POSITION_PATIENT      VARCHAR,
    IMAGE_ORIENTATION_PATIENT   VARCHAR,
    WINDOW_CENTER       VARCHAR,
    WINDOW_WIDTH        VARCHAR,
    INSTANCE_NUMBER     VARCHAR,
    ACQUISITION_NUMBER  VARCHAR,
    PHOTOMETRIC_INTERPRETATION  VARCHAR,
    KVP                 VARCHAR,
    EXPOSURE            VARCHAR,
    CONVOLUTION_KERNEL  VARCHAR,
    PROTOCOL_NAME       VARCHAR,
    TRANSFER_SYNTAX_UID VARCHAR,
    SERIES_LABEL        VARCHAR,
    COLLECTION_NAME     VARCHAR
);
