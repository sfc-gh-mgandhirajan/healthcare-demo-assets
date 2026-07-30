-- =============================================================================
-- 07: Data Load Pipeline
-- Extracts DICOM metadata from binary files on the external stage using
-- the EXTRACT_DICOM_METADATA Python UDF. Demonstrates how Snowflake processes
-- unstructured medical imaging data at scale with zero infrastructure.
--
-- IMPORTANT: Run 01-06 first. Stage directory refresh (02_stages.sql) must
-- complete before this script will find files.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- Series-to-collection mapping reference:
-- UUID                                   | Collection             | Label                                        | Expected Files
-- 7e1bcefd-d097-47c0-9d54-65bd5d9674cf  | nsclc_radiogenomics    | Cardiac CTA Diastolic 70% AMC-015            | 372
-- 3b5970f6-f130-4337-9666-a9741a267268  | varepop_apollo         | Cardiac CTA Systolic 31% AP-26JK             | 459
-- 4bed8f9a-bf84-44da-ac8d-d39e15d9c978  | nsclc_radiogenomics    | Cardiac CT Systolic 38% AMC-027              | 336
-- f53b78ea-92ea-479e-a89f-967bec74e848  | rider_lung_pet_ct      | Gated Segment 0.625mm RIDER-2019259100       | 570
-- 1d92626a-e39d-4cf5-8ed9-864aae909d3d  | rider_lung_pet_ct      | Cine 30-65 BPM RIDER-2266952716              | 280
-- 7f4806f3-5946-41cb-8684-8fd5d51f8931  | covid_19_ny_sbu        | Cardiac 2.5 B41s A130302                     | 85
-- 6dfa81e0-3520-4332-aecc-0767dd1cab74  | covid_19_ny_sbu        | CTA 3.0 CE A095019                           | 130
-- f6fc9ace-5516-418d-b979-7ffaf14de22b  | nlst                   | Chest CT Philips 202207                      | 258
-- bb721c76-8056-4780-97be-1bc61b469454  | nlst                   | Chest CT Siemens B30f 133417                 | 198
-- de9868a4-a9d9-4652-9f0f-04e06a95d09e  | nlst                   | Chest CT Toshiba FC10 200119                 | 160

-- Truncate table before full reload (idempotent)
TRUNCATE TABLE IF EXISTS DICOM_CARDIAC_METADATA;

-- Load all 10 series from the all-data stage.
-- The Python UDF reads each binary .dcm file, parses DICOM tags, and returns VARIANT JSON.
-- A single INSERT...SELECT processes all 2,848 files in parallel across warehouse nodes.
INSERT INTO DICOM_CARDIAC_METADATA
WITH uuid_map AS (
    SELECT column1 AS UUID, column2 AS COLLECTION, column3 AS LABEL FROM VALUES
        ('7e1bcefd-d097-47c0-9d54-65bd5d9674cf', 'nsclc_radiogenomics', 'Cardiac CTA Diastolic 70% AMC-015'),
        ('3b5970f6-f130-4337-9666-a9741a267268', 'varepop_apollo',      'Cardiac CTA Systolic 31% AP-26JK'),
        ('4bed8f9a-bf84-44da-ac8d-d39e15d9c978', 'nsclc_radiogenomics', 'Cardiac CT Systolic 38% AMC-027'),
        ('f53b78ea-92ea-479e-a89f-967bec74e848', 'rider_lung_pet_ct',   'Gated Segment 0.625mm RIDER-2019259100'),
        ('1d92626a-e39d-4cf5-8ed9-864aae909d3d', 'rider_lung_pet_ct',   'Cine 30-65 BPM RIDER-2266952716'),
        ('7f4806f3-5946-41cb-8684-8fd5d51f8931', 'covid_19_ny_sbu',     'Cardiac 2.5 B41s A130302'),
        ('6dfa81e0-3520-4332-aecc-0767dd1cab74', 'covid_19_ny_sbu',     'CTA 3.0 CE A095019'),
        ('f6fc9ace-5516-418d-b979-7ffaf14de22b', 'nlst',                'Chest CT Philips 202207'),
        ('bb721c76-8056-4780-97be-1bc61b469454', 'nlst',                'Chest CT Siemens B30f 133417'),
        ('de9868a4-a9d9-4652-9f0f-04e06a95d09e', 'nlst',                'Chest CT Toshiba FC10 200119')
),
raw_files AS (
    SELECT
        RELATIVE_PATH AS FILE_NAME,
        SIZE AS FILE_SIZE,
        SPLIT_PART(RELATIVE_PATH, '/', 1) AS SERIES_UUID
    FROM DIRECTORY(@IDC_OPEN_DATA_ALL_STG)
    WHERE RELATIVE_PATH LIKE '%.dcm'
),
extracted AS (
    SELECT
        f.FILE_NAME,
        f.FILE_SIZE,
        f.SERIES_UUID,
        EXTRACT_DICOM_METADATA(
            BUILD_SCOPED_FILE_URL(@IDC_OPEN_DATA_ALL_STG, f.FILE_NAME)
        ) AS META
    FROM raw_files f
)
SELECT
    e.FILE_NAME,
    e.FILE_SIZE,
    e.META AS RAW_METADATA,
    e.META:PATIENTID::VARCHAR                   AS PATIENT_ID,
    e.META:PATIENTNAME::VARCHAR                 AS PATIENT_NAME,
    e.META:PATIENTSEX::VARCHAR                  AS PATIENT_SEX,
    e.META:PATIENTAGE::VARCHAR                  AS PATIENT_AGE,
    e.META:STUDYINSTANCEUID::VARCHAR            AS STUDY_INSTANCE_UID,
    e.META:STUDYDATE::VARCHAR                   AS STUDY_DATE,
    e.META:STUDYTIME::VARCHAR                   AS STUDY_TIME,
    e.META:STUDYDESCRIPTION::VARCHAR            AS STUDY_DESCRIPTION,
    e.META:SERIESINSTANCEUID::VARCHAR           AS SERIES_INSTANCE_UID,
    e.META:SERIESNUMBER::VARCHAR                AS SERIES_NUMBER,
    e.META:SERIESDESCRIPTION::VARCHAR           AS SERIES_DESCRIPTION,
    e.META:SOPINSTANCEUID::VARCHAR              AS SOP_INSTANCE_UID,
    e.META:SOPCLASSUID::VARCHAR                 AS SOP_CLASS_UID,
    e.META:MODALITY::VARCHAR                    AS MODALITY,
    e.META:MANUFACTURER::VARCHAR                AS MANUFACTURER,
    e.META:INSTITUTIONNAME::VARCHAR             AS INSTITUTION_NAME,
    e.META:BODYPARTEXAMINED::VARCHAR            AS BODY_PART_EXAMINED,
    e.META:ROWS::NUMBER                         AS IMAGE_ROWS,
    e.META:COLUMNS::NUMBER                      AS IMAGE_COLUMNS,
    e.META:BITSALLOCATED::NUMBER                AS BITS_ALLOCATED,
    e.META:BITSSTORED::NUMBER                   AS BITS_STORED,
    e.META:PIXELSPACING::VARCHAR                AS PIXEL_SPACING,
    e.META:SLICETHICKNESS::VARCHAR              AS SLICE_THICKNESS,
    e.META:SLICELOCATION::VARCHAR               AS SLICE_LOCATION,
    e.META:IMAGEPOSITIONPATIENT::VARCHAR        AS IMAGE_POSITION_PATIENT,
    e.META:IMAGEORIENTATIONPATIENT::VARCHAR     AS IMAGE_ORIENTATION_PATIENT,
    e.META:WINDOWCENTER::VARCHAR                AS WINDOW_CENTER,
    e.META:WINDOWWIDTH::VARCHAR                 AS WINDOW_WIDTH,
    e.META:INSTANCENUMBER::VARCHAR              AS INSTANCE_NUMBER,
    e.META:ACQUISITIONNUMBER::VARCHAR           AS ACQUISITION_NUMBER,
    e.META:PHOTOMETRICINTERPRETATION::VARCHAR   AS PHOTOMETRIC_INTERPRETATION,
    e.META:KVP::VARCHAR                         AS KVP,
    e.META:EXPOSURE::VARCHAR                    AS EXPOSURE,
    e.META:CONVOLUTIONKERNEL::VARCHAR           AS CONVOLUTION_KERNEL,
    e.META:PROTOCOLNAME::VARCHAR                AS PROTOCOL_NAME,
    e.META:TRANSFERSYNTAXUID::VARCHAR           AS TRANSFER_SYNTAX_UID,
    m.LABEL                                     AS SERIES_LABEL,
    m.COLLECTION                                AS COLLECTION_NAME
FROM extracted e
JOIN uuid_map m ON e.SERIES_UUID = m.UUID;

-- Verify the load
SELECT
    COLLECTION_NAME,
    SERIES_LABEL,
    COUNT(*) AS FILE_COUNT,
    COUNT(DISTINCT PATIENT_ID) AS PATIENTS,
    ANY_VALUE(MANUFACTURER) AS MANUFACTURER,
    ROUND(SUM(FILE_SIZE)/1024/1024, 1) AS SIZE_MB
FROM DICOM_CARDIAC_METADATA
GROUP BY 1, 2
ORDER BY FILE_COUNT DESC;
