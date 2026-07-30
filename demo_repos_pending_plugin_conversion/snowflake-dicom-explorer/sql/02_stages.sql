-- =============================================================================
-- 02: External Stages
-- Connects Snowflake to the NCI Imaging Data Commons (IDC) public S3 bucket.
-- No credentials required — IDC is a public open-data resource.
-- =============================================================================

USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;

-- Stage 1: Full IDC open data bucket (directory-enabled for selective SUBPATH refresh)
CREATE OR REPLACE STAGE IDC_OPEN_DATA_ALL_STG
    URL = 's3://idc-open-data'
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'NCI Imaging Data Commons - full public S3 bucket. Directory table refreshed per-series via SUBPATH.';

-- Stage 2: Single cardiac CT series (original demo subset)
CREATE OR REPLACE STAGE IDC_OPEN_DATA_CARDIAC_STG
    URL = 's3://idc-open-data/7e1bcefd-d097-47c0-9d54-65bd5d9674cf/'
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Single cardiac CT angiography series (AMC-015, SIEMENS, 372 slices). Original demo data.';

-- Refresh directory tables for all 10 curated series
-- Each UUID maps to a DICOM series folder in the IDC bucket
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '7e1bcefd-d097-47c0-9d54-65bd5d9674cf/';  -- nsclc_radiogenomics: Cardiac CTA Diastolic 70% AMC-015 (372 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '3b5970f6-f130-4337-9666-a9741a267268/';  -- varepop_apollo: Cardiac CTA Systolic 31% AP-26JK (459 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '4bed8f9a-bf84-44da-ac8d-d39e15d9c978/';  -- nsclc_radiogenomics: Cardiac CT Systolic 38% AMC-027 (336 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = 'f53b78ea-92ea-479e-a89f-967bec74e848/';  -- rider_lung_pet_ct: Gated Segment 0.625mm RIDER-2019259100 (570 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '1d92626a-e39d-4cf5-8ed9-864aae909d3d/';  -- rider_lung_pet_ct: Cine 30-65 BPM RIDER-2266952716 (280 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '7f4806f3-5946-41cb-8684-8fd5d51f8931/';  -- covid_19_ny_sbu: Cardiac 2.5 B41s A130302 (85 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = '6dfa81e0-3520-4332-aecc-0767dd1cab74/';  -- covid_19_ny_sbu: CTA 3.0 CE A095019 (130 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = 'f6fc9ace-5516-418d-b979-7ffaf14de22b/';  -- nlst: Chest CT Philips 202207 (258 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = 'bb721c76-8056-4780-97be-1bc61b469454/';  -- nlst: Chest CT Siemens B30f 133417 (198 files)
ALTER STAGE IDC_OPEN_DATA_ALL_STG REFRESH SUBPATH = 'de9868a4-a9d9-4652-9f0f-04e06a95d09e/';  -- nlst: Chest CT Toshiba FC10 200119 (160 files)

-- Refresh the single-series cardiac stage
ALTER STAGE IDC_OPEN_DATA_CARDIAC_STG REFRESH;
