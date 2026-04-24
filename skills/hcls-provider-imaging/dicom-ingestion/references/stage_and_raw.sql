-- =============================================================================
-- DICOM Ingestion: Stage Setup, Raw Table, and File Formats
-- Database: {database}.{schema}
-- =============================================================================

USE DATABASE {database};
USE SCHEMA {schema};

-- Internal stage
CREATE STAGE IF NOT EXISTS {database}.{schema}.DICOM_STAGE ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- File formats
CREATE OR REPLACE FILE FORMAT {database}.{schema}.DICOM_JSON_FORMAT
    TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE ALLOW_DUPLICATE = TRUE IGNORE_UTF8_ERRORS = TRUE;

CREATE OR REPLACE FILE FORMAT {database}.{schema}.DICOM_CSV_FORMAT
    TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL');

CREATE OR REPLACE FILE FORMAT {database}.{schema}.DICOM_PARQUET_FORMAT TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE;

-- External stage (adapt URL/integration per cloud provider)
-- CREATE STAGE IF NOT EXISTS DICOM_EXTERNAL_STAGE
--     URL = 's3://your-bucket/dicom-exports/'
--     STORAGE_INTEGRATION = your_storage_integration
--     FILE_FORMAT = DICOM_JSON_FORMAT;

-- Raw landing table with change tracking
CREATE OR REPLACE TABLE {database}.{schema}.DICOM_RAW (
    RAW_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
    FILE_PATH VARCHAR NOT NULL,
    METADATA VARIANT NOT NULL,
    FILE_FORMAT VARCHAR DEFAULT 'JSON',
    SOURCE_SYSTEM VARCHAR DEFAULT 'PACS',
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID VARCHAR DEFAULT UUID_STRING()
) CHANGE_TRACKING = TRUE DATA_RETENTION_TIME_IN_DAYS = 14;

-- Batch load from stage
COPY INTO {database}.{schema}.DICOM_RAW (FILE_PATH, METADATA)
FROM (SELECT METADATA$FILENAME, $1 FROM @{database}.{schema}.DICOM_STAGE)
FILE_FORMAT = {database}.{schema}.DICOM_JSON_FORMAT ON_ERROR = 'CONTINUE';
