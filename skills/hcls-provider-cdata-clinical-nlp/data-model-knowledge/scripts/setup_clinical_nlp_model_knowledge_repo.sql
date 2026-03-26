USE DATABASE UNSTRUCTURED_HEALTHDATA;
USE SCHEMA DATA_MODEL_KNOWLEDGE;

CREATE TABLE IF NOT EXISTS CLINICAL_NLP_MODEL_REFERENCE (
    TABLE_NAME          VARCHAR     NOT NULL,
    COLUMN_NAME         VARCHAR     NOT NULL,
    DATA_TYPE           VARCHAR     NOT NULL,
    CONSTRAINTS         VARCHAR,
    DESCRIPTION         VARCHAR,
    FHIR_RESOURCE       VARCHAR,
    FHIR_FIELD          VARCHAR,
    CATEGORY            VARCHAR     NOT NULL,
    CONTAINS_PHI        VARCHAR(1)  NOT NULL,
    RELATIONSHIPS       VARCHAR,
    ENUM_VALUES         VARCHAR,
    SEARCH_TEXT         VARCHAR     NOT NULL
);

CREATE STAGE IF NOT EXISTS clinical_nlp_model_stage;

PUT file://references/clinical_nlp_model_search_corpus.csv @clinical_nlp_model_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

TRUNCATE TABLE IF EXISTS CLINICAL_NLP_MODEL_REFERENCE;

COPY INTO CLINICAL_NLP_MODEL_REFERENCE
FROM @clinical_nlp_model_stage/clinical_nlp_model_search_corpus.csv
FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
);

CREATE OR REPLACE CORTEX SEARCH SERVICE CLINICAL_NLP_MODEL_SEARCH_SVC
    ON search_text
    ATTRIBUTES table_name, column_name, data_type, category, contains_phi, fhir_resource
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
AS (
    SELECT search_text, table_name, column_name, data_type, fhir_resource,
           fhir_field, category, description, constraints, contains_phi,
           relationships, enum_values
    FROM CLINICAL_NLP_MODEL_REFERENCE
);
