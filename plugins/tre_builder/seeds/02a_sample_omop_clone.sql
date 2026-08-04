-- =====================================================================
-- tre-builder · seed 02a · SAMPLE_OMOP  (DATA_SOURCE = clone)
-- =====================================================================
-- Zero-copy clone of an in-account reference cohort into the vacuum.
-- Fast, no storage cost, and INDEPENDENT of the source after creation
-- (copy-on-write): dropping the source later does not affect this copy,
-- so the vacuum stays self-contained.
--
-- Parameters:
--   {{TRE}}            environment slug
--   {{SAMPLE_SOURCE}}  fully-qualified source schema holding the reference
--                      cohort tables (brand-neutral; e.g. a *_REFERENCE_DB.COHORT).
--                      The tre-deploy skill supplies this; it is never hard-coded here.
--
-- Lands the neutral schema TRE_{{TRE}}_DB.OMOP_CDM with the SAMPLE_OMOP tables.
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- Standard OMOP CDM tables + patient-feature / trajectory helpers.
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.PERSON               CLONE {{SAMPLE_SOURCE}}.PERSON;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.VISIT_OCCURRENCE     CLONE {{SAMPLE_SOURCE}}.VISIT_OCCURRENCE;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.CONDITION_OCCURRENCE CLONE {{SAMPLE_SOURCE}}.CONDITION_OCCURRENCE;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.DRUG_EXPOSURE        CLONE {{SAMPLE_SOURCE}}.DRUG_EXPOSURE;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.MEASUREMENT          CLONE {{SAMPLE_SOURCE}}.MEASUREMENT;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.OBSERVATION          CLONE {{SAMPLE_SOURCE}}.OBSERVATION;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.PATIENT_FEATURES     CLONE {{SAMPLE_SOURCE}}.PATIENT_FEATURES;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.OMOP_CDM.TRAJECTORY_ARCHETYPES CLONE {{SAMPLE_SOURCE}}.TRAJECTORY_ARCHETYPES;

-- Base read grant (bottom tier -> inherited up the hierarchy).
GRANT SELECT ON ALL TABLES IN SCHEMA TRE_{{TRE}}_DB.OMOP_CDM TO ROLE TRE_{{TRE}}_ANALYST;

-- Record the load mode.
MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
  USING (SELECT 'data_source' AS KEY, 'clone' AS VALUE) s ON t.KEY = s.KEY
  WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

SELECT 'SAMPLE_OMOP loaded via clone into TRE_{{TRE}}_DB.OMOP_CDM ('
    || (SELECT COUNT(*) FROM TRE_{{TRE}}_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='OMOP_CDM')
    || ' tables)' AS status;
