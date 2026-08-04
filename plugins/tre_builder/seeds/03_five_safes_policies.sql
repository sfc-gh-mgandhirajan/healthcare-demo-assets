-- =====================================================================
-- tre-builder · seed 03 · Five Safes Policy Pack
-- =====================================================================
-- Binds Safe Data (masking + classification tags) and Safe Projects /
-- Safe Settings (protocol registry + time-bound row access) to the
-- SAMPLE_OMOP data. Policies are DEFINER's-rights and enforced no matter
-- which tool or agent queries the data.
--
-- Tiering (by ACTIVE role — CURRENT_ROLE(), so "USE ROLE X" changes the
-- view predictably even for users who hold multiple roles / secondary
-- roles; real single-tier users get their tier automatically):
--   ADMIN / PI     : see all rows, UNMASKED (governance/oversight).
--   RESEARCHER     : see only persons in an ACTIVE, non-expired protocol
--                    cohort; PII masked.
--   ANALYST        : see all rows (for aggregates) but PII masked.
--   (any other role, e.g. ACCOUNTADMIN): no rows — operate via a TRE role.
--
-- Parameter: {{TRE}}
-- Run after 02a (SAMPLE_OMOP must exist).
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- ---------- Safe Projects: protocol registry + cohort -----------------
CREATE TABLE IF NOT EXISTS TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOLS (
  PROTOCOL_ID STRING, TITLE STRING, PI STRING, STATUS STRING,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), EXPIRES_AT TIMESTAMP_NTZ );

CREATE TABLE IF NOT EXISTS TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOL_COHORT (
  PROTOCOL_ID STRING, PERSON_ID NUMBER );

-- Seed one ACTIVE demo protocol scoped to a 200-person cohort so the
-- row-access behaviour is demonstrable out of the box.
MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOLS t
  USING (SELECT 'PROTO-001' AS PROTOCOL_ID) s ON t.PROTOCOL_ID = s.PROTOCOL_ID
  WHEN NOT MATCHED THEN INSERT (PROTOCOL_ID, TITLE, PI, STATUS, EXPIRES_AT)
    VALUES ('PROTO-001','Aging-trajectory pilot cohort','TRE_{{TRE}}_PI','ACTIVE',
            DATEADD('year',1,CURRENT_TIMESTAMP()));

INSERT INTO TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOL_COHORT (PROTOCOL_ID, PERSON_ID)
  SELECT 'PROTO-001', PERSON_ID
  FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON SAMPLE (200 ROWS)
  WHERE NOT EXISTS (SELECT 1 FROM TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOL_COHORT c WHERE c.PROTOCOL_ID='PROTO-001');

-- ---------- Safe Data: classification tag ------------------------------
CREATE TAG IF NOT EXISTS TRE_{{TRE}}_DB.GOVERNANCE.DATA_CLASS
  ALLOWED_VALUES 'PHI','PII','QUASI_IDENTIFIER','PUBLIC'
  COMMENT = 'TRE data classification (Safe Data)';

-- ---------- Safe Data: masking policies (unmask only for PI/ADMIN) -----
CREATE OR REPLACE MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_DATE
  AS (val DATE) RETURNS DATE ->
    CASE WHEN CURRENT_ROLE() IN ('TRE_{{TRE}}_PI','TRE_{{TRE}}_ADMIN')
         THEN val ELSE DATE_FROM_PARTS(YEAR(val),1,1) END;  -- generalize to year

CREATE OR REPLACE MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_TEXT
  AS (val STRING) RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('TRE_{{TRE}}_PI','TRE_{{TRE}}_ADMIN')
         THEN val ELSE '***MASKED***' END;

-- Apply to PERSON PII/PHI columns + tag them.
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.PERSON MODIFY
  COLUMN BIRTH_DATE   SET MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_DATE,
  COLUMN DEATH_DATE   SET MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_DATE,
  COLUMN DEATH_CAUSE  SET MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_TEXT,
  COLUMN SES_CHILDHOOD SET MASKING POLICY TRE_{{TRE}}_DB.GOVERNANCE.MASK_TEXT;

ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.PERSON MODIFY
  COLUMN BIRTH_DATE    SET TAG TRE_{{TRE}}_DB.GOVERNANCE.DATA_CLASS = 'PHI',
  COLUMN DEATH_DATE    SET TAG TRE_{{TRE}}_DB.GOVERNANCE.DATA_CLASS = 'PHI',
  COLUMN DEATH_CAUSE   SET TAG TRE_{{TRE}}_DB.GOVERNANCE.DATA_CLASS = 'PHI',
  COLUMN SES_CHILDHOOD SET TAG TRE_{{TRE}}_DB.GOVERNANCE.DATA_CLASS = 'QUASI_IDENTIFIER';

-- ---------- Safe Settings: time-bound, protocol-scoped row access ------
CREATE OR REPLACE ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE
  AS (person_id NUMBER) RETURNS BOOLEAN ->
    -- PI / ADMIN: full visibility.
    CURRENT_ROLE() IN ('TRE_{{TRE}}_PI','TRE_{{TRE}}_ADMIN')
    -- RESEARCHER: only persons in an ACTIVE, non-expired protocol cohort.
    OR ( CURRENT_ROLE() = 'TRE_{{TRE}}_RESEARCHER'
         AND person_id IN (
           SELECT c.PERSON_ID
           FROM TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOL_COHORT c
           JOIN TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOLS p ON p.PROTOCOL_ID = c.PROTOCOL_ID
           WHERE p.STATUS = 'ACTIVE' AND p.EXPIRES_AT > CURRENT_TIMESTAMP() ) )
    -- ANALYST: all rows (masked) for aggregate work.
    OR ( CURRENT_ROLE() = 'TRE_{{TRE}}_ANALYST' );

-- Apply the row access policy to every person-keyed table.
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.PERSON               ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.VISIT_OCCURRENCE     ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.CONDITION_OCCURRENCE ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.DRUG_EXPOSURE        ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.MEASUREMENT          ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.OBSERVATION          ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.PATIENT_FEATURES     ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);
ALTER TABLE TRE_{{TRE}}_DB.OMOP_CDM.TRAJECTORY_ARCHETYPES ADD ROW ACCESS POLICY TRE_{{TRE}}_DB.GOVERNANCE.PERSON_SCOPE ON (PERSON_ID);

MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
  USING (SELECT 'phase' AS KEY, '2-policies' AS VALUE) s ON t.KEY = s.KEY
  WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

SELECT 'Five Safes policy pack applied: masking (4 cols), row access (8 tables), protocol PROTO-001' AS status;
