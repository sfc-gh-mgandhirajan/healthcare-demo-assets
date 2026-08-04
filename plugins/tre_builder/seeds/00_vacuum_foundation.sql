-- =====================================================================
-- tre-builder · seed 00 · Vacuum Foundation
-- =====================================================================
-- Stands up an ISOLATED, namespaced TRE: warehouse, database, schemas,
-- and the 3-tier Five Safes role hierarchy. Idempotent. No dependency on
-- anything else in the account. Paired with 99_teardown.sql.
--
-- Parameters (the tre-deploy skill substitutes these before running):
--   {{TRE}}          short slug for this environment, e.g. COH  (A-Z0-9_)
--   {{WH_SIZE}}      warehouse size, default XSMALL
--   {{DEPLOY_ROLE}}  role running the deploy, default ACCOUNTADMIN
--                    (recommended: TRE_DEPLOYER — see deployer_bootstrap.sql)
--
-- Objects created (all namespaced by {{TRE}}):
--   Warehouse : TRE_{{TRE}}_WH
--   Database  : TRE_{{TRE}}_DB   (schemas: OMOP_CDM, GOVERNANCE, AIRLOCK)
--   Roles     : TRE_{{TRE}}_ADMIN > PI > RESEARCHER > ANALYST
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- ---------- Warehouse (business-hours friendly; suspends fast) --------
CREATE WAREHOUSE IF NOT EXISTS TRE_{{TRE}}_WH
  WAREHOUSE_SIZE = '{{WH_SIZE}}'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'tre-builder vacuum warehouse for {{TRE}}';

-- Use it immediately so the deploy runs even if the deploy role has no
-- default warehouse (later statements need compute).
USE WAREHOUSE TRE_{{TRE}}_WH;

-- ---------- Database + schemas ----------------------------------------
CREATE DATABASE IF NOT EXISTS TRE_{{TRE}}_DB
  COMMENT = 'tre-builder vacuum TRE for {{TRE}} — Five Safes enforced';

CREATE SCHEMA IF NOT EXISTS TRE_{{TRE}}_DB.OMOP_CDM
  COMMENT = 'Safe Data — OMOP CDM (SAMPLE_OMOP)';
CREATE SCHEMA IF NOT EXISTS TRE_{{TRE}}_DB.GOVERNANCE
  COMMENT = 'Policies, tags, protocol registry, deploy config';
CREATE SCHEMA IF NOT EXISTS TRE_{{TRE}}_DB.AIRLOCK
  COMMENT = 'Safe Outputs — egress requests, review, audit';
CREATE SCHEMA IF NOT EXISTS TRE_{{TRE}}_DB.WORKSPACE
  COMMENT = 'Researcher scratch — build scoped aggregates here, then submit to the airlock';

-- Drop the default PUBLIC schema to keep the vacuum tidy (ignore if absent).
DROP SCHEMA IF EXISTS TRE_{{TRE}}_DB.PUBLIC;

-- ---------- 3-tier role hierarchy (ANALYST < RESEARCHER < PI < ADMIN) -
CREATE ROLE IF NOT EXISTS TRE_{{TRE}}_ADMIN
  COMMENT = 'TRE steward: deploy, own policies, airlock approver (role A)';
CREATE ROLE IF NOT EXISTS TRE_{{TRE}}_PI
  COMMENT = 'Principal Investigator: owns protocols, airlock approver (role B)';
CREATE ROLE IF NOT EXISTS TRE_{{TRE}}_RESEARCHER
  COMMENT = 'Researcher: query within an approved protocol, request egress';
CREATE ROLE IF NOT EXISTS TRE_{{TRE}}_ANALYST
  COMMENT = 'Analyst: masked / aggregate read only';

-- Inheritance: higher tiers inherit everything below them.
GRANT ROLE TRE_{{TRE}}_ANALYST    TO ROLE TRE_{{TRE}}_RESEARCHER;
GRANT ROLE TRE_{{TRE}}_RESEARCHER TO ROLE TRE_{{TRE}}_PI;
GRANT ROLE TRE_{{TRE}}_PI         TO ROLE TRE_{{TRE}}_ADMIN;
-- Steward role rolls up to SYSADMIN so a human admin can manage it.
GRANT ROLE TRE_{{TRE}}_ADMIN      TO ROLE SYSADMIN;

-- ---------- Base grants (data-tier differences come from POLICIES) ----
-- Warehouse usage granted at the bottom tier; inheritance carries it up.
GRANT USAGE ON WAREHOUSE TRE_{{TRE}}_WH TO ROLE TRE_{{TRE}}_ANALYST;

-- Read visibility of the environment (again, bottom tier -> inherited up).
GRANT USAGE ON DATABASE TRE_{{TRE}}_DB TO ROLE TRE_{{TRE}}_ANALYST;
GRANT USAGE ON SCHEMA TRE_{{TRE}}_DB.OMOP_CDM   TO ROLE TRE_{{TRE}}_ANALYST;
GRANT USAGE ON SCHEMA TRE_{{TRE}}_DB.GOVERNANCE TO ROLE TRE_{{TRE}}_ANALYST;
GRANT USAGE ON SCHEMA TRE_{{TRE}}_DB.AIRLOCK    TO ROLE TRE_{{TRE}}_ANALYST;
GRANT USAGE ON SCHEMA TRE_{{TRE}}_DB.WORKSPACE  TO ROLE TRE_{{TRE}}_ANALYST;

-- Researchers build scoped result objects in WORKSPACE, then submit them
-- to the airlock. (Analysts inherit nothing extra; only RESEARCHER+ create.)
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA TRE_{{TRE}}_DB.WORKSPACE TO ROLE TRE_{{TRE}}_RESEARCHER;

-- Steward manages the environment.
GRANT ALL ON DATABASE TRE_{{TRE}}_DB TO ROLE TRE_{{TRE}}_ADMIN;
GRANT ALL ON SCHEMA TRE_{{TRE}}_DB.OMOP_CDM   TO ROLE TRE_{{TRE}}_ADMIN;
GRANT ALL ON SCHEMA TRE_{{TRE}}_DB.GOVERNANCE TO ROLE TRE_{{TRE}}_ADMIN;
GRANT ALL ON SCHEMA TRE_{{TRE}}_DB.AIRLOCK    TO ROLE TRE_{{TRE}}_ADMIN;
GRANT ALL ON SCHEMA TRE_{{TRE}}_DB.WORKSPACE  TO ROLE TRE_{{TRE}}_ADMIN;

-- ---------- Deploy config / manifest (single source of deploy truth) --
CREATE TABLE IF NOT EXISTS TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG (
  KEY        STRING,
  VALUE      STRING,
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
USING (SELECT 'tre_name' AS KEY, '{{TRE}}' AS VALUE) s ON t.KEY = s.KEY
WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
USING (SELECT 'phase' AS KEY, '1-foundation' AS VALUE) s ON t.KEY = s.KEY
WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

GRANT SELECT ON TABLE TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG TO ROLE TRE_{{TRE}}_ANALYST;

SELECT 'Vacuum foundation ready: TRE_{{TRE}}_DB + TRE_{{TRE}}_WH + 4 roles' AS status;
