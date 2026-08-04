-- =====================================================================
-- tre-builder · deployer bootstrap  (run ONCE, by ACCOUNTADMIN)
-- =====================================================================
-- Mints a LEAST-PRIVILEGE role, TRE_DEPLOYER, that can deploy tre-builder
-- TREs WITHOUT being ACCOUNTADMIN, then you delegate it to trusted people.
-- "The admin sets up the factory once; trusted people self-serve."
--
-- After this, deployers run the phased seeds with DEPLOY_ROLE=TRE_DEPLOYER.
--
-- What TRE_DEPLOYER can do: create the isolated database, warehouse, and
-- tier roles for a TRE, and (by owning what it creates) manage that TRE's
-- policies, tags, semantic view, agent, and airlock. It CANNOT touch data
-- or objects outside the TREs it deploys. MANAGE GRANTS is deliberately
-- NOT granted — owning the roles/objects it creates is sufficient.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS TRE_DEPLOYER
  COMMENT = 'Least-privilege role that can deploy tre-builder Trusted Research Environments';

-- Account-level create verbs the phased seeds need.
GRANT CREATE DATABASE  ON ACCOUNT TO ROLE TRE_DEPLOYER;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TRE_DEPLOYER;
GRANT CREATE ROLE      ON ACCOUNT TO ROLE TRE_DEPLOYER;

-- Cortex for the in-perimeter explorer agent + Cortex Analyst.
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE TRE_DEPLOYER;

-- Roll the deployer up to SYSADMIN so its created objects are visible to
-- the standard admin hierarchy (optional but tidy).
GRANT ROLE TRE_DEPLOYER TO ROLE SYSADMIN;

-- ---------- Delegate to trusted deployers (edit this list) ------------
-- GRANT ROLE TRE_DEPLOYER TO USER <user1>;
-- GRANT ROLE TRE_DEPLOYER TO USER <user2>;

-- ---------- CLONE MODE ONLY: grant read on your sample source ----------
-- TRE_DEPLOYER also needs SELECT on the in-account reference cohort it clones from:
--   GRANT USAGE ON DATABASE  <SRC_DB> TO ROLE TRE_DEPLOYER;
--   GRANT USAGE ON SCHEMA    <SRC_DB>.<SRC_SCHEMA> TO ROLE TRE_DEPLOYER;
--   GRANT SELECT ON ALL TABLES IN SCHEMA <SRC_DB>.<SRC_SCHEMA> TO ROLE TRE_DEPLOYER;

SELECT 'TRE_DEPLOYER minted. Grant it to trusted deployers, then run the phased seeds with DEPLOY_ROLE=TRE_DEPLOYER.' AS status;
