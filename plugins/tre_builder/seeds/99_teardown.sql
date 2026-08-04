-- =====================================================================
-- tre-builder · seed 99 · Teardown
-- =====================================================================
-- Cleanly removes the entire vacuum for {{TRE}}. Because everything is
-- namespaced and self-contained, teardown is a small, safe set of drops.
-- Dropping the database removes its schemas, tables, masking/row-access
-- policies, tags, procedures, the airlock, and the semantic view/agent
-- objects that live inside it.
--
-- Parameter: {{TRE}}
--
-- SAFETY: this only touches TRE_{{TRE}}_* objects. It never touches the
-- SAMPLE_OMOP source used by clone mode, nor anything outside the vacuum.
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- Database (schemas, tables, policies, tags, procs, semantic view, agent).
DROP DATABASE IF EXISTS TRE_{{TRE}}_DB;

-- Warehouse.
DROP WAREHOUSE IF EXISTS TRE_{{TRE}}_WH;

-- Roles (children first for tidiness; order is not strictly required).
DROP ROLE IF EXISTS TRE_{{TRE}}_ANALYST;
DROP ROLE IF EXISTS TRE_{{TRE}}_RESEARCHER;
DROP ROLE IF EXISTS TRE_{{TRE}}_PI;
DROP ROLE IF EXISTS TRE_{{TRE}}_ADMIN;

SELECT 'Teardown complete: TRE_{{TRE}}_DB, TRE_{{TRE}}_WH, and 4 roles removed' AS status;
