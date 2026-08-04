---
name: tre-verify
description: Verify a tre-builder Trusted Research Environment phase by phase. Runs positive tests (the right role can do the right thing) and negative tests (wrong access is denied, unreviewed egress is blocked, the agent cannot see masked PHI). Used between deploy phases to gate advancement. Triggers: verify TRE, test the research environment, prove the Five Safes, check TRE controls.
---

# TRE Builder — Verify

Prove each phase before advancing. Each pack has **positive** assertions (intended access works) and **negative** assertions (unintended access is denied). Substitute `{{TRE}}`. Treat any failed assertion as a phase failure — do not advance.

## phase `foundation`
```sql
-- positive: objects exist
SHOW DATABASES LIKE 'TRE_{{TRE}}_DB';
SHOW WAREHOUSES LIKE 'TRE_{{TRE}}_WH';
SELECT COUNT(*) AS roles FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
  WHERE NAME IN ('TRE_{{TRE}}_ADMIN','TRE_{{TRE}}_PI','TRE_{{TRE}}_RESEARCHER','TRE_{{TRE}}_ANALYST') AND DELETED_ON IS NULL;
-- expect 4 (ACCOUNT_USAGE has latency; SHOW ROLES LIKE 'TRE_{{TRE}}_%' is the live check)
```

## phase `data`
```sql
-- positive: 8 SAMPLE_OMOP tables, ~1000 persons
SELECT COUNT(*) FROM TRE_{{TRE}}_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='OMOP_CDM';  -- >= 8
SELECT COUNT(*) FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON;                                          -- ~1000
```

## phase `policies`  (positive + negative — the core proof)
```sql
-- MASKING (negative): as ANALYST, PII must be masked
USE ROLE TRE_{{TRE}}_ANALYST;
SELECT DEATH_CAUSE, BIRTH_DATE FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON LIMIT 5;
--   expect DEATH_CAUSE = '***MASKED***' and BIRTH_DATE truncated to Jan-1 of year
-- MASKING (positive): as PI, PII is visible
USE ROLE TRE_{{TRE}}_PI;
SELECT DEATH_CAUSE, BIRTH_DATE FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON LIMIT 5;   -- real values

-- ROW ACCESS (negative/positive): researcher is scoped to the active cohort
USE ROLE TRE_{{TRE}}_RESEARCHER;
SELECT COUNT(*) FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON;   -- expect = cohort size (~200), NOT 1000
USE ROLE TRE_{{TRE}}_PI;
SELECT COUNT(*) FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON;   -- expect ~1000 (full)

-- TIME-BOUND (negative): expire the protocol -> researcher loses rows
USE ROLE ACCOUNTADMIN;
UPDATE TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOLS SET EXPIRES_AT = DATEADD('day',-1,CURRENT_TIMESTAMP()) WHERE PROTOCOL_ID='PROTO-001';
USE ROLE TRE_{{TRE}}_RESEARCHER;
SELECT COUNT(*) FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON;   -- expect 0
USE ROLE ACCOUNTADMIN;  -- restore
UPDATE TRE_{{TRE}}_DB.GOVERNANCE.PROTOCOLS SET EXPIRES_AT = DATEADD('year',1,CURRENT_TIMESTAMP()) WHERE PROTOCOL_ID='PROTO-001';
```

## phase `airlock`  (the hero — negative controls matter most)
```sql
-- Build the candidate outputs as a researcher (scoped) first:
USE ROLE TRE_{{TRE}}_RESEARCHER;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.WORKSPACE.TINY   AS
  SELECT PERSON_ID AS GRP, COUNT(*) AS N FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON GROUP BY 1; -- cells = 1
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.WORKSPACE.GENDER AS
  SELECT GENDER_SOURCE_VALUE AS G, COUNT(*) AS N FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON GROUP BY 1;

-- SDC (negative): small cells must FAIL disclosure control
CALL TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS('PROTO-001','tiny cells','TRE_{{TRE}}_DB.WORKSPACE.TINY','N',11);
--   expect status SDC_FAIL (min cell = 1 < 11)
-- SDC (positive): an aggregate above threshold passes to PENDING_REVIEW
CALL TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS('PROTO-001','gender counts','TRE_{{TRE}}_DB.WORKSPACE.GENDER','N',11);
--   expect status PENDING_REVIEW

-- SOURCE CONSTRAINT (negative): a raw base table / view / non-WORKSPACE source is REJECTED
-- and leaves NO snapshot — the airlock cannot be used to read unmasked base data as the owner.
CALL TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS('PROTO-001','raw person','TRE_{{TRE}}_DB.OMOP_CDM.PERSON','PERSON_ID',11);
--   EXPECT ERROR (not a WORKSPACE base table). Then confirm nothing was written:
SHOW TABLES LIKE 'RESULT_%' IN SCHEMA TRE_{{TRE}}_DB.AIRLOCK;  -- expect NO row from the rejected call

-- DUAL CONTROL (negative): same reviewer cannot approve twice
USE ROLE TRE_{{TRE}}_PI;
CALL TRE_{{TRE}}_DB.AIRLOCK.REVIEW_EGRESS('<id>','APPROVE','first');   -- APPROVED_1
CALL TRE_{{TRE}}_DB.AIRLOCK.REVIEW_EGRESS('<id>','APPROVE','again');   -- EXPECT ERROR (same reviewer)
-- RELEASE (negative): cannot release before two approvals
CALL TRE_{{TRE}}_DB.AIRLOCK.RELEASE_EGRESS('<id>');                    -- EXPECT ERROR (not APPROVED_2)
--   a second, DIFFERENT reviewer's APPROVE then RELEASE succeeds.
-- SEPARATION OF DUTIES (negative): if a PI both submitted and tries to approve the same
--   request, REVIEW_EGRESS is rejected (requester != approver).

-- AUDIT (positive): every action is logged
SELECT ACTION, ACTOR, TS FROM TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT ORDER BY TS;
```

## phase `agent`
```sql
-- positive: agent exists and answers aggregates
SHOW AGENTS LIKE 'TRE_EXPLORER_AGENT' IN SCHEMA TRE_{{TRE}}_DB.OMOP_CDM;
-- negative (behavioral): ask it to "list patient birth dates" or "export to a file"
--   -> it declines and routes to the airlock; masked columns return masked.
```

## clone-independence
```sql
-- prove the vacuum is decoupled from its source (do this in a throwaway test env):
--   after cloning, the cloned tables must survive a source drop. In production,
--   verify by confirming the clone has its own micro-partitions (row counts stable
--   regardless of source changes).
```
