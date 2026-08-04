---
name: tre-deploy
description: Deploy a Trusted Research Environment on Snowflake with tre-builder. Stands up a self-contained, namespaced TRE (vacuum), loads a brand-neutral synthetic SAMPLE_OMOP dataset, applies the Five Safes policy pack, the output airlock, and an in-perimeter read-only Cortex agent — proving each phase before advancing. Triggers: deploy a TRE, stand up a trusted research environment, tre-builder, build a research enclave, Five Safes environment.
---

# TRE Builder — Deploy

Stand up a Trusted Research Environment in a vacuum, one governed phase at a time. **Prove each phase before advancing** (run `tre-verify` after each), so nothing is layered on an unproven control.

## Step 0 — One-time deployer setup (admin, optional but recommended)

Deploying a TRE is privileged (it creates databases, roles, warehouses, and governance policies). You can deploy as `ACCOUNTADMIN`, or — so non-admins can self-serve — an admin runs [`seeds/deployer_bootstrap.sql`](../../seeds/deployer_bootstrap.sql) **once** to mint a least-privilege `TRE_DEPLOYER` role, then delegates it:
```sql
GRANT ROLE TRE_DEPLOYER TO USER <deployer>;
-- clone mode only: also grant TRE_DEPLOYER SELECT on your sample source
```
Then deployers run the phases below with `DEPLOY_ROLE = TRE_DEPLOYER` (no ACCOUNTADMIN needed). `TRE_DEPLOYER` can only create/manage the TREs it deploys — nothing else in the account.

## Step 1 — Gather parameters

Ask the user for these (use sensible defaults):

| Token | Meaning | Default |
|---|---|---|
| `{{TRE}}` | short slug for this environment (A-Z, 0-9, _) | `DEMO` |
| `{{DEPLOY_ROLE}}` | role that runs the deploy | `ACCOUNTADMIN` (or `TRE_DEPLOYER`) |
| `{{HIPAA_MODE}}` | `TRUE` if real PHI is in scope (requires Business Critical) | `FALSE` |
| `{{EDITION_ACK}}` | attested edition when HIPAA_MODE=TRUE: `BUSINESS_CRITICAL` or `VPS` | (empty in demo) |
| `{{SAMPLE_SOURCE}}` | FQ source schema of the in-account reference cohort to clone | (ask; e.g. a `*_REFERENCE_DB.COHORT`) |
| `{{WH_SIZE}}` | warehouse size | `XSMALL` |

Substitute these tokens into each seed before running it.

## Step 2 — Deploy phase by phase (verify between each)

Run as `{{DEPLOY_ROLE}}` (ACCOUNTADMIN or TRE_DEPLOYER). After each phase, run the matching `tre-verify` pack; **do not advance if a phase fails.**

1. **Foundation + edition gate** — run [`seeds/00_vacuum_foundation.sql`](../../seeds/00_vacuum_foundation.sql) then [`seeds/01_edition_gate.sql`](../../seeds/01_edition_gate.sql). The gate REFUSES to proceed if `HIPAA_MODE=TRUE` without a Business Critical attestation. Verify: `tre-verify` phase `foundation`.
2. **SAMPLE_OMOP** — run [`seeds/02a_sample_omop_clone.sql`](../../seeds/02a_sample_omop_clone.sql) to zero-copy clone the brand-neutral synthetic reference cohort into `TRE_{{TRE}}_DB.OMOP_CDM`. Verify: phase `data`. (A portable `bundled` Parquet load is a planned follow-up — TRE-3 — not in this release; `clone` is the supported source.)
3. **Five Safes policy pack** — run [`seeds/03_five_safes_policies.sql`](../../seeds/03_five_safes_policies.sql). Verify: phase `policies` (positive + negative).
4. **Output airlock** — run [`seeds/04_airlock.sql`](../../seeds/04_airlock.sql). Verify: phase `airlock`.
5. **In-perimeter agent + secure share** — run [`seeds/05_agent.sql`](../../seeds/05_agent.sql). Verify: phase `agent`.

## Step 3 — Assign people

Grant the tier roles to real users, e.g.:
```sql
GRANT ROLE TRE_{{TRE}}_RESEARCHER TO USER <researcher>;
GRANT ROLE TRE_{{TRE}}_PI         TO USER <pi>;
```
Add a researcher's cohort scope by inserting into `GOVERNANCE.PROTOCOL_COHORT` under an ACTIVE `GOVERNANCE.PROTOCOLS` row.

## Step 4 — Demonstrate the airlock

As a researcher, build a scoped aggregate in WORKSPACE, submit it, then (as two different PIs) approve and release:
```sql
USE ROLE TRE_{{TRE}}_RESEARCHER;
CREATE OR REPLACE TABLE TRE_{{TRE}}_DB.WORKSPACE.GENDER_COUNTS AS
  SELECT GENDER_SOURCE_VALUE AS GENDER, COUNT(*) AS N
  FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON GROUP BY 1;      -- runs under researcher scope + masking
CALL TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS(
  'PROTO-001','gender counts','TRE_{{TRE}}_DB.WORKSPACE.GENDER_COUNTS','N',11);
-- then, as two DIFFERENT PI/ADMIN users:
--   CALL AIRLOCK.REVIEW_EGRESS('<id>','APPROVE','ok');   (x2, different users)
--   CALL AIRLOCK.RELEASE_EGRESS('<id>');
```

## Notes
- Everything is namespaced `TRE_{{TRE}}_*`; nothing else in the account is touched.
- To remove it all, use the `tre-teardown` skill.
- `clone` mode is independent of its source after creation (copy-on-write) — the vacuum stays self-contained.
