---
name: tre-teardown
description: Cleanly remove a tre-builder Trusted Research Environment. Drops the namespaced database (schemas, tables, policies, tags, procedures, semantic view, agent), the warehouse, and the four tier roles. Never touches the SAMPLE_OMOP clone source or anything outside the vacuum. Triggers: tear down the TRE, remove the research environment, delete tre-builder deploy, clean up the TRE.
---

# TRE Builder — Teardown

Because everything is namespaced and self-contained, teardown is a small, safe set of drops.

## Step 1 — Confirm the target
Ask the user for `{{TRE}}` and confirm they intend to remove `TRE_{{TRE}}_DB`, `TRE_{{TRE}}_WH`, and the four `TRE_{{TRE}}_*` roles.

## Step 2 — Run teardown
Substitute `{{TRE}}` and run [`seeds/99_teardown.sql`](../../seeds/99_teardown.sql) as `ACCOUNTADMIN` (or the deploying role).

## What it removes
- `TRE_{{TRE}}_DB` and everything inside it: `OMOP_CDM` (data, semantic view, agent, secure view), `GOVERNANCE` (policies, tags, protocol registry, config), `AIRLOCK` (egress ledger, audit, procedures).
- `TRE_{{TRE}}_WH`.
- Roles `TRE_{{TRE}}_ADMIN`, `_PI`, `_RESEARCHER`, `_ANALYST`.

## What it does NOT touch
- The `SAMPLE_OMOP` clone source (e.g. a `*_REFERENCE_DB.COHORT`) — clone mode makes an independent copy, so dropping the TRE never affects the source.
- Anything outside the `TRE_{{TRE}}_*` namespace.

## Note on regulated teardown (roadmap)
For a real regulated TRE, teardown should also produce proof-of-destruction and preserve the airlock audit trail per retention policy. Phase 1 removes the objects; the retention/attestation workflow is a Phase 2 enhancement.
