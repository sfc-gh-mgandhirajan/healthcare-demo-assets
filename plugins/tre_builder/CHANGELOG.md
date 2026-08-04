# Changelog

All notable changes to tre-builder are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [0.2.2] — 2026-08-03

### Security — output airlock: close the egress privilege-escalation path (PR #25 review)

`AIRLOCK.REQUEST_EGRESS` ran `EXECUTE AS OWNER` and pasted caller-supplied `P_SOURCE_OBJECT`/`P_COUNT_COL` into dynamic SQL, so the snapshot was read as the owner — meaning a researcher could submit `OMOP_CDM.PERSON` (or a view) and, depending on the deploy role, capture an **unmasked** copy, defeating the Five Safes it enforces. The `CREATE OR REPLACE TABLE` also auto-committed before the SDC check, leaving failed snapshots behind.

- **Source constrained to a WORKSPACE base table**, validated via `INFORMATION_SCHEMA` (views, `OMOP_CDM`, and other schemas rejected) and rebuilt as quoted identifiers — no raw interpolation. WORKSPACE tables are masked-at-rest (materialized under the researcher's masked/row-scoped role), so the owner snapshot is faithful **regardless of which role deployed the TRE**.
- **`P_COUNT_COL`** must be an existing numeric column (validated + quoted).
- **SDC runs before any snapshot**; the `RESULT_` table is created only on pass and dropped on failure — nothing unreviewed persists.
- **`REVIEW_EGRESS`**: added separation-of-duties — the requester cannot approve their own egress (in addition to the existing distinct-second-approver rule; `RELEASE_EGRESS` still gates on two approvals).
- **`tre-verify`** gains an airlock negative assertion (a researcher submitting a raw `OMOP_CDM` source is denied and leaves no `RESULT_` table).

### Changed
- **Dropped the non-functional `bundled` data source** (it required Parquet the plugin never shipped). `clone` is the supported source; portable bundled Parquet remains backlog (TRE-3). Removed `seeds/02b_sample_omop_bundled.sql` and the `DATA_SOURCE` choice from `tre-deploy`.
- **Removed `author.url`** from the manifest (personal profile; matches maude/dicom which omit `url`).

## [0.2.1] — 2026-08-03

### Chore — incubator-ready manifest (Snowflake-Solutions HCLS incubator)

Normalized `.cortex-plugin/plugin.json` to the Snowflake-Solutions HCLS incubator plugin convention so the plugin can be submitted to `sf-hcls-solutions-incubator` under `plugins/tre_builder/` and kept in sync without manifest drift: added `displayName`, `license` (Apache-2.0), `keywords` (incl. `hcls`), `author.email`; changed `skills`/`agents` from arrays to strings (`"./skills"`, `"./agents"`) to match sibling plugins (maude, dicom). No behavior change.

## [0.2.0] — 2026-07-17

### Feature — least-privilege deployer role (TRE-2 / INIT-97)

Deploying no longer requires ACCOUNTADMIN. `seeds/deployer_bootstrap.sql` (run once by an admin) mints a least-privilege `TRE_DEPLOYER` role (CREATE DATABASE/WAREHOUSE/ROLE + SNOWFLAKE.CORTEX_USER); delegate it to trusted people so they self-serve. All phased seeds are parameterized with `{{DEPLOY_ROLE}}` (default ACCOUNTADMIN, recommended TRE_DEPLOYER). Validated: `TRE_DEPLOYER` can create the database, warehouse, roles, masking + row-access policies, semantic view, and agent purely via ownership. Also: `seed 00` now `USE WAREHOUSE`s the TRE warehouse it creates (so a deployer with no default warehouse works), and the airlock's WORKSPACE read grant targets `{{DEPLOY_ROLE}}`.

Backlog opened under INIT-97: TRE-3 (bundle SAMPLE_OMOP parquet), TRE-4 (catalog distribution), TRE-5 (fleet registry + tre-status), TRE-6 (lifecycle/teardown), TRE-7 (idempotent re-deploy/upgrade), TRE-8 (separate-account mode).

## [0.1.0] — 2026-07-17

### Feature — Phase 1 MVP (INIT-97)

Initial release. Vacuum-deployable, phase-gated Trusted Research Environment builder.

- **Vacuum foundation** — parameterized `TRE_<NAME>_DB` / `_WH` and 3-tier RBAC (`TRE_<NAME>_ADMIN > PI > RESEARCHER > ANALYST`); idempotent deploy + clean teardown.
- **Edition gate** — detects account edition; `HIPAA_MODE=true` requires Business Critical and fails loudly otherwise; labeled non-HIPAA `DEMO_MODE`.
- **SAMPLE_OMOP** — brand-neutral synthetic OMOP dataset in two load modes (`clone` in-account zero-copy, or `bundled` Parquet via `COPY INTO`), both landing `TRE_<NAME>_DB.OMOP_CDM`.
- **Five Safes policy pack** — dynamic masking (dates, cause-of-death, socioeconomic), row access per protocol, tags + classification, secure views, protocol registry with time-bound access.
- **Output airlock (hero)** — `EGRESS_REQUESTS` + request/review/release procedures enforcing statistical disclosure control (minimum cell count, aggregation), dual sign-off, append-only `AIRLOCK_AUDIT`.
- **In-perimeter Cortex agent** — read-only NL exploration over the OMOP semantic view; policy-masked columns are invisible to the agent by architecture.
- **Deploy / verify / teardown skills** — `tre-deploy` orchestrates the phased build; `tre-verify` runs positive + negative test packs that gate advancement; `tre-teardown` removes the vacuum.
- **Docs** — SATRE / Five Safes conformance mapping.

Roadmap: Phase 2 (FHIR/genomics/RWE, MCP airlock + agent), Phase 3 (Clean Rooms, federated analysis, Marketplace).
