# tre-builder v0.2.0

**Stand up a compliant Trusted Research Environment (TRE) on Snowflake — one governed, phased, vacuum-deployable build.**

TRE Builder is a CoCo plugin that deploys a self-contained Trusted Research Environment into an isolated Snowflake database, enforces the **Five Safes** through policy-as-code and a native **output airlock**, and lets you **prove each control before advancing**. Deploy in a vacuum, verify every Safe holds, then layer the next phase. Tear it all down cleanly when you are done.

Grounded in a proven reference build (City of Hope TRE) and the external TRE standards landscape (SATRE, Five Safes, Genomics England / OpenSAFELY / NHS SDE airlock practice).

---

## Why this exists

Standing up a TRE is repetitive, governance-heavy, and easy to get subtly wrong — and the hardest control (output disclosure review) is the one most builds under-serve. TRE Builder productizes a working reference into a repeatable, self-service deploy, with the **output airlock as the hero feature**, not an afterthought.

## Core principles

1. **Deploy in a vacuum.** Everything is namespaced (`TRE_<NAME>_DB`, `TRE_<NAME>_WH`, `TRE_<NAME>_ADMIN/PI/RESEARCHER/ANALYST`) and self-contained — no runtime dependency on anything else in the account. Clean teardown included.
2. **Prove each phase.** Every phase deploys, then runs a verification pack — **positive** tests (the right role can do the right thing) and **negative** tests (wrong access is denied, unreviewed egress is blocked, the AI agent cannot read masked PHI). Phase N+1 will not deploy until Phase N is green.
3. **Rules are load-bearing.** The Five Safes are enforced structurally — RBAC, masking / row-access policies, and gated egress procedures — never by researcher goodwill.
4. **SATRE + Five Safes as the spine.** External, credible governance rather than homegrown.

## The Five Safes → Snowflake

| Safe | What it means | Snowflake mechanism |
|---|---|---|
| Safe People | only trusted, trained researchers | 3-tier RBAC, MFA, network policy |
| Safe Projects | approved, time-bound purposes | protocol registry + time-bound access |
| Safe Settings | analysis in a controlled environment | row access policies, in-perimeter Cortex agent |
| Safe Data | minimized, de-identified, masked | dynamic masking, tagging, classification, secure views |
| Safe Outputs | nothing leaves without review | **the output airlock** (SDC + dual sign-off + audit) |

## Sample data — `SAMPLE_OMOP`

Ships with a brand-neutral, **fully synthetic** OMOP CDM dataset (1,000 patients; PERSON, VISIT/CONDITION/DRUG/MEASUREMENT/OBSERVATION, plus patient-feature and trajectory tables). Rich maskable fields (birth/death dates, cause of death, socioeconomic and adversity scores) make the masking and small-cell disclosure-control demos concrete. No real participant data.

Loaded via a zero-copy **`clone`** of an in-account reference cohort into `TRE_<NAME>_DB.OMOP_CDM` (fast, same account). (A portable `bundled` Parquet load is a planned follow-up, TRE-3 — not in this release.)

## Phases

- **Phase 1 (this release):** vacuum foundation + edition gate, `SAMPLE_OMOP`, Five Safes policy pack, output airlock, in-perimeter Cortex agent, secure-share demo. Deploy → verify → teardown.
- **Phase 2 (roadmap):** FHIR R4 / EMR ingestion, genomics summaries, real-world-evidence layer, Cortex Search over protocols, MCP server/client for the airlock and agent.
- **Phase 3 (roadmap):** multi-institutional Data Clean Rooms, federated analysis, AI training pipelines, Marketplace publication.

## Requirements

- To **deploy**: `ACCOUNTADMIN`, or a delegated least-privilege `TRE_DEPLOYER` role (mint it once via `seeds/deployer_bootstrap.sql`, then grant it to trusted people so they self-serve without ACCOUNTADMIN).
- To **use** a deployed TRE: just the tier role (`TRE_<NAME>_RESEARCHER` / `_PI` / etc.) — no admin rights.
- For `HIPAA_MODE=true`: **Business Critical** edition (the deploy will detect and **fail loudly** otherwise). Customer-managed keys and PrivateLink recommended.

## Quickstart

Ask your CoCo: *"deploy a TRE with tre-builder."* The `tre-deploy` skill will gather parameters (`TRE_NAME`, `SAMPLE_SOURCE`, `HIPAA_MODE`), run the phased seeds, and verify each phase. To remove it, ask to run `tre-teardown`.

## Layout

```
tre-builder/
  .cortex-plugin/plugin.json
  seeds/    00_vacuum_foundation · 01_edition_gate · 02a_sample_omop_clone ·
            03_five_safes_policies · 04_airlock · 05_agent · 99_teardown
  skills/   tre-deploy · tre-verify · tre-teardown
  agents/   tre-explorer
  docs/     five-safes-satre-conformance
```

## License / data provenance

`SAMPLE_OMOP` is fully synthetic (back-crafted from published aggregate statistics; no real participant records) and safe to redistribute.
