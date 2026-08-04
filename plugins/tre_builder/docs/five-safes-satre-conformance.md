# Five Safes / SATRE Conformance Mapping

How tre-builder implements the **Five Safes** and aligns to **SATRE** (Standard Architecture for Trusted Research Environments, the community reference spec adopted by NHS Secure Data Environments). This is the governance spine: external, credible standards rather than homegrown controls.

## Five Safes -> tre-builder mechanisms

| Safe | Requirement | tre-builder (Phase 1) | Status |
|---|---|---|---|
| **Safe People** | only trusted, trained, authorized researchers | 3-tier RBAC (`ADMIN > PI > RESEARCHER > ANALYST`); roles granted to named users; MFA + network policy expected at account level | Implemented (RBAC); MFA/network policy = operator config |
| **Safe Projects** | access tied to an approved, time-bound purpose | `GOVERNANCE.PROTOCOLS` registry + `PROTOCOL_COHORT`; row access checks `STATUS='ACTIVE' AND EXPIRES_AT > now` | Implemented |
| **Safe Settings** | analysis in a controlled environment; data does not move | zero-copy in-account; row access policies; read-only in-perimeter Cortex agent (no write tools; policies apply to it) | Implemented |
| **Safe Data** | minimized, de-identified, masked | dynamic masking (dates -> year, cause-of-death / SES -> masked) unmasked only for PI/ADMIN; classification tags | Implemented |
| **Safe Outputs** | nothing leaves without disclosure review | the **airlock**: SDC (minimum cell count), dual independent sign-off, append-only audit; secure aggregate share view with small-cell suppression | Implemented (native); human review in the loop |

## The "sixth safe" — automated airlock disclosure control

Modern TRE practice (Genomics England, OpenSAFELY, SACRO) treats output disclosure control as a first-class, semi-automated control. tre-builder's airlock enforces the **automatable** part (minimum cell count, aggregation, dual sign-off, audit) and keeps a **human approver in the loop** for judgment. It does not claim to fully automate output checking — that would be non-conformant and unsafe.

## SATRE alignment (indicative)

- **Information governance** — RBAC, protocol registry, time-bound access, full audit (`AIRLOCK_AUDIT`).
- **Computing technology** — isolated namespaced environment; policy-as-code enforced by the platform regardless of tool/agent.
- **Data management** — classification tags, masking, secure views; minimized shareable surface.
- **Supporting researchers** — in-perimeter NL agent for exploration within policy.

Full SATRE self-assessment (the published checklist) should be completed per deployment; this plugin provides the technical controls that most checklist items map onto.

## Regulated deployment notes

- `HIPAA_MODE=TRUE` requires **Business Critical** edition (customer-managed keys, PrivateLink); the edition gate refuses otherwise.
- Researcher accreditation/onboarding and output-review judgment are **process** controls the plugin supports but does not replace.

## Reconciliation with prior art (INIT-13)

tre-builder generalizes the working City of Hope TRE reference (INIT-13 / `TRE_HEALTHCARE_DB` / `JacinthLaval/tre-healthcare-snowflake`) into a repeatable, brand-neutral, self-service plugin. It reuses the proven patterns (OMOP model, 3-tier RBAC, masking/row-access, in-perimeter agent, secure sharing) rather than re-deriving them; the net-new contribution is the productized, vacuum-deployable, phase-verified packaging plus the native output airlock.
