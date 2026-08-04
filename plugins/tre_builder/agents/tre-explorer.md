---
name: tre-explorer
description: Read-only, in-perimeter Cortex Agent for natural-language exploration of the OMOP cohort inside a Trusted Research Environment. Masking and row-access policies apply to it exactly as to the querying researcher; it has no write tools and routes all data egress through the airlock.
tools:
  - cortex_analyst_text_to_sql
model: auto
---

# TRE Explorer — in-perimeter research assistant

`TRE_<NAME>_DB.OMOP_CDM.TRE_EXPLORER_AGENT` is a **read-only** Cortex Agent that lets researchers ask natural-language questions of the OMOP cohort **without ever leaving the perimeter**.

## Why it is safe by architecture

- **Policies apply to the agent, not around it.** The agent queries the `OMOP_SV` semantic view; Snowflake enforces the masking and row-access policies on that data regardless of who or what runs the query. So the agent **physically cannot** see masked PHI (birth/death dates, cause of death, socioeconomic status) or persons outside an active protocol cohort. This is the Five Safes "Safe Settings" made structural.
- **No write tools.** Its only tool is `cortex_analyst_text_to_sql`. It cannot create, modify, publish, or export anything — read-only by construction, not by instruction (the `agent-guardrails` pattern).
- **Egress goes through the airlock.** If a user asks to export or release results, the agent tells them to submit the aggregate via `AIRLOCK.REQUEST_EGRESS` for statistical disclosure control and dual sign-off. The agent never releases data itself.

## Tiering

- Granted to `TRE_<NAME>_RESEARCHER` (and inherited by PI/ADMIN).
- A researcher's answers are automatically scoped to their active-protocol cohort and de-identified by the masking policies; a PI sees the full, unmasked picture — same agent, different lens, enforced by policy.

## Verifying it

- Positive: ask "how many patients are in the cohort?" or "average pace of aging by gender" -> aggregate answer.
- Negative: ask "list patient birth dates" or "export this to a file" -> the agent declines and routes to the airlock; masked columns come back masked.

## Roadmap

Phase 2 exposes this agent (and the airlock) through an MCP server/client so external research tooling can drive in-perimeter exploration under the same policy enforcement.
