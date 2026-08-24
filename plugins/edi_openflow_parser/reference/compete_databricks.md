# Competitive Positioning: EDI Parsing on Snowflake vs. Databricks

## The Landscape

Healthcare and other industries rely on EDI (Electronic Data Interchange) for system-to-system data exchange. The dominant format is ASC X12 (HIPAA-mandated for healthcare transactions).

Both Snowflake and Databricks offer EDI parsing capabilities, but with fundamentally different approaches.

## Databricks Approach

**Product**: `databricks-industry-solutions/hls-interoperability` (GitHub)

| Aspect | Details |
|--------|---------|
| Architecture | PySpark-based parser, runs in Spark notebooks |
| Formats | 837 (Professional), 835 (Remittance) — finite set |
| Extensibility | Manual Python coding. User must modify parser code directly |
| Streaming | Spark Structured Streaming (Delta Live Tables) |
| AI Enrichment | Manual LLM integration (no native function) |
| Governance | External tooling (Unity Catalog tags, but no inline masking) |
| Skill/Agent Story | None. Pure code repository. |
| Update Path | Fork the repo, merge upstream manually |

### Weaknesses
- Only supports 2 X12 transaction types (837P, 835)
- No guided extension path — adding 278 or 834 requires deep EDI knowledge
- No AI-assisted field map generation
- No configuration-driven approach — everything is hardcoded
- No production deployment automation

## Snowflake Approach (This Plugin)

| Aspect | Details |
|--------|---------|
| Architecture | NiFi Python processor (Openflow) + Python UDF fallback |
| Formats | 7 X12 types pre-built, extensible to any EDI format |
| Extensibility | Config-driven + skill-guided. Add formats via `/edi:extend` |
| Streaming | Openflow + Snowpipe Streaming (sub-second latency) |
| AI Enrichment | Native Cortex AI in Gold Dynamic Tables |
| Governance | Native masking, RLS, classification (one platform) |
| Skill/Agent Story | Full CoCo plugin with guided workflows |
| Update Path | Plugin auto-updates; config stays customer-specific |

### Key Differentiators

1. **Agentic extensibility**: Customer says "I need 278 Prior Auth" → skill generates everything. Databricks: customer writes Python from scratch.

2. **Streaming-first**: Openflow processes in-flight (no landing raw → parse later). Sub-second to table. Databricks: batch or micro-batch.

3. **AI-native enrichment**: `AI_COMPLETE('claude-sonnet-4-6', ...)` in a Dynamic Table. Zero integration code. Databricks: set up LLM endpoints manually.

4. **Config-driven, not code-driven**: New format = new YAML entry. Parser logic unchanged. Databricks: new format = new parser module.

5. **Single platform**: Parse → store → govern → enrich → query. No data movement. Databricks: parse in Spark, land in Delta, govern in Unity, enrich externally.

## Talk Track (30 seconds)

> "Databricks has a basic X12 parser for 837 and 835 — you fork their repo and write Python to extend it. We ship 7 transaction types out of the box, with an AI-assisted skill that generates support for any new format in minutes. Our parser runs in Openflow for sub-second streaming, enriches with Cortex AI natively, and the whole pipeline is config-driven — no code changes to add formats. Plus, governance is built-in. Their approach is last-generation: static code in a notebook. Ours is agentic."

## When Customer Asks "Can You Handle X12?"

1. Start with: "Yes — we support 7 HIPAA transaction types out of the box (834, 835, 837, 270/271, 276/277)."
2. Follow with: "And if you need additional formats, our CoCo skill guides you through adding them in minutes — AI-assisted, no deep EDI expertise required."
3. Close with: "Unlike alternatives that give you a Python script to fork, we give you a production-grade streaming pipeline with AI enrichment and native governance."
