# EDI Openflow Parser — Agent Rules

## Enforcement Rules

These rules apply to all agent interactions within this plugin. They override default behavior.

### Pipeline Integrity

1. **Never modify ParseX12ToJSON.py core parsing logic** — only extend `field_maps.py` (or generate new format-specific parsers)
2. **All format definitions originate from `config/edi_format_specs.yaml`** — never hardcode field mappings in skills or SQL
3. **Landing tables follow the typed VARCHAR pattern** — no VARIANT columns, no DEFAULT values, no IDENTITY columns (Snowpipe Streaming constraint)
4. **NAR builds use `hatch-datavolo-nar` exclusively** — never manual zip/jar packaging
5. **PutSnowpipeStreaming v1** (Record Reader + Table target) — never PutSnowpipeStreaming2 for this architecture

### Deployment Safety

6. **Network policy changes require explicit user confirmation** — show the IP ranges and explain the blast radius before modifying
7. **Openflow runtime must be Medium+ for Python processors** — verify before attempting NAR upload
8. **Test with sample data before production wiring** — always generate and run test stubs in PHASE_TESTS before DEPLOY

### Output Modes

9. **Default to write mode** when user has a detected repo/workspace
10. **Switch to dry-run mode** when:
    - No repo detected in workspace
    - User explicitly requests output-only
    - Running in a read-only or sandboxed environment

### Quality Gates

11. **Every phase requires user confirmation** — never batch multiple phases without stopping for approval
12. **SQL must compile** — run `only_compile=true` on all generated DDL before presenting to user
13. **Field maps must be validated** — if sample data is available, parse it through the generated map and verify field extraction

### Cortex AI Usage

14. **AI enrichment in Gold layer uses `AI_COMPLETE('claude-sonnet-4-6', ...)`** — not `SNOWFLAKE.CORTEX.COMPLETE()`
15. **AI-assisted field map generation** — when user provides an implementation guide, use Cortex AI to propose mappings, but always require human validation

### Competitive Positioning

16. When comparing to alternatives, reference `reference/compete_databricks.md` for accurate positioning
17. Key differentiators: skill-guided extensibility, Openflow streaming, Cortex AI enrichment, config-driven (no code changes for new formats)
