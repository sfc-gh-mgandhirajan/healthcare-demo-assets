---
name: edi-deploy
description: Orchestrates building and deploying the EDI parsing pipeline (Openflow or Python UDF)
parent_skill: edi-router
tools: [snowflake_sql_execute, ask_user_question, read, write, glob, grep, bash]
---

# EDI Deploy — Pipeline Deployment Orchestrator

You orchestrate the deployment of the EDI parsing pipeline. Supports two paths:

## Path Selection

Ask the user which deployment path they want:

```
Which deployment path would you like?

A) Openflow (Primary) — Streaming ingestion via NiFi NAR processor
   - Best for: production, high-volume, continuous streaming
   - Requires: Openflow runtime (Medium+), S3/SFTP source

B) Batch (planned) — Python stored procedure + task
   - Status: Not yet implemented. Coming in a future release.
```

## Openflow Path

### Phase 0: Infrastructure DDL
Before wiring Openflow, ensure the target database and landing tables exist.
Execute the SQL scripts in order:
1. `sql/00_prerequisites.sql` — creates database and warehouse
2. `sql/01_landing_tables.sql` — creates typed landing tables per transaction type
3. `sql/02_gold_layer.sql` — creates Gold Dynamic Tables with AI enrichment

Run these via `snowflake_sql_execute`. If tables already exist (IF NOT EXISTS), this is a no-op.

### Gate: Runtime Verification (`gates/gate-runtime.md`)
- Verify Openflow deployment exists
- Confirm runtime is Medium+ (required for Python processors)
- Check NAR extension slot availability

### Phase 1: NAR Build (`phases/phase-openflow-nar.md`)
- `hatch build --target nar` in backbone repo
- Verify NAR file produced (should be ~900KB+)
- Output NAR file path

### Phase 2: Flow Wiring (`phases/phase-openflow-flow.md`)
- Configure: ListS3 → FetchS3 → ParseX12ToJSON → RouteOnAttribute → PutSnowpipeStreaming
- Do NOT insert SplitContent: the processor parses a whole interchange and emits one record per boundary segment. Splitting on `ST*` strips the ISA from every split after the first, which then fails delimiter detection and routes to `failure`.
- RouteOnAttribute routing rules based on `x12.transaction.types`
- One PutSnowpipeStreaming per transaction type (targets different tables)

### Phase 3: Network Policy (`phases/phase-network.md`)
- Identify SPCS container IPs for the Openflow runtime
- Verify they're in the account's network policy allow-list
- If not: show the ALTER NETWORK POLICY statement, require explicit confirmation

## Post-Deployment

After deployment completes:
1. Run a smoke test (small file → verify landing table populated)
2. Update `.deployment/manifest.json` with deployment metadata
3. Suggest: "Run /edi:status to monitor pipeline health"
