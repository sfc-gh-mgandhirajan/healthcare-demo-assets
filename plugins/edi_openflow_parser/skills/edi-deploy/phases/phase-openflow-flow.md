---
name: phase-openflow-flow
description: Wire the Openflow flow from source to Snowpipe Streaming
parent_skill: edi-deploy
phase_id: DEPLOY_P2
---

# Phase: Openflow Flow Wiring

## Purpose
Configure the Openflow (NiFi) flow that routes EDI files from source through parsing to typed Snowflake tables.

## Flow Architecture

```
ListS3 → FetchS3 → ParseX12ToJSON → RouteOnAttribute → PutSnowpipeStreaming × N
```

## Processor Configuration

### ListS3
- Bucket: user-specified
- Prefix: user-specified (e.g., `edi/inbound/`)
- Region: match account region

### FetchS3
- Same bucket/credentials as ListS3

### ParseX12ToJSON
- Transaction Type Filter: comma-separated list of supported types (e.g., `834,835,837,278`)
- Output Mode: `ndjson`
- Include Raw Segments: `true`
- Include Envelope: `true`

### RouteOnAttribute
- Route on: `${x12.transaction.types}`
- Routes:
  - `837` → PutSnowpipeStreaming (LANDING_837_CLAIMS)
  - `835` → PutSnowpipeStreaming (LANDING_835_REMITTANCES)
  - `834` → PutSnowpipeStreaming (LANDING_834_ENROLLMENTS)
  - `{new_type}` → PutSnowpipeStreaming (LANDING_{new_type}_{name})
  - unmatched → funnel to error queue or log

### PutSnowpipeStreaming (per table)
- Account: user's account locator
- User: service account with keypair auth
- Database: X12_EDI_AI
- Schema: per transaction type
- Table: per transaction type
- Record Reader: JsonTreeReader
- Authentication: key pair (recommended) or user/password

## Key Considerations

1. **S3 directory objects**: If source has directory placeholder objects (0-byte), add a filter to ListS3 (size > 0)
2. **Do not split the interchange**: Feed each EDI file to ParseX12ToJSON whole. The processor iterates every `ST`/`SE` transaction set in the file and emits one record per boundary segment, so splitting is unnecessary. Splitting on `ST*` is actively harmful: only the first split retains the ISA, and `ParseX12ToJSON` requires it to detect delimiters — every subsequent split raises `Cannot detect X12 delimiters` and routes to `failure`, so all but the first transaction set of each file would be silently lost to the error queue.
3. **Back-pressure**: Configure back-pressure on PutSnowpipeStreaming to prevent overwhelming the streaming channel
4. **Error handling**: Route ParseX12ToJSON failures to a dedicated error queue for investigation

## Output
Provide the user with:
- Processor configuration values for each step
- Required credentials/service account setup
- Suggested flow template (if Openflow API supports import)
