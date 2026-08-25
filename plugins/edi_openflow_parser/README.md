# X12 EDI Parser for Snowflake

## Table of Contents

- [What is this?](#what-is-this)
- [Why this matters in Healthcare](#why-this-matters-in-healthcare)
- [How it works](#how-it-works)
- [How to Deploy](#how-to-deploy)
- [Getting Started Prompts](#getting-started-prompts)
- [What's in the Plugin](#whats-in-the-plugin)
- [Snowflake Features Used](#snowflake-features-used)
- [Transaction Types Supported](#transaction-types-supported)
- [Security and Governance](#security-and-governance)

---

## What is this?

X12 EDI Parser is a Snowflake-native solution that transforms ASC X12 HIPAA transaction files into structured, queryable relational data — automatically, continuously, and with config-driven extensibility.

It takes X12 transaction files (claims, enrollment, remittances, eligibility, authorizations) and:
1. **Parses** them in-flight via Openflow (NiFi) or batch via Python stored procedure
2. **Routes** each transaction type to its own typed landing table
3. **Enriches** with Cortex AI in Gold Dynamic Tables (ICD-10 decoding, procedure classification)
4. **Extends** to any new X12 transaction type via an AI-assisted CoCo skill — no deep EDI expertise required

---

## Why this matters in Healthcare

### The Problem

Healthcare organizations exchange billions of EDI transactions annually — claims (837), remittances (835), enrollment (834), eligibility checks (270/271), prior authorizations (278). This data is:

- **Trapped in non-relational formats** — X12 segment/element structure can't be queried with standard SQL
- **Requires specialized parsers** — each transaction type has different record boundaries, qualifiers, and field positions
- **Brittle to extend** — adding a new X12 transaction type means hiring EDI consultants or writing custom code from scratch
- **Disconnected from analytics** — most organizations can't join X12 data with their clinical or financial data estate

Custom parser development typically costs $50-200K per transaction type and takes 4-12 weeks of specialized developer time.

### The Solution

This plugin replaces custom EDI development with a config-driven framework:

| Metric | Custom Development | This Plugin |
|--------|-------------------|-------------|
| Cost per new format | $50-200K | ~$0 (config + AI-guided) |
| Time to add format | 4-12 weeks | Minutes (interactive skill) |
| EDI expertise required | Deep (X12 segment specs) | None (AI proposes mappings) |
| Streaming support | Custom integration | Built-in (Openflow) |
| AI enrichment | Manual LLM wiring | Native (Cortex AI in DTs) |
| Maintenance | Per-parser codebase | Single config file |

### Use Cases

| Use Case | Who Benefits | What They Get |
|----------|-------------|---------------|
| **Claims Processing** | Revenue cycle teams | Real-time claim status, payment reconciliation, denial analytics |
| **Eligibility Verification** | Patient access | Instant benefit/coverage queries joined with clinical data |
| **Enrollment Management** | Member services | Automated enrollment processing, coverage gap detection |
| **Prior Authorization** | Utilization management | Auth tracking, approval rate analytics, turnaround time |
| **Remittance Reconciliation** | Finance teams | Payment-to-claim matching, adjustment analysis, underpayment detection |
| **Interoperability Compliance** | IT/Compliance | Centralized EDI processing meeting HIPAA transaction standards |

---

## How it works

```
EDI Files (S3/SFTP/Stage)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  Ingestion (choose one path)                            │
│                                                         │
│  Path A: Openflow (streaming, production)               │
│  ListS3 → FetchS3 → ParseX12ToJSON                      │
│  → RouteOnAttribute → PutSnowpipeStreaming × N tables   │
│                                                         │
│  Path B: Python UDF Lite (batch, PoC)                   │
│  Snowpipe → RAW_EDI → Task → parse_edi() stored proc   │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  Landing Tables (typed VARCHAR, per transaction type)    │
│                                                         │
│  CLAIMS.LANDING_837_CLAIMS                              │
│  ENROLLMENTS.LANDING_834_ENROLLMENTS                    │
│  REMITTANCES.LANDING_835_REMITTANCES                    │
│  ELIGIBILITY.LANDING_270_INQUIRIES                      │
│  ... (one per transaction type)                         │
└────────────────────────┬────────────────────────────────┘
                         │ Dynamic Tables (10 min lag)
                         ▼
┌─────────────────────────────────────────────────────────┐
│  Gold Layer (type casting + Cortex AI enrichment)       │
│                                                         │
│  GOLD.GOLD_CLAIMS — ICD-10 decode, chronic flag        │
│  GOLD.GOLD_ENROLLMENTS — date normalization            │
│  GOLD.GOLD_REMITTANCES — adjustment totals             │
└─────────────────────────────────────────────────────────┘
```

### Key Architecture Decisions

- **Config-driven extensibility** — Adding a new transaction type = add a YAML entry + run `/edi:extend`, no parser code changes
- **Parsing in-flight (Openflow)** — Transforms happen before data lands in Snowflake, sub-second to table
- **All-VARCHAR landing tables** — Snowpipe Streaming compatibility; type casting deferred to Gold layer
- **Qualifier-aware field mapping** — Same segment (e.g., NM1) maps differently based on qualifier (85=billing, IL=subscriber)
- **AI enrichment at Gold layer only** — Landing is fast/cheap; enrichment is optional and configurable per type
- **Three-layer enforcement** — AGENTS.md + execution_contract + PreToolUse hooks prevent architectural shortcuts

---

## How to Deploy

### Install the Plugin

```bash
cortex plugin install Snowflake-Solutions/sf-hcls-solutions-incubator --plugin edi_openflow_parser
```

Or find it in the **HCLS Industry Skills** profile in Cortex Code.

### Prerequisites

- Snowflake account in a Cortex AI-supported region
- Role with CREATE DATABASE, CREATE SCHEMA, CREATE TABLE, CREATE DYNAMIC TABLE
- Warehouse MEDIUM or larger
- For Openflow path: Openflow runtime (Medium+ for Python processors)
- For Python UDF path: Internal stage with EDI files

### Deployment (Guided by CoCo)

Run the extend command to add your transaction types:

```
/edi:extend
```

CoCo guides you through gates and phases interactively:

1. **Gate 1: Connection** — Verifies Snowflake account, database, Cortex AI access
2. **Gate 2: Source Detection** — Locates plugin source in workspace, sets output mode
3. **Gate 3: Format Specification** — Gathers what format to add (from impl guide, AI inference, or manual)
4. **Phase 1: Field Map** — Generates segment-to-field mappings with AI assistance
5. **Phase 2: Landing DDL** — Generates typed landing table (compile-checked)
6. **Phase 3: Gold DT** — Generates Dynamic Table with casts + optional AI enrichment
7. **Phase 4: Tests** — Generates pytest stubs and sample EDI data

Each phase requires your approval before proceeding.

### Post-Deployment

```
/edi:extend       # Add or customize an X12 transaction format
/edi:deploy       # Build NAR + wire Openflow, or deploy Python UDF
/edi:status       # Check pipeline health (DT refresh, row counts, errors)
```

---

## Getting Started Prompts

Copy-paste these into Cortex Code to get started:

### First-Time Setup

```
/edi:extend

I need to process X12 837 Professional claims from our clearinghouse.
Set up the database, landing tables, and Gold layer with ICD-10 enrichment.
```

### Add a New Transaction Type

```
/edi:extend

Add support for X12 278 Prior Authorization transactions. I don't have
an implementation guide — use AI to propose the field mappings based on
the standard. We need to track authorization numbers, decision codes,
and approved quantities.
```

### Deploy the Streaming Pipeline

```
/edi:deploy

Deploy the Openflow streaming path. My EDI files land in S3 bucket
"my-company-edi-inbound" under the prefix "x12/claims/". I already
have an Openflow runtime called "prod_runtime".
```

### Deploy Without Openflow

```
/edi:deploy

I don't have Openflow. Deploy the Python UDF lite path so I can
process X12 files from an internal stage on a 5-minute schedule.
```

### Check Pipeline Health

```
/edi:status

Show me the current state of my EDI pipeline — row counts, DT refresh
status, and any errors in the last 24 hours.
```

### Customize an Existing Type

```
/edi:extend

I need to add fields to the existing 835 remittance parser. Our payer
sends a PLB (Provider Level Balance) segment that we're not capturing.
Add fields for PLB adjustment reason and amount.
```

### Explore What's Supported

```
What X12 transaction types does this plugin support? Show me the field
maps for 837 claims — I want to see what fields are being extracted.
```

---

## What's in the Plugin

| Component | Purpose |
|-----------|---------|
| `AGENTS.md` | Enforcement rules — prevents architectural shortcuts |
| `plugin.json` | Manifest with execution_contract + hook definitions |
| `execution_contract.json` | Quality gates, prohibited patterns, pipeline order |
| `hooks/enforce-contract.sh` | Hard-blocks unsafe DDL patterns (PreToolUse) |
| `commands/` | Slash commands: extend, deploy, status |
| `config/edi_format_specs.yaml` | X12 format configuration and deployment defaults |
| `config/x12_known_types.yaml` | Pre-built transaction type field maps (authoritative source) |
| `src/x12_processors/` | Bundled parsing engine (NiFi processor + field maps) |
| `sql/` | Infrastructure DDL (prerequisites, landing tables, Gold DTs) |
| `tests/` | Unit tests + sample X12 files + demo walkthrough |

### Composed Skills (Domain Logic)

| Skill | What It Does |
|-------|-------------|
| `edi-router` | Intent routing — classifies extend/deploy/status and loads sub-skill |
| `edi-extend` | 3-gate, 4-phase pipeline: validate → field map → DDL → Gold DT → tests |
| `edi-deploy` | Dual-path deployment: Openflow NAR build + wiring, or Python UDF + Task |
| `edi-status` | Pipeline health monitoring: DT refresh, row counts, streaming lag, errors |

---

## Snowflake Features Used

| Feature | How It's Used |
|---------|---------------|
| **Openflow (NiFi on SPCS)** | Streaming ingestion: parse X12 in-flight via custom Python processor |
| **Snowpipe Streaming** | Sub-second landing into typed tables from Openflow |
| **Dynamic Tables** | Gold layer: automatic refresh with type casting + AI enrichment |
| **Cortex AI (AI_COMPLETE)** | ICD-10 code decoding, procedure classification, urgency scoring |
| **Python Stored Procedures** | Lite path: parse_edi() for batch processing without Openflow |
| **Tasks** | Scheduled parsing (lite path): process new files every 5 minutes |
| **Snowpipe** | Auto-ingest from stage to RAW_EDI table (lite path) |
| **Stages** | Source file storage for batch processing |
| **Network Policies** | SPCS container IP allow-listing for Openflow auth |

---

## Transaction Types Supported

### Deployed (landing tables + Gold DTs included)

| Code | Name | Key Fields Extracted | Example Use |
|------|------|---------------------|-------------|
| 837 | Health Care Claim | Claim ID, diagnosis codes, procedures, providers, amounts, dates | Claim analytics, denial management |
| 835 | Claim Payment/Remittance | Claim ID, payment amount, adjustments, service lines | Payment reconciliation, underpayment detection |
| 834 | Benefit Enrollment | Member ID, demographics, coverage dates, plan info | Enrollment tracking, coverage gap analysis |

### Parseable (field maps included, extend with `/edi:extend` to generate DDL)

| Code | Name | Key Fields Extracted | Example Use |
|------|------|---------------------|-------------|
| 270 | Eligibility Inquiry | Subscriber, provider, service type, date range | Eligibility verification automation |
| 271 | Eligibility Response | Benefits, coverage levels, copays, deductibles | Real-time benefit display |
| 276 | Claim Status Request | Trace number, claim reference, service dates | Claim follow-up automation |
| 277 | Claim Status Response | Status codes, action codes, payment amounts | Denial tracking, appeal prioritization |

Adding a custom type: run `/edi:extend` to generate landing tables, Gold DTs, and tests. Or edit `config/x12_known_types.yaml` directly.

---

## Security and Governance

| Control | Implementation |
|---------|---------------|
| Execution contract | Strict mode — prohibited patterns enforced at tool-call level |
| PreToolUse hook | Blocks ad-hoc DDL, manual NAR packaging, network policy changes without confirmation |
| Network policy verification | SPCS container IPs validated before deployment (prevents account lockout) |
| All-VARCHAR landing | No implicit type coercion at ingest — prevents data loss from casting errors |
| Quality gates | Every generated artifact compile-checked before user sees it |
| Phase-gated workflow | User must approve each generation step — no batch execution |

---

## License

Apache-2.0
