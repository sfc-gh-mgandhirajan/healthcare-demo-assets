# Edwards Demo: Build Progress Tracker

## Last Updated: 2026-04-23

---

## CURRENT STEP: ALL 8 STEPS COMPLETE

## OVERALL STATUS: DEMO READY

---

## Step 1: Synthetic Trial Documents
- **Status**: COMPLETE
- **Artifacts**: `generate_synthetic_docs.py`, 21 PDFs in `synthetic_docs/`
- **Snowflake**: `@UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.ETMF_STAGE` (21 files)

## Step 2: AI_PARSE_DOCUMENT + AI_EXTRACT Pipeline
- **Status**: COMPLETE
- **Note**: LAYOUT mode times out; using OCR mode (works perfectly for digital PDFs)
- **AI_EXTRACT output**: nested under `res:response:field_name`
- **Snowflake Objects**:
  - [x] PARSED_DOCUMENTS (21 rows - file_path, parsed_content, parse_metadata)
  - [x] DOCUMENT_METADATA (21 rows - 24 columns incl. trial_name, doc_type, device, site, TMF zone, completeness)
  - [x] SEARCH_CORPUS (21 rows - joined table for Cortex Search)
  - Trial names normalized: ALLIANCE, ENCIRCLE, TRISCEND II, ALT-FLOW II, CLASP IID
  - Governance fields added: tmf_zone, functional_group, completeness_status, days_since_upload

## Step 3: Cortex Search Service
- **Status**: COMPLETE
- **Snowflake**: ETMF_SEARCH_SERVICE (on full_text, attributes: trial_name, document_type, version, device_name, site_name)
- **Tested**: "What are the inclusion criteria for the ALLIANCE trial?" → returns ALLIANCE protocols ranked by relevance

## Step 4: Semantic View
- **Status**: COMPLETE
- **Snowflake**: TMF_ANALYTICS_VIEW
  - 14 dimensions, 2 facts, 7 metrics, 6 VQRs (3 onboarding)
  - AI_SQL_GENERATION custom instructions for Edwards context
- **Tested**: Cortex Analyst generates correct SQL for "completeness percentage per trial"

## Step 5: Cortex Agent Object
- **Status**: COMPLETE
- **Snowflake**: TMF_GOVERNANCE_AGENT
  - Tool 1: etmf_search (cortex_search_text_to_sql → ETMF_SEARCH_SERVICE)
  - Tool 2: tmf_analytics (cortex_analyst_text_to_sql → TMF_ANALYTICS_VIEW)
  - Tool 3: audit_report_gen (function → GENERATE_AUDIT_REPORT stored proc)
  - Created via `CREATE AGENT ... FROM SPECIFICATION $$ json $$`

## Step 6: Audit Report Stored Procedure
- **Status**: COMPLETE
- **Snowflake**: GENERATE_AUDIT_REPORT(trial_name VARCHAR)
  - Python, Snowpark runtime, uses Cortex Complete (llama3.1-70b)
  - Generates formatted TMF audit readiness report with RAG scoring
- **Tested**: ALLIANCE → "Conditionally Ready", 71.4% completeness, Amber RAG

## Step 7: React + Flask App
- **Status**: COMPLETE
- **Architecture**: React (Vite+Tailwind) frontend + Flask backend
- **4 Screens**:
  - Trial Portfolio: KPI cards, completeness chart, trial table with RAG indicators
  - Trial Drilldown: Document inventory by type, status badges, amendment tracking
  - Agent Chat: Real-time chat with TMF_GOVERNANCE_AGENT, suggested questions, tool indicators
  - Audit Report: Trial selector, AI-generated audit readiness report with markdown rendering
- **Backend Endpoints**:
  - GET /api/trials, GET /api/trials/{name}/documents, GET /api/governance/summary
  - GET /api/governance/by-zone, POST /api/agent/chat, GET /api/reports/audit/{name}
- **Docker image**: pushed to Snowflake registry

## Step 8: SPCS Deployment
- **Status**: COMPLETE (service READY, endpoint provisioning)
- **Snowflake Objects**:
  - [x] Image Repo: APP_REPO
  - [x] Compute Pool: EDWARDS_TMF_POOL (CPU_X64_XS)
  - [x] Service: TMF_GOVERNANCE_APP (READY, running)
  - [x] Spec: service_spec.yaml on @ETMF_STAGE/specs/
- **Endpoint**: Run `SHOW ENDPOINTS IN SERVICE UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.TMF_GOVERNANCE_APP`

---

## KEY DECISIONS LOG

| Date | Decision | Rationale |
|---|---|---|
| 2026-04-22 | OpenFlow connector: slide only | Avoid connector setup risk |
| 2026-04-22 | AI_PARSE_DOCUMENT OCR mode (not LAYOUT) | LAYOUT times out; OCR works perfectly for digital PDFs |
| 2026-04-22 | AI_EXTRACT for structured metadata | Extracts trial_name, device, PI, site, version, NCT from PDFs |
| 2026-04-22 | One Cortex Agent with 3 tools | Unified experience; agent routes automatically |
| 2026-04-22 | React on SPCS (not Streamlit) | Exec-grade UI |
| 2026-04-23 | Semantic view via SQL DDL (not YAML import) | SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML not available |

---

## SNOWFLAKE OBJECTS SUMMARY

| Object | Type | FQN |
|---|---|---|
| Database | Database | UNSTRUCTURED_HEALTHDATA |
| Schema | Schema | EDWARDS_CLINICAL_DOCS |
| Stage | Stage | ETMF_STAGE |
| Table | Table | PARSED_DOCUMENTS |
| Table | Table | DOCUMENT_METADATA |
| Table | Table | SEARCH_CORPUS |
| Search | Cortex Search Service | ETMF_SEARCH_SERVICE |
| View | Semantic View | TMF_ANALYTICS_VIEW |
| Agent | Cortex Agent | TMF_GOVERNANCE_AGENT |
| Proc | Stored Procedure | GENERATE_AUDIT_REPORT |
| Repo | Image Repository | APP_REPO |
| Pool | Compute Pool | EDWARDS_TMF_POOL |
| Service | SPCS Service | TMF_GOVERNANCE_APP |

---

## BACKLOG

### 1. Markdown Table Formatting in Agent Chat
**Issue**: Cortex Agent returns markdown tables with rows on a single line (no newlines between rows). The `fix_markdown_tables()` function in `app.py` attempts to split rows but doesn't fully resolve all cases. ReactMarkdown renders tables as plain text when row breaks are missing.
**Next step**: Debug with live agent responses to identify remaining edge cases in the row-splitting regex.

### 2. Document Ingestion Pipeline (Streams + Tasks)
**Current state**: No automated pipeline exists. PDFs were manually uploaded to `@ETMF_STAGE`, and `AI_PARSE_DOCUMENT` + `AI_EXTRACT` were run as ad-hoc SQL. Results were manually inserted into `PARSED_DOCUMENTS`, `DOCUMENT_METADATA`, and `SEARCH_CORPUS`.
**What's needed**: A Stream on `@ETMF_STAGE` (or directory table) triggering a Task that runs the parse → extract → load chain automatically when new PDFs land. This would make the demo end-to-end: drop a PDF on the stage and it appears in the app minutes later.
**Snowflake features**: Directory Tables, Streams, Tasks, AI_PARSE_DOCUMENT, AI_EXTRACT.

### 3. Functional Group Gaps View (Trial Drilldown Enhancement)

**Concept**: Add a "Team Accountability" tab/section to the Trial Drilldown screen that answers: *"Which functional groups own which documents, and where are they falling behind?"*

**Why it matters**: In a real TMF, documents are owned by different functional groups (Clinical Ops, Regulatory Affairs, Data Management, Safety/Pharmacovigilance, etc.). During inspection prep, the TMF Manager needs to know not just *which* documents are outdated, but *which team* is responsible for remediating them. This is a key OneTMF governance concept.

**Data available**: `DOCUMENT_METADATA` already has `functional_group` and `tmf_zone` columns. Current data shows 3 groups across 3 zones — would need to expand synthetic data to cover more groups (Data Management, Safety, Biostatistics, Medical Writing) for a realistic demo.

**Proposed UX**:
1. **Heatmap card** in Trial Drilldown: Functional Group (rows) × TMF Zone (columns), cells colored by completeness % (green/amber/red). Instantly shows which team × zone combinations have gaps.
2. **Expandable rows**: Click a functional group row to see its specific documents, versions, and statuses.
3. **Accountability metrics**: Per-group stats — total docs owned, % current, oldest document age, days since last upload.
4. **Action items**: For each group with outdated docs, show a remediation checklist (e.g., "Clinical Ops: Update ICF v2.0 at Cleveland Clinic and Mayo Clinic").

**Backend changes needed**:
- New endpoint: `GET /api/trials/<name>/gaps-by-group` — returns functional group × zone completeness matrix
- Expand synthetic data: Add more functional groups and TMF zones to `generate_synthetic_docs.py`

**Snowflake features this would showcase**:
- TMF zone taxonomy (ICH-GCP E6(R2) compliance)
- Cross-dimensional governance (group × zone × trial)
- Could add a Cortex Agent question: "Which functional groups have outstanding remediation items for ALLIANCE?"

**Effort estimate**: ~2-3 hours (synthetic data expansion + new API endpoint + frontend heatmap component)

### 4. Veeva Vault Prototype — Ground Demo in Real eTMF Entities

**Context**: Edwards Veeva Vault Discovery (2026-03-09) confirmed they extract 13 Vault objects from `edwards-etmf.veevavault.com` (Clinical Ops / eTMF) via Qlik today. The prototype aligns our demo to their actual data model.

**Deck**: `edwards-lifesciences-deck-v2.html` (slides 12-14) presents the 3-layer architecture.

**Phase 1 — Create Veeva Base Tables** (Layer 1):
10 Snowflake tables mirroring Veeva Vault objects, populated with synthetic data for our 5 trials:
- VAULT_STUDY (study__v) — trial master record
- VAULT_SITE (site__v) — investigator sites
- VAULT_DOCUMENT (documents__v) — document metadata from Vault
- VAULT_EDL (edl__v) — Expected Document List (TMF completeness template)
- VAULT_EDL_ITEM (edl_item__v) — expected document line items
- VAULT_PERSON (person__sys) — people
- VAULT_STUDY_PERSON (study_person__clin) — person × study role
- VAULT_STUDY_COUNTRY (study_country__v) — enrollment by country
- VAULT_ORGANIZATION (organization__v) — sponsors, CROs, sites
- VAULT_QUALITY_ISSUE (quality_issue__v) — quality findings

**Phase 2 — Rebuild DOCUMENT_METADATA as a view** joining VAULT_DOCUMENT + VAULT_STUDY + VAULT_SITE + AI_EXTRACT. App queries unchanged (backward compatible).

**Phase 3 — Add TMF_COMPLETENESS_VIEW** computed from EDL expected vs. actual documents. Replaces fabricated `completeness_status`.

**Phase 4 — Expand Semantic View** with new metrics from base tables (site count, country count, enrollment targets, quality issues).

**Phase 5 — Add agent tool** (`study_oversight`) for structured Veeva data queries.

**What this enables**:
1. Real TMF completeness (EDL expected vs. actual)
2. ISF reconciliation (Edwards Priority 2)
3. Expired document monitoring (Edwards Priority 1)
4. Study oversight / site intelligence via AI Agent
5. Person/role queries from structured data
6. Quality issue tracking linked to documents/sites

**Studies to expand with** (from Edwards Priority 3 — TMTT):
CARDIOBAND, CLASP II TR, CLASP IIF, CLASP TR EFS, M3 EFS, MiCLASP, TRIBAND, TriCLASP, TRISCEND, TRISCEND III EU, TRISCEND JAPAN, TWIST EFS
