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
