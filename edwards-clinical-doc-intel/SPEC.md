# Edwards Lifesciences: Clinical Document Intelligence Demo

## Spec Version: 1.0 | Created: 2026-04-22

---

## 1. OBJECTIVE

Shatter the perception that Snowflake is only an EDW. Demonstrate end-to-end
unstructured clinical document AI -- from ingestion to governance console --
running entirely on Snowflake, for Edwards Lifesciences executive team.

---

## 2. AUDIENCE

- Edwards Lifesciences Executive Team
- Key personas: VP Clinical Affairs, VP Regulatory, CTO/CIO, Head of Data
- Edwards context: 16K employees, 10+ active structural heart trials, Veeva Vault eTMF since 2015
- Active Snowflake deal: Veeva OpenFlow Connector ($200K ACV, in Tech Validation)

---

## 3. EDWARDS-SPECIFIC CONTEXT

### Product Portfolio (use these names throughout demo)
- **THV**: SAPIEN 3, SAPIEN X4 (next-gen, ALLIANCE trial)
- **TMTT**: PASCAL (mitral/tricuspid repair), EVOQUE (tricuspid replacement), SAPIEN M3 (mitral replacement)
- **New Frontiers**: APTURE shunt (HFpEF), J-Valve (aortic regurgitation)

### Active Trials to Feature in Demo
| Trial | Device | Indication | Status |
|---|---|---|---|
| ALLIANCE | SAPIEN X4 | Aortic stenosis + valve-in-valve | Enrolling |
| ENCIRCLE | SAPIEN M3 | Mitral replacement (failed TEER) | Enrolling (Failed TEER registry) |
| TRISCEND II | EVOQUE | Tricuspid replacement | Enrollment complete, 2-yr data pending |
| ALT-FLOW II | APTURE shunt | HFpEF | Enrolling |
| CLASP IID | PASCAL | Degenerative mitral regurgitation | Enrolling |

### Known Pain Points (from Veeva case study)
- Clinical ops spent excessive time locating documents for audits
- Wrong document versions found during internal audit
- OneTMF initiative created to restructure accountability by functional group
- TMF Governance Board meets biweekly; needs cross-trial visibility

---

## 4. SNOWFLAKE FEATURES USED

| Feature | Purpose | Skill |
|---|---|---|
| Snowflake Stage | Store ingested eTMF documents (PDFs) | N/A (SQL) |
| AI_PARSE_DOCUMENT | Full-text extraction (OCR + LAYOUT mode) for search corpus | `cortex-ai-functions` |
| AI_EXTRACT | Structured field extraction (trial name, PI, site ID, version, dates) | `cortex-ai-functions` |
| Cortex Search Service | Semantic search across parsed document corpus | `search-optimization` |
| Semantic View | Business metrics over structured metadata (Cortex Analyst tool) | `semantic-view` |
| Cortex Agent (object) | Orchestrates search + analyst + custom tools | `cortex-agent` |
| Stored Procedure | Custom tool: audit report generator (Python, calls COMPLETE) | `snowpark-python` |
| SPCS | Hosts React+Flask app as a service | `deploy-to-spcs` |

### Features NOT Demoed Live
- OpenFlow Connector (slide only -- assumed landed)
- Dynamic Tables (skip for v1; could add for incremental doc refresh later)

---

## 5. ARCHITECTURE

```
Veeva Vault eTMF ──(OpenFlow, slide only)──> Snowflake Stage (@etmf_stage)
                                                    │
                                 ┌──────────────────┴──────────────────┐
                                 │                                     │
                        AI_PARSE_DOCUMENT                        AI_EXTRACT
                        (LAYOUT, page_split)                  (responseFormat)
                                 │                                     │
                        PARSED_DOCUMENTS table              DOCUMENT_METADATA table
                        (full markdown text,                (trial_name, doc_type,
                         page content)                       version, PI, site_id,
                                 │                           irb_expiry, amendment)
                                 │                                     │
                        Cortex Search Service              Semantic View
                        (semantic search)                  (Cortex Analyst)
                                 │                                     │
                                 └──────────────┬──────────────────────┘
                                                │
                                    Cortex Agent Object
                                    ├── tool: etmf_search (Cortex Search)
                                    ├── tool: tmf_analytics (Semantic View)
                                    └── tool: audit_report_gen (Stored Proc)
                                                │
                                    ┌───────────┴───────────┐
                                    │   SPCS Service        │
                                    │  ┌─────────────────┐  │
                                    │  │  Flask Backend   │  │
                                    │  │  /api/trials     │  │
                                    │  │  /api/agent/chat │  │
                                    │  │  /api/reports    │  │
                                    │  └────────┬────────┘  │
                                    │  ┌────────┴────────┐  │
                                    │  │  React Frontend  │  │
                                    │  │  Vite + Tailwind │  │
                                    │  │  + shadcn/ui     │  │
                                    │  └─────────────────┘  │
                                    └───────────────────────┘
```

---

## 6. SNOWFLAKE OBJECTS TO CREATE

### Database & Schema
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_EDWARDS_CLINICAL_DOCS   -- main schema (existing DB)
```

### Stages
```
@UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.ETMF_STAGE   -- raw PDFs
```

### Tables
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.PARSED_DOCUMENTS
  - DOC_ID (VARCHAR)
  - RELATIVE_PATH (VARCHAR)
  - TRIAL_NAME (VARCHAR)
  - PARSED_CONTENT (VARIANT)   -- AI_PARSE_DOCUMENT output
  - FULL_TEXT (VARCHAR)         -- extracted markdown text
  - PAGE_COUNT (INT)
  - PARSED_AT (TIMESTAMP)

UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.DOCUMENT_METADATA
  - DOC_ID (VARCHAR)
  - RELATIVE_PATH (VARCHAR)
  - TRIAL_NAME (VARCHAR)
  - DOCUMENT_TYPE (VARCHAR)    -- Protocol, ICF, Regulatory Submission
  - VERSION (VARCHAR)
  - PI_NAME (VARCHAR)
  - SITE_ID (VARCHAR)
  - IRB_EXPIRY_DATE (DATE)
  - AMENDMENT_NUMBER (VARCHAR)
  - FUNCTIONAL_GROUP (VARCHAR) -- Clinical Ops, Data Mgmt, Safety, Statistics, etc.
  - UPLOAD_STATUS (VARCHAR)    -- Current, Outdated, Missing
  - EXTRACTED_AT (TIMESTAMP)
```

### Cortex Search Service
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.ETMF_SEARCH_SERVICE
  - ON: FULL_TEXT
  - ATTRIBUTES: TRIAL_NAME, DOCUMENT_TYPE, SITE_ID, VERSION
  - WAREHOUSE: BI_WH
  - TARGET_LAG: '1 hour'
```

### Semantic View
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.TMF_ANALYTICS_VIEW
  - Base table: DOCUMENT_METADATA
  - Metrics: document_count, completeness_pct, overdue_count, avg_days_since_upload
  - Dimensions: trial_name, document_type, site_id, functional_group, upload_status
  - Time dimensions: irb_expiry_date, extracted_at
```

### Cortex Agent
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.TMF_GOVERNANCE_AGENT
  - Model: auto
  - Tools:
    1. etmf_search → Cortex Search Service (ETMF_SEARCH_SERVICE)
    2. tmf_analytics → Semantic View (TMF_ANALYTICS_VIEW)
    3. audit_report_gen → Stored Procedure (GENERATE_AUDIT_REPORT)
  - Instructions: "You are a TMF Governance Assistant for Edwards Lifesciences
    clinical trials. Help clinical ops teams find documents, track completeness,
    and generate audit readiness reports. Always cite document sources.
    Use formal, regulatory-appropriate language."
  - Sample questions:
    - "What changed between ALLIANCE protocol v4 and v5?"
    - "Which sites have overdue document uploads for ENCIRCLE?"
    - "Generate audit readiness report for TRISCEND II"
    - "Compare inclusion criteria between TRISCEND II and CLASP II TR"
```

### Stored Procedure
```
UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.GENERATE_AUDIT_REPORT(trial_name VARCHAR)
  - Language: Python
  - Runtime: Snowpark
  - Logic:
    1. Query DOCUMENT_METADATA for completeness by TMF zone
    2. Call Cortex Search for recent amendments/key docs
    3. Call CORTEX.COMPLETE to generate formatted audit report
    4. Return markdown report
```

### SPCS Objects
```
IMAGE REPOSITORY: UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.APP_REPO
COMPUTE POOL: EDWARDS_TMF_POOL (GPU_NV_XS or STANDARD_2)
SERVICE: UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.TMF_GOVERNANCE_APP
SPEC FILE: @UNSTRUCTURED_HEALTHDATA.EDWARDS_CLINICAL_DOCS.ETMF_STAGE/app_spec.yaml
```

---

## 7. SYNTHETIC DATA SPEC

### Documents to Generate (PDFs)
| Trial | Doc Type | Versions | Sites | Files |
|---|---|---|---|---|
| ALLIANCE | Protocol | v3.0, v4.0, v5.0 | N/A (master) | 3 |
| ALLIANCE | ICF | v2.0, v3.1 | Site-001 (Cleveland Clinic), Site-002 (Mayo) | 4 |
| ENCIRCLE | Protocol | v2.0, v3.0 | N/A | 2 |
| ENCIRCLE | ICF | v1.0 | Site-003 (Cedars-Sinai), Site-004 (Mt. Sinai) | 2 |
| ENCIRCLE | Regulatory Sub | FDA PMA supplement | N/A | 1 |
| TRISCEND II | Protocol | v4.0, v5.0 | N/A | 2 |
| TRISCEND II | ICF | v3.0 | Site-005 (Mass General), Site-006 (Stanford) | 2 |
| TRISCEND II | Regulatory Sub | CE Mark, Health Canada | N/A | 2 |
| ALT-FLOW II | Protocol | v1.0, v2.0 | N/A | 2 |
| CLASP IID | Protocol | v3.0 | N/A | 1 |
| **Total** | | | | **~21 PDFs** |

### Clinical Terminology to Include
- Aortic stenosis, mitral regurgitation, tricuspid regurgitation
- NYHA Class I-IV, echocardiographic endpoints (LVEF, EROA, regurgitant volume)
- Paravalvular leak, hemolysis, stroke, major bleeding (VARC-3 definitions)
- Heart team, multidisciplinary assessment, TEER, SAVR
- Inclusion/exclusion criteria with specific hemodynamic thresholds

### Metadata Variations for Governance Demo
- Some sites intentionally have OUTDATED ICF versions (shows governance gap)
- Some functional groups have MISSING uploads (shows OneTMF accountability gap)
- IRB expiry dates: some within 30 days (shows amber/red RAG status)

---

## 8. DEMO FLOW (30 minutes)

| Min | Act | Content | Snowflake Feature | Screen |
|---|---|---|---|---|
| 0-2 | Setup | Architecture slide, OpenFlow assumed landed | Slide | N/A |
| 2-8 | Act 1 | AI_PARSE_DOCUMENT + AI_EXTRACT side-by-side on ALLIANCE protocol | AISQL | SQL worksheet or live in app |
| 8-14 | Act 2 | Semantic search across trials in React UI | Cortex Search | Screen: Search |
| 14-22 | Act 3 | Agent chat: cross-tool orchestration | Cortex Agent | Screen: Agent Chat |
| 22-28 | Act 4 | TMF Governance Console walkthrough | React + SPCS | Screens 1-4 |
| 28-30 | Close | "One platform. Not an EDW." | Slide | N/A |

### Screen Specifications

#### Screen 1: Trial Portfolio Overview
- Card grid (5 trial cards: ALLIANCE, ENCIRCLE, TRISCEND II, ALT-FLOW II, CLASP IID)
- Each card: trial name, device name, completeness %, days since last upload, RAG badge
- Data source: `/api/trials` (Snowpark query on DOCUMENT_METADATA)

#### Screen 2: Trial Drilldown
- Left: Document inventory DataTable (sortable, filterable by type/site/group)
- Right: Completeness heatmap (rows=TMF zones, cols=sites, color=completeness %)
- Data source: `/api/trials/{id}/completeness`, `/api/trials/{id}/documents`

#### Screen 3: Agent Chat (slide-out drawer, accessible from any screen)
- Streaming markdown responses via SSE
- Source citations as expandable cards (doc name, page, section)
- SQL results as inline tables (from Cortex Analyst)
- Thinking/reflection steps in collapsible accordion
- Sample question chips at top
- Data source: `POST /api/agent/chat` (Cortex Agent REST API)

#### Screen 4: Audit Report
- One-click "Generate Audit Report" per trial
- Rendered markdown sections: Executive Summary, Completeness by Zone, Missing Docs, Amendment History, Risk Items
- Export-to-PDF button
- Data source: `POST /api/reports/audit` (custom tool stored procedure)

---

## 9. BUILD STEPS & SKILL MAP

| Step | Description | CoCo Skill | Depends On | Est. Effort |
|---|---|---|---|---|
| 1 | Generate synthetic Edwards trial PDFs | `hcls-provider-cdata-clinical-docs` | None | Medium |
| 2 | Upload to stage + AI_PARSE_DOCUMENT + AI_EXTRACT pipeline | `cortex-ai-functions` | Step 1 | Medium |
| 3 | Create Cortex Search Service | `search-optimization` | Step 2 (PARSED_DOCUMENTS table) | Low |
| 4 | Create Semantic View over DOCUMENT_METADATA | `semantic-view` | Step 2 (DOCUMENT_METADATA table) | Low |
| 5 | Create Cortex Agent object with 3 tools | `cortex-agent` | Steps 3 + 4 | Medium |
| 6 | Build audit report stored procedure | `snowpark-python` | Step 2 | Medium |
| 7 | Build React + Flask app | `build-react-app` | Steps 5 + 6 | High |
| 8 | Deploy to SPCS | `deploy-to-spcs` | Step 7 | Medium |

### Parallelization
- Steps 3 + 4 can run in parallel
- Step 6 can run in parallel with Step 5
- Step 7 scaffolding can start while Steps 3-6 complete

---

## 10. TALKING POINTS (for demo presenter)

| Edwards Priority | Demo Proof Point |
|---|---|
| TMF Governance Board efficiency | AI-generated audit readiness reports replace manual doc reviews |
| OneTMF accountability gaps | Real-time completeness scoring by functional group and site |
| 10+ concurrent global trials | Cross-trial semantic search in seconds |
| TMTT portfolio growth (35-45% YoY) | Scale doc infrastructure without new tools |
| Regulatory multi-jurisdiction | Auto-compare submission packages (FDA, CE, Health Canada) |
| "Snowflake = EDW" perception | Zero structured data in Acts 1-3: 100% unstructured doc AI |
| "Could we deploy this?" | "This IS running on Snowflake right now. Same compute, same security." |

---

## 11. SUCCESS CRITERIA

- [ ] 21 synthetic PDFs uploaded to stage
- [ ] AI_PARSE_DOCUMENT successfully extracts text from all PDFs
- [ ] AI_EXTRACT returns structured metadata for all PDFs
- [ ] Cortex Search returns relevant results for 5+ sample queries
- [ ] Semantic View answers completeness/governance questions via Cortex Analyst
- [ ] Cortex Agent routes correctly between search, analyst, and custom tool
- [ ] React app renders all 4 screens with live data
- [ ] SPCS service is READY with public endpoint
- [ ] Full demo runs end-to-end in under 30 minutes
- [ ] Edwards exec leaves thinking "this is NOT just an EDW"
