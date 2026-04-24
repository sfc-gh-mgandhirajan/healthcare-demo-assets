---
name: hcls-provider-imaging
description: "**[REQUIRED]** Use for ALL DICOM medical imaging tasks on Snowflake. This is the entry point for healthcare imaging solutions combining platform skills with clinical imaging workflows. Triggers: DICOM, medical imaging, radiology, imaging pipeline, PACS, imaging viewer, imaging AI, imaging governance, HIPAA imaging, clinical images, pathology images, imaging metadata, imaging ML, imaging model, imaging analytics, healthcare imaging, imaging data lake, imaging FHIR, imaging study, imaging series, DICOM schema reference, Cortex Agent imaging, conversational imaging, natural language query imaging, cohort analysis, patient cohort, cross-modality cohort, imaging cohort, React imaging app, imaging dashboard, cohort dashboard, imaging UI, equipment utilization, semantic view imaging."
platform_affinities:
  produces: [tables, views, dynamic_tables, streams, tasks, stages, masking_policies, cortex_search_service, cortex_agent, semantic_view, ml_models]
  benefits_from:
    - skill: dynamic-tables
      when: "incremental refresh needed for ongoing DICOM ingestion feeds"
    - skill: data-governance
      when: "imaging tables contain PHI (patient name, ID, birth date, referring physician)"
    - skill: developing-with-streamlit
      when: "user wants a Streamlit-based imaging metadata dashboard"
    - skill: deploy-to-spcs
      when: "user needs a pixel-level DICOM viewer deployed as a container service"
    - skill: machine-learning
      when: "user wants to train or deploy radiology AI models"
    - skill: search-optimization
      when: "user needs full-text search over radiology reports or imaging metadata"
    - skill: cortex-agent
      when: "user wants natural-language querying of imaging data via conversational agent"
    - skill: semantic-view
      when: "user needs self-service analytics over imaging tables via text-to-SQL"
---

# Healthcare Imaging Solutions on Snowflake

## Environment Configuration (REQUIRED — Run First)

Before any routing, collect the target Snowflake environment from the user. These values are used by **all sub-skills** and **all reference SQL files** as `{database}`, `{schema}`, `{warehouse}`.

### Step: Confirm Environment

1. **Detect current context:**
```sql
SELECT CURRENT_DATABASE() AS current_db, CURRENT_SCHEMA() AS current_schema, CURRENT_WAREHOUSE() AS current_wh;
```

2. **Ask the user** to confirm or override. Use `ask_user_question` with detected values as defaults:

| Parameter | Placeholder | Description | Default |
|-----------|-------------|-------------|---------|
| Database | `{database}` | Target database for all imaging objects | Detected or `UNSTRUCTURED_HEALTHDATA` |
| Schema | `{schema}` | Target schema for imaging tables, DTs, views, policies | Detected or `DICOM_IMAGING` |
| Warehouse | `{warehouse}` | Warehouse for DTs, Cortex Search, tasks, queries | Detected or current warehouse |

**MANDATORY STOPPING POINT:** Do NOT proceed to intent routing until the user confirms `{database}`, `{schema}`, and `{warehouse}`.

### Substitution Rule

All SQL in sub-skills and `references/*.sql` files uses these placeholders:
- `{database}` — e.g., `UNSTRUCTURED_HEALTHDATA`
- `{schema}` — e.g., `DICOM_IMAGING`
- `{warehouse}` — e.g., `BI_WH`
- `{database}.{schema}` — fully qualified schema path

**Before executing any SQL**, substitute all `{placeholder}` tokens with the confirmed values. Sub-skills MUST NOT assume or hardcode any database, schema, or warehouse name.

## Setup

1. **Load** `references/dicom-standards.md` for DICOM domain context
2. **Confirm Environment** (above) — collect `{database}`, `{schema}`, `{warehouse}`
3. **Proceed to Intent Detection**

## Data Model Grounding Rule (CRITICAL)

**All sub-skills MUST read `dicom-parser/references/data_model_ddl.sql` for column definitions, data types, constraints, and relationships.** Do NOT generate DDL or column lists from LLM memory. The DDL file is the contract — if a column is not in the file, it does not exist.

For schema questions or data model references, route to `data-model-knowledge/SKILL.md` which directs to the authoritative DDL file.

## Schema Reference

All DICOM data model DDLs, table definitions, and column metadata live as SQL reference files within the sub-skills that own them:

| Schema Concern | Authoritative Source |
|----------------|---------------------|
| 19-table DICOM data model (DDLs) | `dicom-parser/references/data_model_ddl.sql` |
| Data model knowledge / schema reference | `data-model-knowledge/SKILL.md` → reads DDL file |
| Validation & quality queries | `dicom-parser/references/validation_queries.sql` |
| Analytics Dynamic Tables | `dicom-analytics/references/dynamic_tables.sql` |
| Cortex Search, Semantic View, Agent | `dicom-analytics/references/cortex_services.sql` |
| Ad-hoc query templates | `dicom-analytics/references/query_templates.sql` |
| Ingestion stages, raw table | `dicom-ingestion/references/stage_and_raw.sql` |
| Cascading ingestion DTs | `dicom-ingestion/references/cascading_dynamic_tables.sql` |
| Stream/Task/Snowpipe pipelines | `dicom-ingestion/references/stream_task_snowpipe.sql` |
| Governance tags, masking, RAPs | `imaging-governance/references/policies_and_tags.sql` |
| Audit, de-identification, retention | `imaging-governance/references/audit_deidentification_retention.sql` |

Sub-skills read DDLs and column definitions directly from these reference SQL files. The `data_model_ddl.sql` file is the single source of truth for the 19-table DICOM data model.

## Intent Detection

| Intent | Triggers | Load |
|--------|----------|------|
| PARSE | "parse DICOM", "extract DICOM tags", "DICOM schema", "DICOM data model", "pydicom", "DICOM to Snowflake", "build DICOM tables" | `dicom-parser/SKILL.md` |
| INGEST | "ingest DICOM", "imaging pipeline", "load images", "PACS integration", "stage DICOM", "stream images", "dynamic table imaging" | `dicom-ingestion/SKILL.md` |
| ANALYTICS | "imaging analytics", "metadata extraction", "imaging search", "Cortex search imaging", "study analytics", "radiology NLP", "report extraction", "equipment utilization", "temporal trends", "quality scoring" | `dicom-analytics/SKILL.md` |
| COHORT | "cohort analysis", "patient cohort", "cross-modality cohort", "imaging cohort", "longitudinal cohort", "modality combination patients" | `dicom-analytics/SKILL.md` (Step 2) |
| AGENT | "Cortex Agent", "natural language query", "conversational imaging", "ask about imaging data", "chat with imaging data", "imaging assistant" | `dicom-analytics/SKILL.md` (Steps 8-9) + `cortex-agent` skill |
| VIEWER | "React imaging app", "imaging dashboard", "cohort dashboard", "imaging UI", "DICOM viewer", "radiology UI", "deploy viewer" | `imaging-viewer/SKILL.md` |
| GOVERNANCE | "imaging governance", "HIPAA", "PHI masking", "imaging audit", "imaging classification", "imaging access policy", "de-identification" | `imaging-governance/SKILL.md` |
| ML | "imaging model", "train imaging", "imaging classification ML", "pathology model", "radiology AI", "deploy imaging model", "imaging inference" | `imaging-ml/SKILL.md` |
| MODEL_KNOWLEDGE | "DICOM schema reference", "data model reference", "what columns does DICOM have", "DICOM table structure", "generate DDL" | `data-model-knowledge/SKILL.md` |

### Intent Disambiguation

- **COHORT vs ANALYTICS**: If user mentions "cohort" explicitly (patient cohort, cross-modality cohort, imaging cohort), route to COHORT which jumps directly to `dicom-analytics/SKILL.md` Step 2. If the request is general analytics without the word "cohort", route to ANALYTICS which presents the full Step 1 menu.
- **AGENT vs ANALYTICS**: If user wants conversational/natural-language access ("ask about imaging data", "chat with my imaging tables"), route to AGENT. If user wants to build analytics objects (Dynamic Tables, views, search services), route to ANALYTICS.
- **Equipment utilization**: Routes to ANALYTICS. For deep-dive equipment analysis (scanner downtime, comparative performance, throughput by hour), `dicom-analytics/SKILL.md` Step 3 handles this. For surface-level "how many studies per scanner", ANALYTICS Step 1 menu covers it.
- **VIEWER**: Always targets React-based frontend via `imaging-viewer/SKILL.md`. For Streamlit metadata dashboards, route to ANALYTICS and invoke `developing-with-streamlit` skill. For pixel-level DICOM rendering via SPCS, VIEWER handles this through `deploy-to-spcs`.
- **DICOM schema / data model questions**: Route to PARSE. The `dicom-parser` sub-skill owns the canonical 19-table data model DDLs in `references/data_model_ddl.sql`.

**Short-prompt disambiguation:** If the user prompt is 3 words or fewer (e.g., "DICOM analytics", "imaging help") and matches only broad triggers, present the full intent table and ask: "What specifically would you like to do?" before routing. Do not assume the most common intent for short/vague prompts.

MANDATORY STOPPING POINT: If intent is ambiguous, multiple intents are detected, or the prompt is very short/vague, present the detected intents to the user and confirm before routing.

## Multi-Intent Handling

When a user request spans multiple intents (e.g., "ingest DICOM files, build cohort analytics, and set up a governance layer"):

1. **Parse** the request to identify ALL matching intents from the table above
2. **Present** detected intents to user for confirmation
3. **Execute** in dependency order: PARSE -> INGEST -> ANALYTICS/COHORT -> GOVERNANCE -> AGENT -> VIEWER/ML
4. **Chain outputs**: each sub-skill receives the outputs of prior sub-skills as context
5. **Stop** between each intent — confirm completion of current before starting next

### Dependency Order

| Dependency | Reason |
|-----------|--------|
| PARSE before INGEST | Tables must exist before pipelines load data |
| INGEST before ANALYTICS/COHORT | Data must be loaded before analytics views are built |
| ANALYTICS before AGENT | Semantic View and Cortex Search must exist before Agent can use them as tools |
| ANALYTICS before VIEWER | Dashboard needs analytics objects (DTs, views) as data sources |
| GOVERNANCE parallel with ANALYTICS | Masking policies are independent of analytics views |
| AGENT after ANALYTICS | Agent requires Semantic View (Step 8) and Cortex Search Service (Step 7) as tools |
| VIEWER last | UI layer consumes all upstream objects |

### Output Chaining

When chaining intents, pass these outputs forward:

| From | To | What Gets Passed |
|------|----|-----------------|
| PARSE | INGEST | Table names, column definitions, DDL |
| INGEST | ANALYTICS | Populated table names, row counts, schema |
| INGEST | GOVERNANCE | Table names with PHI columns identified |
| ANALYTICS | AGENT | Semantic View name, Cortex Search Service name |
| ANALYTICS | VIEWER | Dynamic Table names, view names for dashboard data sources |
| GOVERNANCE | VIEWER | Masking policy names (for role-aware UI) |

MANDATORY STOPPING POINT: Between each intent in a multi-intent chain, confirm the prior intent completed successfully. Present created objects and ask user to proceed.

## Intent-Specific Routing Instructions

### PARSE Routing
1. **Load** `dicom-parser/SKILL.md`
2. Sub-skill owns the 19-table DICOM data model DDLs in `references/data_model_ddl.sql`

### INGEST Routing
1. **Load** `dicom-ingestion/SKILL.md`
2. Sub-skill builds pipelines (COPY INTO, Dynamic Tables, Streams/Tasks) using DDLs from `dicom-parser/references/data_model_ddl.sql` for column mappings

### ANALYTICS Routing
1. **Load** `dicom-analytics/SKILL.md`
2. Sub-skill presents full analytics menu (Step 1) covering cohorts, equipment, temporal, quality, NLP, search, semantic view, agent

### COHORT Routing
1. **Load** `dicom-analytics/SKILL.md` and **jump directly to Step 2** (Cross-Modality Cohort Analysis)
2. Skip the Step 1 menu — user intent is already specific

### AGENT Routing
1. **Check prerequisites**: Semantic View (`DICOM_ANALYTICS_SV`) and Cortex Search Service (`IMAGING_SEARCH_SVC`) must exist
2. If prerequisites missing: route to ANALYTICS first (Steps 7-8) to create them, then return to AGENT
3. **Load** `dicom-analytics/SKILL.md` Steps 8-9 for Semantic View + Cortex Agent creation
4. **Load** `cortex-agent` skill for Agent creation best practices and patterns
5. Agent combines Semantic View (structured text-to-SQL) + Cortex Search (report retrieval) as tools

### VIEWER Routing
1. **Load** `imaging-viewer/SKILL.md`
2. VIEWER builds React-based frontends — NOT Streamlit
3. For Streamlit dashboards, redirect to ANALYTICS + `developing-with-streamlit`
4. For SPCS pixel viewers, VIEWER invokes `deploy-to-spcs`

### GOVERNANCE Routing
1. **Load** `imaging-governance/SKILL.md`
2. Sub-skill applies masking policies, row-access policies, classification, audit
3. PHI column inventory is defined in `imaging-governance/SKILL.md` under the PHI Column Inventory section

### ML Routing
1. **Load** `imaging-ml/SKILL.md`
2. Sub-skill handles model training, registry, deployment, inference

## Workflow

```
Start
  |
  v
Confirm Environment ({database}, {schema}, {warehouse})
  |
  v
Detect Intent(s) from table above
  |
  v
Multiple intents? --YES--> Execute in dependency order (see Multi-Intent)
  |                         PARSE -> INGEST -> ANALYTICS/COHORT -> GOVERNANCE -> AGENT -> VIEWER/ML
  NO                        [STOP between each intent]
  |
  v
Route to sub-skill:
  +---> PARSE ----------> dicom-parser/SKILL.md
  |
  +---> INGEST ---------> dicom-ingestion/SKILL.md
  |
  +---> ANALYTICS ------> dicom-analytics/SKILL.md (full Step 1 menu)
  |
  +---> COHORT ----------> dicom-analytics/SKILL.md (jump to Step 2)
  |
  +---> GOVERNANCE ------> imaging-governance/SKILL.md
  |
  +---> AGENT ----------> Check prerequisites -> dicom-analytics Steps 8-9
  |                        + cortex-agent skill
  +---> VIEWER ----------> imaging-viewer/SKILL.md (React)
  |
  +---> ML --------------> imaging-ml/SKILL.md
```

## Cross-Cutting Concerns

All sub-skills should apply these platform patterns:

- **Environment Parameterization**: All SQL uses `{database}`, `{schema}`, `{warehouse}` placeholders. The router collects these values from the user at startup and passes them to every sub-skill. Sub-skills and reference SQL files MUST substitute placeholders before execution. Never hardcode database, schema, or warehouse names.
- **Schema Source of Truth**: All DDLs and column definitions live as `references/*.sql` files within the sub-skills that own them. The canonical 19-table DICOM data model is in `dicom-parser/references/data_model_ddl.sql`. Other sub-skills reference this file for column mappings when needed.
- **DICOM Parsing**: The `dicom-parser` sub-skill contains a comprehensive 19-table DICOM data model and a pydicom-based parser script. Use it as the foundation before ingestion or analytics.
- **Data Engineering**: Dynamic Tables for incremental refresh, Streams/Tasks for event-driven pipelines.
- **AI/ML**: Cortex AI functions (COMPLETE, EXTRACT, SENTIMENT), Cortex Search for imaging metadata, ML Registry for models.
- **Cortex Agent**: For conversational natural-language access to imaging data. The Agent combines a Semantic View (structured text-to-SQL queries over DICOM tables) and a Cortex Search Service (semantic search over radiology reports and findings) as tools. **Load** `cortex-agent` skill for Agent creation patterns and best practices. Prerequisites: `DICOM_ANALYTICS_SV` (Semantic View) and `IMAGING_SEARCH_SVC` (Cortex Search) must exist — built in `dicom-analytics/SKILL.md` Steps 7-8.
- **Semantic View**: For self-service text-to-SQL analytics over DICOM tables. Semantic Views annotate tables with dimensions, measures, and relationships so Cortex Analyst can generate SQL from natural language. Built in `dicom-analytics/SKILL.md` Step 8. Once created, the Semantic View becomes a tool for the Cortex Agent.
- **Apps**: React-based frontends for imaging dashboards and cohort browsers via `imaging-viewer/SKILL.md`. For Streamlit metadata dashboards, invoke `developing-with-streamlit`. For SPCS pixel rendering, invoke `deploy-to-spcs`.
- **Security**: Data masking for PHI, row-access policies per role, SYSTEM$CLASSIFY for PII detection, audit trails via ACCESS_HISTORY.

## Fallback Behavior

If user request does not match any intent:
1. Present the intent table and ask user to clarify which workflow they need
2. If the request is about DICOM schema or table structure, route to PARSE (owns the data model DDLs)
3. If the request is about non-imaging Snowflake features, suggest the appropriate platform skill instead

## Stopping Points

- After environment configuration — confirm {database}, {schema}, {warehouse}
- After intent detection if ambiguous — ask user to clarify
- After each intent in a multi-intent chain — confirm completion before starting next
- Before creating any database objects (tables, Dynamic Tables, services, agents)
- Before deploying apps or models
- Before creating Cortex Agent (capstone integration — requires Semantic View + Search Service)
- After AGENT routing prerequisite check — if Semantic View or Search Service missing, confirm building them first
