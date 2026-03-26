---
name: hcls-provider-cdata-clinical-nlp
description: "**[REQUIRED]** Use for ALL clinical NLP tasks on Snowflake. This is the entry point for GenAI-powered clinical NLP solutions extracting structured information from unstructured clinical notes. Triggers: clinical NLP, NER, named entity recognition, clinical notes, discharge summary, text extraction, medical NLP, unstructured data, ICD coding, medication extraction, entity extraction, negation detection, clinical context, FHIR mapping, condition extraction, lab extraction, vital signs, procedure extraction, allergy extraction, adverse event, social history, family history, care plan, oncology staging, clinical data model, NLP schema."
platform_affinities:
  produces: [tables, dynamic_tables, views, cortex_search_service, stored_procedures, tasks, streams, stages, masking_policies, row_access_policies, tags, database_roles]
  benefits_from:
    - skill: dynamic-tables
      when: "building extraction pipeline — 6 DTs transform NOTE_DOCUMENT into typed clinical rows via Cortex COMPLETE"
    - skill: snowpark-python
      when: "building normalization SP — orchestrates exact match, deterministic mapping, and Cortex COMPLETE fuzzy match as batch SQL"
    - skill: cortex-ai-functions
      when: "using Cortex AI COMPLETE for entity extraction (DTs) and fuzzy terminology matching (SP)"
    - skill: data-governance
      when: "extracted entities contain PHI (patient names, MRNs, dates) — tag-based masking policies on clinical tables"
    - skill: developing-with-streamlit
      when: "user wants extraction QA reviewer, normalization QA dashboard, pipeline monitoring, or terminology browser"
    - skill: search-optimization
      when: "user needs semantic search over extracted clinical entities via Cortex Search Services"
    - skill: machine-learning
      when: "user wants to train or fine-tune clinical NER models"
---

# Clinical NLP on Snowflake

## Setup

1. **Verify** Snowflake connection is active and target database/schema exist
2. **Run Preflight Check** for Data Model Knowledge (see below)

## Preflight Check (REQUIRED -- Run at Skill Load)

Before routing to any sub-skill, verify the Clinical NLP Data Model Knowledge repository is available:

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "test", "columns": ["SEARCH_TEXT"], "limit": 1}'
);
```

| Result | Status | Behavior |
|--------|--------|----------|
| Returns results | READY | Step 0 (Data Model Knowledge pre-step) will use dynamic search results |
| Error / does not exist | MISSING | Step 0 skipped -- sub-skills fall back to hardcoded schema definitions. Inform user: "Clinical NLP data model search service not available -- using hardcoded schemas" |

This preflight runs ONCE at router load. The result determines whether Step 0 below executes or is skipped.

## Intent Detection

| Intent | Triggers | Load |
|--------|----------|------|
| EXTRACT_CONDITIONS_DIAGNOSTICS | "extract conditions", "diagnoses", "problems", "symptoms", "risk factors", "ICD coding" | `extraction-conditions-diagnostics/SKILL.md` |
| EXTRACT_THERAPEUTICS | "extract medications", "prescriptions", "dosage", "sig", "extract procedures", "surgeries", "CPT", "extract allergies", "intolerances", "drug allergy", "RxNorm" | `extraction-therapeutics/SKILL.md` |
| EXTRACT_OBSERVATIONS | "extract labs", "vitals", "exam findings", "imaging measurements", "scores", "LOINC" | `extraction-observations/SKILL.md` |
| EXTRACT_PATIENT_CONTEXT | "social history", "family history", "tobacco", "alcohol", "occupation", "living situation" | `extraction-patient-context/SKILL.md` |
| EXTRACT_ONCOLOGY | "cancer staging", "TNM", "tumor", "grade", "histology", "ECOG", "RECIST", "biomarkers" | `extraction-oncology/SKILL.md` |
| EXTRACT_SAFETY_CARE_PLANNING | "adverse events", "drug safety", "ADR", "MedDRA", "care plan", "goals", "referrals", "follow-up", "discharge plan" | `extraction-safety-care-planning/SKILL.md` |
| EXTRACT_ALL | "extract all entities", "full NLP pipeline", "process clinical note", "comprehensive extraction" | runs all 6 extraction sub-skills in sequence |
| NORMALIZE_CONDITIONS_DIAGNOSTICS | "normalize conditions", "code conditions", "ICD-10 mapping", "SNOMED conditions" | `normalization-conditions-diagnostics/SKILL.md` |
| NORMALIZE_THERAPEUTICS | "normalize medications", "RxNorm mapping", "CPT coding", "code procedures" | `normalization-therapeutics/SKILL.md` |
| NORMALIZE_OBSERVATIONS | "normalize labs", "LOINC mapping", "code observations", "code vitals" | `normalization-observations/SKILL.md` |
| NORMALIZE_PATIENT_CONTEXT | "normalize SDOH", "code social history", "Z-code mapping", "code family history" | `normalization-patient-context/SKILL.md` |
| NORMALIZE_ONCOLOGY | "normalize tumor", "ICD-O coding", "morphology code", "topography code" | `normalization-oncology/SKILL.md` |
| NORMALIZE_SAFETY | "normalize adverse events", "MedDRA coding", "code adverse events" | `normalization-safety-care-planning/SKILL.md` |
| NORMALIZE_ALL | "normalize all", "code all entities", "full terminology normalization" | runs all 6 normalization sub-skills in sequence |
| GOVERNANCE | "PHI masking", "de-identification", "clinical data governance", "audit", "role setup", "feature view", "Cortex guardrails", "ML governance" | `governance/SKILL.md` |
| MODEL_KNOWLEDGE | "data model reference", "clinical NLP schema", "generate DDL", "what columns", "FHIR mapping", "table relationships" | `data-model-knowledge/SKILL.md` |
| PIPELINE_SETUP | "deploy pipeline", "production setup", "create extraction DTs", "create normalization SP", "pipeline architecture", "automate clinical NLP" | `pipeline-implementation/SKILL.md` |

## Terminology Preference Gate (NORMALIZE_* intents only)

For any NORMALIZE_* intent, **ask the user** for their code system preference before proceeding. Do NOT assume a default.

### Gate Prompt

> What code system(s) should we normalize to? This depends on your use case:
>
> | Use Case | Recommended Code Systems |
> |----------|------------------------|
> | **US billing / claims** | ICD-10-CM (conditions), CPT (procedures), RxNorm (medications) |
> | **Clinical interoperability / FHIR** | SNOMED CT (conditions, procedures, findings), RxNorm (medications), LOINC (observations) |
> | **Research / analytics** | SNOMED CT (broad clinical coverage) |
> | **Pharmacovigilance / regulatory** | MedDRA (adverse events) |
> | **Cancer registry** | ICD-O-3 (site + histology) |
> | **Dual coding** | Both ICD-10-CM and SNOMED CT (maximum interoperability) |

### Set `$NORM_CODE_SYSTEMS` Context Variable

Store the user's selection and pass to all normalization sub-skills:

| User Selection | `$NORM_CODE_SYSTEMS` | Effect |
|---------------|---------------------|--------|
| ICD-10-CM only | `ICD-10-CM` | Conditions → ICD-10-CM; skip SNOMED CT matching |
| SNOMED CT only | `SNOMED CT` | Conditions → SNOMED CT; procedures → SNOMED CT |
| Both | `ICD-10-CM,SNOMED CT` | Attempt ICD-10-CM first, fill gaps with SNOMED CT (or vice versa per user preference) |
| All available | `ALL` | Use the preferred code system per concept category (see sub-skill defaults) |

### Multi-System Coding Behavior

When the user selects multiple code systems (e.g., `ICD-10-CM,SNOMED CT`):
- **Single best code per entity**: Each entity gets ONE code from the highest-priority matching system. The `code` and `code_system` fields on clinical tables hold a single value.
- **Priority order**: The first system listed in `$NORM_CODE_SYSTEMS` is preferred. If no match is found in that system, the next system is tried.
- **Not dual-row insertion**: We do NOT create duplicate rows for the same entity with different code systems. One entity = one row = one code.
- **Audit trail**: If a future use case needs the same entity coded in multiple systems simultaneously, that would be a separate enhancement storing secondary codes in the NLP layer tables.

**Per-category overrides**: If the user specifies different systems per concept category (e.g., "ICD-10-CM for conditions but SNOMED CT for procedures"), store as:
```
$NORM_CODE_SYSTEMS = {
    "CONDITIONS": "ICD-10-CM",
    "THERAPEUTICS": "SNOMED CT,RxNorm",
    "OBSERVATIONS": "LOINC",
    "PATIENT_CONTEXT": "ICD-10-CM",
    "ONCOLOGY": "ICD-O-3",
    "SAFETY": "MedDRA"
}
```

**NORMALIZE_ALL**: Must ask this gate before running the full pipeline.

**EXTRACT_* intents**: This gate does NOT apply — extraction is code-system-agnostic (captures text only, codes are NULL until normalization).

## Data Model Knowledge -- Automatic Pre-Step (Conditional on Preflight)

**CRITICAL:** For all EXTRACT_*, NORMALIZE_*, and GOVERNANCE intents, **execute Step 0 if preflight status is READY**. If preflight status is MISSING, skip Step 0 and let sub-skills use hardcoded schema definitions.

### Step 0: Query Data Model Knowledge (automatic for EXTRACT_* and GOVERNANCE)

Before generating any extraction prompts, building output schemas, or applying governance policies:

1. **Query the Clinical NLP model search service** for relevant tables/columns:
```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CLINICAL_NLP_MODEL_SEARCH_SVC',
    '{"query": "<context from user request>", "columns": ["table_name", "column_name", "data_type", "constraints", "description", "fhir_resource", "enum_values"]}'
);
```

2. **Use the search results** -- not hardcoded definitions -- as the source of truth for:
   - Output JSON schema for LLM extraction prompts (EXTRACT_*)
   - Target table columns and data types (EXTRACT_*)
   - PHI column identification (GOVERNANCE)
   - Foreign key relationships (all)
   - Valid enum values for validation (all)

3. **Pass results to the sub-skill** as grounding context.

| Intent | Step 0 Query Focus | What Gets Grounded |
|--------|-------------------|--------------------|
| EXTRACT_CONDITIONS_DIAGNOSTICS | CONDITION table columns + enum values | LLM prompt JSON schema, INSERT target |
| EXTRACT_THERAPEUTICS | MEDICATION_REQUEST + PROCEDURE + ALLERGY_INTOLERANCE columns | Combined LLM prompt for meds/procedures/allergies |
| EXTRACT_OBSERVATIONS | OBSERVATION table columns + categories | LLM prompt for labs + regex for vitals/scores |
| EXTRACT_PATIENT_CONTEXT | SOCIAL_HISTORY_OBSERVATION + FAMILY_MEMBER_HISTORY columns | LLM prompt for social/family extraction |
| EXTRACT_ONCOLOGY | TUMOR_EPISODE columns + staging/biomarker fields | LLM prompt for oncology extraction |
| EXTRACT_SAFETY_CARE_PLANNING | ADVERSE_EVENT + CARE_PLAN_ITEM columns | LLM prompt for ADE + care plan extraction |
| EXTRACT_ALL | All clinical + NLP layer columns | Full pipeline prompts (all 6 clusters) |
| GOVERNANCE | PHI-flagged columns across all 17 tables | Masking policy targets |

| NORMALIZE_CONDITIONS_DIAGNOSTICS | CONDITION code fields + CONCEPT_DIMENSION (DISEASE, SYMPTOM) | Code system lookup targets |
| NORMALIZE_THERAPEUTICS | MEDICATION_REQUEST + PROCEDURE + ALLERGY_INTOLERANCE code fields | Code system lookup targets |
| NORMALIZE_OBSERVATIONS | OBSERVATION code fields + CONCEPT_DIMENSION (LAB, SCORE) | LOINC/SNOMED CT lookup targets |
| NORMALIZE_PATIENT_CONTEXT | SOCIAL_HISTORY_OBSERVATION + FAMILY_MEMBER_HISTORY code fields | Z-code and SNOMED CT lookup targets |
| NORMALIZE_ONCOLOGY | TUMOR_EPISODE site/histology code fields | ICD-O-3 lookup targets |
| NORMALIZE_SAFETY | ADVERSE_EVENT event code fields | MedDRA/SNOMED CT lookup targets |
| NORMALIZE_ALL | All code fields across 9 clinical tables | Full normalization pipeline |

## NLP Engine Strategy

| Engine | When to Use | Strengths |
|--------|------------|-----------|
| **Cortex AI COMPLETE** (Primary) | Complex entity extraction, context detection, relationship mapping | Understands clinical context, handles ambiguity, no model deployment needed |
| **Regex / SQL patterns** (Selective) | Vitals (BP, HR, Temp, SpO2), ICD-10 codes, structured patterns | Deterministic, fast, complete for well-defined patterns |
| **spaCy / scispaCy UDF** (Optional) | High-throughput batch NER, UMLS linking | Fast inference, entity linking to UMLS CUIs |

The engine choice is **per entity type** -- Cortex AI for complex clinical reasoning, regex for deterministic patterns.

## Partially Flattened Design

Clinical tables carry **promoted NLP fields** directly (`is_negated`, `temporality`, `certainty`, `evidence_text`, `extraction_confidence`), eliminating the need for NLP layer joins in typical queries.

## NLP Layer Tables (Optional Deep Audit)

3 NLP layer tables are available for full provenance tracing but are **not required** for typical clinical queries:
- **NLP_NOTE_ENTITY_MENTION**: One row per extracted span (text, offsets, candidate type, engine metadata)
- **NLP_NOTE_ENTITY_ATTRIBUTE**: Additional context attributes per mention (experiencer, severity, body site, laterality, etc.)
- **NLP_NOTE_ENTITY_RELATION**: Relations between mentions (HAS_ANATOMICAL_SITE, INDICATION_OF, CAUSED_BY, etc.)

## Workflow

```
Start
  |
  v
Run Preflight Check (CLINICAL_NLP_MODEL_SEARCH_SVC)
  |
  v
Detect Intent from table above
  |
  v
Is intent NORMALIZE_*?
  |                |
  YES              NO
  |                |
  v                v
  Run Terminology  Continue
  Preference Gate  |
  (ask user for    |
  $NORM_CODE_      |
  SYSTEMS)         |
  |                |
  v                v
Is intent EXTRACT_*, NORMALIZE_*, or GOVERNANCE?
  |                              |
  YES                            NO
  |                              |
  v                              v
  Preflight READY?               Skip Step 0
  |          |                   (MODEL_KNOWLEDGE)
  YES        NO                  |
  |          |                   |
  v          v                   |
  Step 0:    Skip Step 0         |
  Query      (use hardcoded      |
  CLINICAL   schemas)            |
  _NLP_MODEL                     |
  _SEARCH_SVC                    |
  |          |                   |
  v          v                   v
  +---> EXTRACT_CONDITIONS_DIAGNOSTICS --> extraction-conditions-diagnostics/SKILL.md
  |
  +---> EXTRACT_THERAPEUTICS ------------> extraction-therapeutics/SKILL.md
  |
  +---> EXTRACT_OBSERVATIONS ------------> extraction-observations/SKILL.md
  |
  +---> EXTRACT_PATIENT_CONTEXT ---------> extraction-patient-context/SKILL.md
  |
  +---> EXTRACT_ONCOLOGY ----------------> extraction-oncology/SKILL.md
  |
  +---> EXTRACT_SAFETY_CARE_PLANNING ----> extraction-safety-care-planning/SKILL.md
  |
  +---> EXTRACT_ALL ---------------------> runs all 6 extraction sub-skills
  |
  |     [NORMALIZE_* intents receive $NORM_CODE_SYSTEMS from gate]
  |
  +---> NORMALIZE_CONDITIONS_DIAGNOSTICS -> normalization-conditions-diagnostics/SKILL.md
  |
  +---> NORMALIZE_THERAPEUTICS -----------> normalization-therapeutics/SKILL.md
  |
  +---> NORMALIZE_OBSERVATIONS -----------> normalization-observations/SKILL.md
  |
  +---> NORMALIZE_PATIENT_CONTEXT --------> normalization-patient-context/SKILL.md
  |
  +---> NORMALIZE_ONCOLOGY ---------------> normalization-oncology/SKILL.md
  |
  +---> NORMALIZE_SAFETY -----------------> normalization-safety-care-planning/SKILL.md
  |
  +---> NORMALIZE_ALL --------------------> runs all 6 normalization sub-skills
  |
  +---> GOVERNANCE ----------------------> governance/SKILL.md
  |
  +---> MODEL_KNOWLEDGE -----------------> data-model-knowledge/SKILL.md
  |
  +---> PIPELINE_SETUP ------------------> pipeline-implementation/SKILL.md
```

## Cross-Cutting Concerns

- **Data Model Knowledge (Preflight-Conditional Pre-Step)**: For EXTRACT_* and GOVERNANCE intents, the router **runs a preflight check** on `CLINICAL_NLP_MODEL_SEARCH_SVC`. If READY, it queries the Cortex Search Service before loading the sub-skill to ground all schema-dependent work in the latest data model. If MISSING, sub-skills fall back to hardcoded definitions.
- **Provenance Tracking**: Every extracted entity records `source` (EHR_STRUCTURED vs GENAI_NLP_NOTE), `engine_family` (LLM/RULES/HYBRID), `engine_name`, `confidence_score`, and `provenance_document_id` linking back to the source note.
- **Context Attributes**: All extraction sub-skills detect negation, temporality, certainty, experiencer, severity, anatomical site, and laterality via the NLP_NOTE_ENTITY_ATTRIBUTE table.
- **FHIR Alignment**: Output tables are FHIR-aligned (Condition, Observation, Procedure, MedicationRequest, AllergyIntolerance, AdverseEvent, FamilyMemberHistory, CarePlan) without requiring raw FHIR JSON.
- **Terminology Normalization**: 6 normalization sub-skills (one per concept category) map extracted text to standard codes using CONCEPT_DIMENSION lookup with Cortex AI fuzzy matching fallback. Supports SNOMED CT, ICD-10-CM, RxNorm, LOINC, MedDRA, ICD-O-3. Runs as a separate post-extraction step (Option C architecture) to avoid LLM code hallucination. **User-driven code system selection** via the Terminology Preference Gate (`$NORM_CODE_SYSTEMS`) ensures normalization targets the code systems the user actually needs — no hardcoded defaults.

## Quick Start (Before Sub-Skills Are Built)

Until extraction sub-skills are implemented, use this Cortex AI pattern directly:

```sql
SELECT
    note_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Extract all clinical entities from this note. Return ONLY valid JSON:
        {
            "conditions": [{"display": "", "code_system": "", "clinical_status": "", "category": "", "severity_display": "", "is_negated": false}],
            "medications": [{"medication_display": "", "dosage_text": "", "route_display": "", "frequency_text": "", "status": ""}],
            "procedures": [{"display": "", "status": "", "category": ""}],
            "observations": [{"display": "", "category": "", "value_quantity": null, "value_unit": "", "interpretation": ""}],
            "allergies": [{"substance_display": "", "reaction_display": "", "severity": "", "criticality": ""}]
        }

        Note: ' || note_text
    ) AS extracted_entities
FROM clinical_notes
LIMIT 10;
```

## Evidence Grounding: PubMed CKE

Invoke `$cke-pubmed` when biomedical literature context improves NLP accuracy:
- Entity disambiguation: look up biomedical terms, drug names, or disease concepts
- Terminology validation: verify extracted ICD/SNOMED/UMLS mappings against published usage
- Prompt grounding: ground LLM prompts with PubMed evidence for more accurate clinical entity extraction

## Stopping Points

- **⚠️ MANDATORY STOPPING POINT**: After intent detection if ambiguous — present matched intent and confirm before loading sub-skill
- **⚠️ MANDATORY STOPPING POINT**: Before creating any database objects — present DDL plan for user approval
- **⚠️ MANDATORY STOPPING POINT**: Before running extraction on large datasets (>100 documents) — confirm scope and estimated cost

## Output

Each sub-skill produces typed rows in FHIR-aligned clinical tables (CONDITION, OBSERVATION, PROCEDURE, MEDICATION_REQUEST, ALLERGY_INTOLERANCE, ADVERSE_EVENT, SOCIAL_HISTORY_OBSERVATION, FAMILY_MEMBER_HISTORY, CARE_PLAN_ITEM, TUMOR_EPISODE) with promoted NLP fields and optional terminology codes.
