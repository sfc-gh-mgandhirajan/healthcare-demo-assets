---
name: extraction-conditions-diagnostics
description: "Extract conditions, diagnoses, symptoms, and risk factors from clinical notes using Cortex AI. Maps to FHIR Condition resource. Covers note sections: HPI, PMH, Assessment, Impression, ROS."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Conditions & Diagnostics Extraction

## Scope

| Entity Subtype | Examples | Target Table |
|---------------|----------|--------------|
| Diagnosis | Type 2 diabetes mellitus, pneumonia, CHF | CONDITION |
| Symptom | chest pain, dyspnea, fatigue, nausea | CONDITION (category=SYMPTOM) |
| Problem List Item | hypertension, CKD stage 3, GERD | CONDITION (category=PROBLEM_LIST_ITEM) |
| Risk Factor | obesity, smoking history, family hx of CAD | CONDITION (category=RISK_FACTOR) |
| History Of | history of stroke, prior MI | CONDITION (category=HISTORY_OF) |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| Condition extraction with context | **Cortex AI COMPLETE** | Understands clinical nuance, negation, temporality, severity, certainty |
| Section detection (HPI, PMH, Assessment) | **Regex** | Well-defined headers |

> **ICD-10 / SNOMED coding**: This sub-skill extracts condition *text* and context. Mapping to standard codes (ICD-10-CM, SNOMED CT) is handled downstream by the **terminology normalization layer** (Phase 3).

## Step 0: Data Model Grounding (Provided by Router)

If the router passes grounding context from `CLINICAL_NLP_MODEL_SEARCH_SVC`, use it to construct the extraction prompt. Otherwise fall back to the hardcoded schema below.

### Hardcoded Fallback Schema

```json
{
  "conditions": [
    {
      "display": "string (required)",
      "norm_code": "null (populated downstream by terminology normalization)",
      "norm_code_system": "null (populated downstream by terminology normalization)",
      "clinical_status": "active | recurrence | resolved | inactive | unknown",
      "verification_status": "provisional | differential | confirmed | refuted",
      "category": "PROBLEM_LIST_ITEM | ENCOUNTER_DIAGNOSIS | SYMPTOM | RISK_FACTOR | HISTORY_OF",
      "severity_display": "mild | moderate | severe | null",
      "body_site_display": "string or null",
      "laterality": "LEFT | RIGHT | BILATERAL | null",
      "onset_description": "string or null (temporal context from text)",
      "context": {
        "is_negated": "boolean",
        "temporality": "CURRENT | HISTORICAL | FUTURE",
        "certainty": "CONFIRMED | PROBABLE | POSSIBLE | UNLIKELY | RULED_OUT",
        "experiencer": "PATIENT | FAMILY_MEMBER | OTHER",
        "section_found_in": "string (note section where found)"
      }
    }
  ]
}
```

## Extraction Prompt

```sql
SELECT
    d.document_id,
    d.patient_id,
    d.note_type,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical NLP system. Extract ALL conditions, diagnoses, symptoms, and risk factors from this clinical note.

RULES:
- Extract every condition mentioned, including negated ones (mark is_negated=true)
- Distinguish between current vs historical conditions
- Identify the note section where each condition was found
- Assign category: PROBLEM_LIST_ITEM for active problems, ENCOUNTER_DIAGNOSIS for visit diagnoses, SYMPTOM for symptoms, RISK_FACTOR for risk factors, HISTORY_OF for past conditions
- Do NOT attempt to assign ICD-10 or SNOMED codes — terminology normalization is handled downstream
- Capture severity if stated (mild, moderate, severe)
- Capture body site and laterality if stated
- Assess certainty: CONFIRMED, PROBABLE, POSSIBLE, UNLIKELY, RULED_OUT

Return ONLY valid JSON matching this schema:
{
  "conditions": [
    {
      "display": "",
      "norm_code": null,
      "norm_code_system": null,
      "clinical_status": "active",
      "verification_status": "confirmed",
      "category": "PROBLEM_LIST_ITEM",
      "severity_display": null,
      "body_site_display": null,
      "laterality": null,
      "onset_description": null,
      "context": {
        "is_negated": false,
        "temporality": "CURRENT",
        "certainty": "CONFIRMED",
        "experiencer": "PATIENT",
        "section_found_in": ""
      }
    }
  ]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_conditions
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Post-Processing: Write to CONDITION

After extraction, parse the JSON and insert into the **CONDITION** table — one row per unique condition, with promoted NLP fields written directly:

```sql
INSERT INTO CONDITION (
    condition_id, patient_id, encounter_id, display, clinical_status,
    verification_status, category, onset_datetime, abatement_datetime,
    severity_code, severity_display, body_site_code, body_site_display,
    laterality, recorded_date, source, provenance_document_id,
    norm_code, norm_code_system,
    is_negated, temporality, certainty, evidence_text, extraction_confidence
)
SELECT
    UUID_STRING(),
    :patient_id,
    :encounter_id,
    c.value:display::VARCHAR,
    c.value:clinical_status::VARCHAR,
    c.value:verification_status::VARCHAR,
    c.value:category::VARCHAR,
    TRY_TO_TIMESTAMP(c.value:onset_datetime::VARCHAR),
    TRY_TO_TIMESTAMP(c.value:abatement_datetime::VARCHAR),
    c.value:severity_code::VARCHAR,
    c.value:severity_display::VARCHAR,
    c.value:body_site_code::VARCHAR,
    c.value:body_site_display::VARCHAR,
    c.value:laterality::VARCHAR,
    CURRENT_DATE(),
    'NLP',
    :document_id,
    NULL,
    NULL,
    COALESCE(c.value:is_negated::BOOLEAN, FALSE),
    c.value:temporality::VARCHAR,
    c.value:certainty::VARCHAR,
    c.value:evidence_text::VARCHAR,
    c.value:extraction_confidence::NUMBER(5,4)
FROM TABLE(FLATTEN(PARSE_JSON(:extracted_json):conditions)) c;
```

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, additional attributes), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## Validation Rules

| Rule | Check |
|------|-------|
| `clinical_status` must be in enum | `active, recurrence, resolved, inactive, unknown` |
| `verification_status` must be in enum | `provisional, differential, confirmed, refuted, entered_in_error` |
| `category` must be in enum | `PROBLEM_LIST_ITEM, ENCOUNTER_DIAGNOSIS, SYMPTOM, RISK_FACTOR, HISTORY_OF` |
| `severity_display` if present | `mild, moderate, severe` |
| `laterality` if present | `LEFT, RIGHT, BILATERAL` |
| Negated conditions | Should have `clinical_status` = 'inactive' or `verification_status` = 'refuted' |

## Note Sections to Target

| Section | What to Extract |
|---------|----------------|
| HPI | Active symptoms, presenting conditions |
| PMH | Historical conditions (category=HISTORY_OF) |
| Assessment | Encounter diagnoses, differential diagnoses |
| Impression | Confirmed/probable diagnoses |
| ROS | Symptoms (positive and negative) |
| Problem List | Active problem list items |

## Output

Rows inserted into **CONDITION** table with promoted NLP fields (is_negated, temporality, certainty, evidence_text, extraction_confidence). One row per extracted condition per document.
