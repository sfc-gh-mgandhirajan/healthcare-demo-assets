---
name: extraction-therapeutics
description: "Extract medications, procedures, and allergies from clinical notes using Cortex AI with selective regex. Maps to FHIR MedicationRequest, Procedure, and AllergyIntolerance. Covers note sections: Medications, Allergies, PSH, Plan."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Therapeutics Extraction

## Scope

| Entity Subtype | Examples | Target Table |
|---------------|----------|--------------|
| Medication | metformin 500mg BID, lisinopril 10mg daily | MEDICATION_REQUEST |
| Procedure (surgical) | appendectomy, CABG, total knee replacement | PROCEDURE (category=SURGICAL) |
| Procedure (diagnostic) | colonoscopy, CT abdomen, echocardiogram | PROCEDURE (category=DIAGNOSTIC) |
| Procedure (therapeutic) | chemotherapy, radiation therapy, dialysis | PROCEDURE (category=THERAPEUTIC) |
| Drug Allergy | penicillin allergy (rash), sulfa (anaphylaxis) | ALLERGY_INTOLERANCE |
| Food/Environmental Allergy | latex, shellfish, pollen | ALLERGY_INTOLERANCE |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| Medication extraction with dose/route/frequency | **Cortex AI COMPLETE** | Complex sig parsing, abbreviation expansion |
| Procedure extraction with context | **Cortex AI COMPLETE** | Distinguish planned vs completed, reason linkage |
| Allergy extraction with reaction/severity | **Cortex AI COMPLETE** | Severity assessment, reaction detail |

> **RxNorm / CPT / ICD-10-PCS coding**: This sub-skill extracts entity *text*, attributes, and context. Mapping to standard codes is handled downstream by the **terminology normalization layer** (Phase 3).

## Hardcoded Fallback Schema

```json
{
  "medications": [
    {
      "medication_display": "string (required)",
      "norm_code": "null (populated downstream by terminology normalization)",
      "norm_code_system": "null (populated downstream by terminology normalization)",
      "status": "ACTIVE | COMPLETED | STOPPED | CANCELLED | DRAFT | UNKNOWN",
      "intent": "ORDER | PLAN | PROPOSAL",
      "dosage_text": "string or null (full sig)",
      "dose": "number or null",
      "dose_unit": "string or null (mg, mcg, mL, units, etc.)",
      "route_display": "string or null (oral, IV, subcutaneous, topical, etc.)",
      "frequency_text": "string or null (BID, q6h, daily, PRN, etc.)",
      "duration_value": "number or null",
      "duration_unit": "DAYS | WEEKS | MONTHS | null",
      "indication_display": "string or null (condition this treats)",
      "context": {
        "is_negated": false,
        "temporality": "CURRENT | HISTORICAL | FUTURE",
        "section_found_in": ""
      }
    }
  ],
  "procedures": [
    {
      "display": "string (required)",
      "norm_code": "null (populated downstream by terminology normalization)",
      "norm_code_system": "null (populated downstream by terminology normalization)",
      "status": "PREPARATION | IN_PROGRESS | COMPLETED | NOT_DONE | STOPPED | UNKNOWN",
      "category": "SURGICAL | DIAGNOSTIC | IMAGING | THERAPEUTIC | OTHER",
      "body_site_display": "string or null",
      "laterality": "LEFT | RIGHT | BILATERAL | null",
      "reason_display": "string or null (indication condition)",
      "context": {
        "is_negated": false,
        "temporality": "CURRENT | HISTORICAL | FUTURE",
        "section_found_in": ""
      }
    }
  ],
  "allergies": [
    {
      "substance_display": "string (required)",
      "norm_code": "null (populated downstream by terminology normalization)",
      "norm_code_system": "null (populated downstream by terminology normalization)",
      "reaction_display": "string or null (rash, anaphylaxis, hives, etc.)",
      "severity": "MILD | MODERATE | SEVERE | LIFE_THREATENING | UNKNOWN",
      "criticality": "LOW | HIGH | UNABLE_TO_ASSESS | UNKNOWN",
      "verification_status": "UNCONFIRMED | CONFIRMED | REFUTED",
      "context": {
        "is_negated": false,
        "section_found_in": ""
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
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical NLP system. Extract ALL medications, procedures, and allergies from this clinical note.

MEDICATION RULES:
- Extract every medication mentioned with full sig (dose, unit, route, frequency, duration)
- Expand abbreviations: BID=twice daily, TID=three times daily, q6h=every 6 hours, PRN=as needed, PO=oral, IV=intravenous, SQ/SC=subcutaneous
- Distinguish active vs discontinued vs planned medications
- Link to indication condition if stated ("metformin for diabetes")

PROCEDURE RULES:
- Extract surgical, diagnostic, imaging, and therapeutic procedures
- Classify category: SURGICAL, DIAGNOSTIC, IMAGING, THERAPEUTIC
- Capture body site and laterality if stated
- Note if completed, planned, or refused (NOT_DONE)

ALLERGY RULES:
- Extract drug, food, and environmental allergies
- Capture the reaction manifestation (rash, anaphylaxis, GI upset, etc.)
- Assess severity and criticality from context
- NKDA = no known drug allergies (do not extract as an allergy)
- Do NOT attempt to assign RxNorm, CPT, ICD-10-PCS, or SNOMED codes — terminology normalization is handled downstream

Return ONLY valid JSON matching this schema:
{
  "medications": [{"medication_display": "", "dosage_text": "", "dose": null, "dose_unit": "", "route_display": "", "frequency_text": "", "status": "ACTIVE", "intent": "ORDER", "indication_display": null, "context": {"is_negated": false, "temporality": "CURRENT", "section_found_in": ""}}],
  "procedures": [{"display": "", "status": "COMPLETED", "category": "SURGICAL", "body_site_display": null, "laterality": null, "reason_display": null, "context": {"is_negated": false, "temporality": "HISTORICAL", "section_found_in": ""}}],
  "allergies": [{"substance_display": "", "reaction_display": null, "severity": "UNKNOWN", "criticality": "UNKNOWN", "verification_status": "CONFIRMED", "context": {"is_negated": false, "section_found_in": ""}}]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_therapeutics
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Post-Processing

After extraction, parse JSON and insert into the target clinical tables with promoted NLP fields written directly:

1. **MEDICATION_REQUEST** — one row per medication, including `is_negated`, `temporality`, `evidence_text`, `extraction_confidence`
2. **PROCEDURE** — one row per procedure, including `is_negated`, `temporality`, `evidence_text`, `extraction_confidence`
3. **ALLERGY_INTOLERANCE** — one row per allergy, including `is_negated`, `evidence_text`, `extraction_confidence`

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, additional attributes like DOSAGE, ROUTE, FREQUENCY, DURATION), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## Note Sections to Target

| Section | What to Extract |
|---------|----------------|
| Medications | Active medication list with full sigs |
| Allergies | Drug/food/environmental allergies with reactions |
| PSH (Past Surgical History) | Historical procedures (category=SURGICAL) |
| Plan | Planned medications, procedures, referrals |
| Discharge Medications | Discharge med list (status differentiation) |
| Operative Note | Procedure details with body site |

## Output

Rows inserted into **MEDICATION_REQUEST**, **PROCEDURE**, and **ALLERGY_INTOLERANCE** tables with promoted NLP fields. One row per extracted entity per document.
