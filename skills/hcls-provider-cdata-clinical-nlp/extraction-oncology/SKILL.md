---
name: extraction-oncology
description: "Extract tumor staging, histology, grade, genetic markers, and treatment response from clinical notes using Cortex AI. Maps to TUMOR_EPISODE table. Covers sections: Pathology, Oncology, Radiology, Treatment Summary."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Oncology Extraction

## Scope

| Entity Subtype | Examples | Target Table |
|---------------|----------|--------------|
| Tumor Staging | T2N1M0, Stage IIIA, pT3pN2, cT1c | TUMOR_EPISODE |
| Histology | invasive ductal carcinoma, adenocarcinoma NOS | TUMOR_EPISODE |
| Grade | Gleason 4+3=7, grade 2/3, well-differentiated | TUMOR_EPISODE |
| Biomarkers | ER+/PR+/HER2-, EGFR mutation, KRAS wild-type, MSI-high | TUMOR_EPISODE |
| Treatment Response | complete response, partial response, progressive disease | TUMOR_EPISODE |
| Metastasis | liver mets, bone metastases, brain lesion | TUMOR_EPISODE |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| Full oncology extraction | **Cortex AI COMPLETE** | Oncology is highly contextual — staging systems, grading conventions, biomarker interpretation |

> **ICD-O / NCIt coding**: This sub-skill extracts oncology *text* and structured attributes. Mapping to standard codes (ICD-O morphology, NCIt, SNOMED CT) is handled downstream by the **terminology normalization layer** (Phase 3).

### Validation Regex

TNM staging notation, stage groups, and Gleason scores follow strict deterministic patterns. Use these as **cross-checks** against LLM output — not as parallel extraction.

## Extraction Prompt

```sql
SELECT
    d.document_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical oncology NLP system. Extract ALL tumor-related information from this clinical note.

STAGING RULES:
- Extract TNM staging if present (T, N, M components separately)
- Distinguish clinical (c) vs pathological (p) staging prefix
- Extract overall stage group (e.g., Stage IIIA)
- Identify staging system used (AJCC 8th edition assumed unless stated)

HISTOLOGY RULES:
- Extract tumor histology / morphology (e.g., invasive ductal carcinoma)
- Extract primary site (body site of origin)
- Do NOT attempt to assign ICD-O morphology codes — terminology normalization is handled downstream

GRADE RULES:
- Extract tumor grade (well/moderately/poorly differentiated OR numeric)
- For prostate: extract Gleason score as primary + secondary
- For breast: Nottingham grade
- For CNS: WHO grade

BIOMARKER RULES:
- Extract receptor status (ER, PR, HER2 for breast)
- Extract molecular markers (EGFR, ALK, KRAS, BRAF, PD-L1, MSI, TMB)
- Capture positive/negative/equivocal status and numeric values if given

RESPONSE RULES:
- Extract treatment response assessment (RECIST criteria preferred)
- Categories: COMPLETE_RESPONSE, PARTIAL_RESPONSE, STABLE_DISEASE, PROGRESSIVE_DISEASE
- Note which treatment the response is to

Return ONLY valid JSON:
{
  "tumors": [
    {
      "primary_site_display": "string or null",
      "laterality": "LEFT | RIGHT | BILATERAL | MIDLINE | null",
      "histology_display": "string or null",
      "histology_code": "null (populated downstream by terminology normalization)",
      "grade_display": "string or null",
      "grade_system": "GLEASON | NOTTINGHAM | WHO | GENERIC | null",
      "t_stage": "string or null (e.g., T2, pT3)",
      "n_stage": "string or null (e.g., N1, pN2)",
      "m_stage": "string or null (e.g., M0, M1)",
      "stage_group": "string or null (e.g., Stage IIIA)",
      "staging_type": "CLINICAL | PATHOLOGICAL | null",
      "biomarkers": [
        {
          "name": "string (e.g., ER, HER2, EGFR)",
          "result": "POSITIVE | NEGATIVE | EQUIVOCAL | null",
          "value": "string or null (e.g., 95%, 3+, Allred 8/8)"
        }
      ],
      "treatment_response": "COMPLETE_RESPONSE | PARTIAL_RESPONSE | STABLE_DISEASE | PROGRESSIVE_DISEASE | null",
      "response_to_treatment": "string or null (treatment name)",
      "metastatic_sites": ["string"],
      "context": {
        "is_negated": false,
        "temporality": "CURRENT | HISTORICAL",
        "certainty": "CONFIRMED | PROBABLE | POSSIBLE",
        "section_found_in": ""
      }
    }
  ]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_oncology
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Validation Regex: TNM, Stage Group, Gleason

Use to **cross-check** LLM-extracted staging against deterministic patterns found in the note:

```sql
SELECT
    REGEXP_SUBSTR_ALL(raw_text, '[cp]?T[0-4is][a-d]?\\s*[cp]?N[0-3X][a-c]?\\s*M[0-1X]', 1, 1, 'i') AS tnm_patterns,
    REGEXP_SUBSTR_ALL(raw_text, 'Stage\\s+[I]{1,3}V?\\s*[A-C]?', 1, 1, 'i') AS stage_groups,
    REGEXP_SUBSTR_ALL(raw_text, 'Gleason\\s*(\\d)\\s*\\+\\s*(\\d)\\s*=\\s*(\\d+)', 1, 1, 'ie') AS gleason_scores
FROM NOTE_DOCUMENT
WHERE document_id = :document_id;
```

If regex finds a TNM pattern the LLM missed, or the LLM's `t_stage`/`n_stage`/`m_stage` doesn't match the regex extraction, flag for review.

## Post-Processing

After extraction, parse JSON and insert into the clinical table with promoted NLP fields:

1. **TUMOR_EPISODE** — one row per distinct tumor with staging, histology, grade, response, and promoted fields: `certainty`, `evidence_text`, `extraction_confidence`

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, `candidate_type` = TUMOR/STAGING/BIOMARKER, STAGING_TYPE/GRADE_SYSTEM/RESPONSE_CRITERIA attributes), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## Note Sections to Target

| Section | What to Extract |
|---------|----------------|
| Pathology Report | Histology, grade, staging (pathological), biomarkers |
| Oncology Assessment | Clinical staging, treatment response, biomarker results |
| Radiology Report | Tumor measurements, metastatic sites, response assessment |
| Treatment Summary | Response to therapy, restaging results |
| Synoptic Report | Structured pathology data (staging, margins, LVI) |

## Output

Rows inserted into **TUMOR_EPISODE** table with staging, grading, histology, biomarker, and performance status fields.
