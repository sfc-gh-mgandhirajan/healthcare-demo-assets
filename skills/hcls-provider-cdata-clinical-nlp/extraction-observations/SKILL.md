---
name: extraction-observations
description: "Extract labs, vitals, exam findings, imaging measurements, and clinical scores from clinical notes. Uses Cortex AI for complex findings and regex for deterministic vital sign patterns. Maps to FHIR Observation. Covers sections: Labs, Physical Exam, Imaging, ROS."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Observations Extraction

## Scope

| Entity Subtype | Examples | Target Table | Category |
|---------------|----------|--------------|----------|
| Vital Signs | BP 120/80, HR 72, Temp 98.6, SpO2 98% | OBSERVATION | VITAL_SIGNS |
| Lab Results | HbA1c 7.2%, WBC 12.5, Cr 1.4 | OBSERVATION | LAB |
| Exam Findings | lungs clear, 2/6 systolic murmur, trace edema | OBSERVATION | EXAM |
| Imaging Findings | 2cm nodule, pleural effusion, fracture | OBSERVATION | IMAGING |
| Clinical Scores | GCS 15, MELD 18, Wells score 3, NIHSS 4 | OBSERVATION | SCORE |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| Vital signs (BP, HR, Temp, RR, SpO2, Weight, Height) | **Regex** (primary) | Deterministic `label: number` patterns, high accuracy, fast |
| Clinical scores (GCS, ECOG, Karnofsky, MELD, etc.) | **Regex** (primary) | Well-defined `scale: number` patterns |
| Lab results with values and units | **Cortex AI COMPLETE** | Variable test names, interpretation requires clinical context |
| Exam findings with interpretation | **Cortex AI COMPLETE** | Clinical reasoning needed for normal vs abnormal |
| Imaging measurements | **Cortex AI COMPLETE** | Complex descriptions, spatial relationships |

> **LOINC / SNOMED coding**: This sub-skill extracts observation *text* and values. Mapping to standard codes (LOINC, SNOMED CT) is handled downstream by the **terminology normalization layer** (Phase 3).

## Regex-First: Vital Signs Extraction

Vitals are the best candidate for regex — they follow highly predictable patterns.

```sql
SELECT
    document_id,
    REGEXP_SUBSTR(raw_text, '(?:BP|Blood Pressure)[:\\s]*(\\d{2,3}\\s*/\\s*\\d{2,3})', 1, 1, 'ie', 1) AS blood_pressure,
    REGEXP_SUBSTR(raw_text, '(?:HR|Heart Rate|Pulse)[:\\s]*(\\d{2,3})', 1, 1, 'ie', 1) AS heart_rate,
    REGEXP_SUBSTR(raw_text, '(?:Temp|Temperature)[:\\s]*([\\d.]+)', 1, 1, 'ie', 1) AS temperature,
    REGEXP_SUBSTR(raw_text, '(?:RR|Resp(?:iratory)?\\s*Rate)[:\\s]*(\\d{1,2})', 1, 1, 'ie', 1) AS respiratory_rate,
    REGEXP_SUBSTR(raw_text, '(?:SpO2|O2\\s*Sat|Sat)[:\\s]*(\\d{2,3})\\s*%?', 1, 1, 'ie', 1) AS spo2,
    REGEXP_SUBSTR(raw_text, '(?:Weight|Wt)[:\\s]*([\\d.]+)\\s*(kg|lbs?|pounds?)?', 1, 1, 'ie', 1) AS weight,
    REGEXP_SUBSTR(raw_text, '(?:Height|Ht)[:\\s]*([\\d.]+|\\d+''\\d+"?)', 1, 1, 'ie', 1) AS height,
    REGEXP_SUBSTR(raw_text, '(?:BMI)[:\\s]*([\\d.]+)', 1, 1, 'ie', 1) AS bmi
FROM NOTE_DOCUMENT
WHERE document_id = :document_id;
```

## Regex-First: Clinical Scores

```sql
SELECT
    document_id,
    REGEXP_SUBSTR(raw_text, '(?:GCS|Glasgow)[:\\s]*(\\d{1,2})', 1, 1, 'ie', 1) AS gcs,
    REGEXP_SUBSTR(raw_text, '(?:ECOG)[:\\s]*(\\d)', 1, 1, 'ie', 1) AS ecog,
    REGEXP_SUBSTR(raw_text, '(?:Karnofsky|KPS)[:\\s]*(\\d{1,3})', 1, 1, 'ie', 1) AS karnofsky,
    REGEXP_SUBSTR(raw_text, '(?:MELD)[:\\s]*(\\d{1,2})', 1, 1, 'ie', 1) AS meld,
    REGEXP_SUBSTR(raw_text, '(?:Wells)[:\\s]*(\\d{1,2})', 1, 1, 'ie', 1) AS wells,
    REGEXP_SUBSTR(raw_text, '(?:NIHSS)[:\\s]*(\\d{1,2})', 1, 1, 'ie', 1) AS nihss,
    REGEXP_SUBSTR(raw_text, '(?:APACHE)[:\\s]*(\\d{1,3})', 1, 1, 'ie', 1) AS apache
FROM NOTE_DOCUMENT
WHERE document_id = :document_id;
```

## Cortex AI: Complex Observations

```sql
SELECT
    d.document_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical NLP system. Extract ALL lab results, exam findings, and imaging measurements from this clinical note.

DO NOT extract vital signs or clinical scores (those are extracted separately via regex).

LAB RULES:
- Extract lab test name, numeric value, unit, and interpretation (HIGH, LOW, ABNORMAL, NORMAL, CRITICAL_HIGH, CRITICAL_LOW)
- Include reference range if stated
- Category = "LAB"
- Do NOT attempt to assign LOINC or SNOMED codes — terminology normalization is handled downstream

EXAM RULES:
- Extract physical exam findings with body site
- Note normal vs abnormal findings
- Category = "EXAM"

IMAGING RULES:
- Extract imaging measurements (sizes, volumes)
- Note imaging modality context
- Category = "IMAGING"

Return ONLY valid JSON:
{
  "observations": [
    {
      "display": "",
      "norm_code": "null (populated downstream by terminology normalization)",
      "norm_code_system": "null (populated downstream by terminology normalization)",
      "category": "LAB | EXAM | IMAGING",
      "status": "FINAL",
      "value_quantity": null,
      "value_unit": "",
      "value_string": "",
      "interpretation": "HIGH | LOW | ABNORMAL | NORMAL | CRITICAL_HIGH | CRITICAL_LOW | null",
      "body_site_display": null,
      "laterality": null,
      "method": null,
      "context": {
        "is_negated": false,
        "temporality": "CURRENT | HISTORICAL",
        "section_found_in": ""
      }
    }
  ]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_observations
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Post-Processing: Merge Regex + LLM Results

1. Parse regex vitals and scores into OBSERVATION rows (`category`=VITAL_SIGNS or SCORE) with promoted NLP fields:
   - `is_negated` = FALSE (regex vitals are always asserted)
   - `temporality` = 'CURRENT'
   - `certainty` = 'CONFIRMED'
   - `evidence_text` = raw matched text
   - `extraction_confidence` = 1.0 (deterministic regex match)
2. Parse LLM observations into OBSERVATION rows (`category`=LAB/EXAM/IMAGING) with promoted NLP fields:
   - `is_negated`, `temporality`, `certainty` from LLM extraction
   - `evidence_text` = cited text from LLM
   - `extraction_confidence` from LLM confidence

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, engine metadata), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## Interpretation Logic

| Vital | Normal Range | Interpretation |
|-------|-------------|----------------|
| Systolic BP | 90-120 | <90 LOW, >140 HIGH, >180 CRITICAL_HIGH |
| Diastolic BP | 60-80 | <60 LOW, >90 HIGH, >120 CRITICAL_HIGH |
| HR | 60-100 | <60 LOW, >100 HIGH, >150 CRITICAL_HIGH |
| Temp (F) | 97.0-99.5 | <96.0 LOW, >100.4 HIGH, >104.0 CRITICAL_HIGH |
| SpO2 | 95-100 | <95 LOW, <90 CRITICAL_LOW |
| RR | 12-20 | <12 LOW, >20 HIGH, >30 CRITICAL_HIGH |

## Note Sections to Target

| Section | What to Extract |
|---------|----------------|
| Vitals | All vital sign measurements |
| Labs/Results | Lab values with interpretation |
| Physical Exam | Exam findings by body system |
| Imaging | Radiology findings and measurements |
| ROS (positive findings) | Reported symptoms as observations |

## Output

Rows inserted into **OBSERVATION** table with promoted NLP fields. Covers labs, vitals, exam findings, imaging measurements, and clinical scores.
