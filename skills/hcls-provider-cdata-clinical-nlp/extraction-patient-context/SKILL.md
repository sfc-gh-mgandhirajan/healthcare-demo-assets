---
name: extraction-patient-context
description: "Extract Social Determinants of Health (SDOH), social history, and family history from clinical notes using Cortex AI. Maps to FHIR Observation (social-history) and FamilyMemberHistory. Covers SDOH domains: food insecurity, housing, transportation, financial strain, education, social isolation, safety, employment, substance use, tobacco, alcohol, veteran status."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Patient Context Extraction

## Scope

### Social History & SDOH

| SDOH Domain | Examples in Notes | Target Table |
|-------------|-------------------|--------------|
| TOBACCO | 1 ppd x 30 yrs, former smoker, never smoker, vaping | SOCIAL_HISTORY_OBSERVATION |
| ALCOHOL | 2-3 beers nightly, social drinker, denies alcohol, binge drinking | SOCIAL_HISTORY_OBSERVATION |
| SUBSTANCE_USE | marijuana, cocaine, IV drug use, denies illicits, opioid use disorder | SOCIAL_HISTORY_OBSERVATION |
| EMPLOYMENT | retired teacher, construction worker, unemployed, on disability | SOCIAL_HISTORY_OBSERVATION |
| HOUSING | lives alone, homeless, shelter, assisted living, eviction notice | SOCIAL_HISTORY_OBSERVATION |
| FOOD_INSECURITY | skips meals, food bank, can't afford groceries, food desert | SOCIAL_HISTORY_OBSERVATION |
| TRANSPORTATION | no ride to appointments, missed dialysis due to transport, relies on bus | SOCIAL_HISTORY_OBSERVATION |
| FINANCIAL_STRAIN | can't afford medications, uninsured, cost concerns, medical debt | SOCIAL_HISTORY_OBSERVATION |
| EDUCATION | limited English, cannot read discharge instructions, 8th grade education, health literacy concerns | SOCIAL_HISTORY_OBSERVATION |
| SOCIAL_ISOLATION | no family support, lives alone, widowed recently, no emergency contact | SOCIAL_HISTORY_OBSERVATION |
| SAFETY | safety concerns at home, DV screening positive, elder abuse suspected, gun in home | SOCIAL_HISTORY_OBSERVATION |
| VETERAN_STATUS | served in Vietnam, Gulf War veteran, PTSD service-related, Agent Orange exposure | SOCIAL_HISTORY_OBSERVATION |

### Family History

| Entity Subtype | Examples | Target Table |
|---------------|----------|--------------|
| Family Condition | mother had breast cancer at 52, father died of MI at 60 | FAMILY_MEMBER_HISTORY |
| Negative Family History | no family history of diabetes, no hx of colon cancer | FAMILY_MEMBER_HISTORY |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| SDOH extraction across all domains | **Cortex AI COMPLETE** | SDOH language is oblique, euphemistic, contextual — "patient reports difficulty affording insulin" signals FINANCIAL_STRAIN, not just medication non-adherence |
| Family history extraction | **Cortex AI COMPLETE** | Relationship parsing, condition mapping, age of onset |

> **SNOMED / LOINC / ICD-10 Z-code mapping**: This sub-skill extracts SDOH *text*, domain classification, and evidence. Mapping to standard codes (ICD-10 Z55-Z65, SNOMED CT, LOINC) is handled downstream by the **terminology normalization layer** (Phase 3).

## Extraction Prompt

```sql
SELECT
    d.document_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical NLP system specialized in Social Determinants of Health (SDOH). Extract ALL social history, SDOH factors, and family history from this clinical note.

SDOH EXTRACTION RULES:
- Look for SDOH signals across the ENTIRE note, not just the Social History section
- SDOH language is often indirect — interpret these signals:
  - "patient reports skipping meals due to cost" → FOOD_INSECURITY
  - "missed appointments due to no transportation" → TRANSPORTATION
  - "lives in shelter" or "housing unstable" → HOUSING
  - "cannot afford medications" or "requesting samples" → FINANCIAL_STRAIN
  - "limited English proficiency" or "interpreter used" → EDUCATION
  - "no support system" or "socially isolated" → SOCIAL_ISOLATION
  - "safety concerns at home" or "DV screen positive" → SAFETY
  - "veteran" or "military service" → VETERAN_STATUS

- Assign exactly ONE sdoh_domain per observation. If a statement spans two domains (e.g., "can't afford rides to dialysis"), create TWO separate observations — one per domain
- ALWAYS include evidence_text: the exact quote or close paraphrase from the note that triggered the observation
- Capture status: ACTIVE (current issue), RESOLVED (was an issue, no longer), UNKNOWN (unclear)
- Note if from a screening tool (PRAPARE, AHC-HRSN) or from narrative text

TOBACCO / ALCOHOL / SUBSTANCE RULES:
- Tobacco: capture status (current, former, never), quantity (ppd), duration (years), pack-years if calculable
- Alcohol: capture pattern (daily, social, none), quantity, type
- Substances: capture type, route (smoked, IV, oral), frequency
- Mark negated items (e.g., "denies alcohol" → is_negated=true, status=RESOLVED or omit)

FAMILY HISTORY RULES:
- Capture relationship (mother, father, sibling, etc.)
- Capture condition for each family member
- Capture age at onset or age at death if mentioned
- Note if family member is deceased
- "No family history of X" → is_negated=true

Do NOT attempt to assign SNOMED, LOINC, or ICD-10 Z-codes — terminology normalization is handled downstream.

Return ONLY valid JSON:
{
  "social_history": [
    {
      "display": "string (concise label, e.g., 'Food insecurity - skips meals due to cost')",
      "sdoh_domain": "FOOD_INSECURITY | HOUSING | TRANSPORTATION | FINANCIAL_STRAIN | EDUCATION | SOCIAL_ISOLATION | SAFETY | EMPLOYMENT | SUBSTANCE_USE | TOBACCO | ALCOHOL | VETERAN_STATUS | OTHER",
      "status": "ACTIVE | RESOLVED | UNKNOWN",
      "screening_instrument": "PRAPARE | AHC_HRSN | NARRATIVE | null",
      "evidence_text": "string (exact quote or close paraphrase from the note)",
      "value_string": "string (detailed description)",
      "value_quantity": null,
      "value_unit": "pack-years | drinks/week | null",
      "effective_period_start": null,
      "effective_period_end": null,
      "context": {
        "is_negated": false,
        "temporality": "CURRENT | HISTORICAL",
        "section_found_in": ""
      }
    }
  ],
  "family_history": [
    {
      "relationship_code": "MOTHER | FATHER | SIBLING | CHILD | OTHER",
      "relationship_display": "string",
      "condition_display": "string",
      "onset_age": null,
      "deceased_flag": null,
      "context": {
        "is_negated": false,
        "section_found_in": ""
      }
    }
  ]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_patient_context
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Validation: Pack-Year Arithmetic Check

The LLM extracts pack-years as `value_quantity`. Use this regex to **validate** the LLM's math when the note contains explicit ppd x years:

```sql
SELECT
    REGEXP_SUBSTR(raw_text, '(\\d+\\.?\\d*)\\s*(?:ppd|packs?\\s*/\\s*day)', 1, 1, 'ie', 1) AS packs_per_day,
    REGEXP_SUBSTR(raw_text, '(?:x|for|times)\\s*(\\d+\\.?\\d*)\\s*(?:years?|yrs?)', 1, 1, 'ie', 1) AS years_smoking,
    REGEXP_SUBSTR(raw_text, '(\\d+\\.?\\d*)\\s*(?:pack[- ]?years?)', 1, 1, 'ie', 1) AS stated_pack_years
FROM NOTE_DOCUMENT
WHERE document_id = :document_id;
```

If `packs_per_day * years_smoking` != LLM-extracted `value_quantity`, flag for review.

## Post-Processing

After extraction, parse JSON and insert into clinical tables with promoted NLP fields:

1. **SOCIAL_HISTORY_OBSERVATION** — one row per SDOH observation, with `sdoh_domain`, `status`, `screening_instrument`, `evidence_text`, and `extraction_confidence` populated
2. **FAMILY_MEMBER_HISTORY** — one row per family member + condition pair, with `is_negated`, `evidence_text`, and `extraction_confidence` populated
3. **NLP_NOTE_ENTITY_RELATION** — CO_OCCURRING relation when a single statement triggers multiple SDOH domains

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, `candidate_type` = SOCIAL_HISTORY or FAMILY_HISTORY), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## SDOH Domain Reference

| Domain | ICD-10 Z-Code Range | Gravity Project Category | Common Note Signals |
|--------|--------------------|--------------------------|--------------------|
| FOOD_INSECURITY | Z59.4x | Food Insecurity | skips meals, food bank, food stamps, SNAP, WIC, food desert |
| HOUSING | Z59.0x-Z59.1x | Housing Instability | homeless, shelter, couch surfing, eviction, substandard housing |
| TRANSPORTATION | Z59.82 | Transportation Insecurity | no ride, missed due to transport, relies on bus/Medicaid transport |
| FINANCIAL_STRAIN | Z59.5x-Z59.7x | Financial Insecurity | can't afford, uninsured, underinsured, requesting samples, copay concerns |
| EDUCATION | Z55.x | Education/Literacy | limited English, interpreter, low literacy, cannot read instructions |
| SOCIAL_ISOLATION | Z60.2, Z63.x | Social Connection | lives alone, no support, widowed, no emergency contact, isolated |
| SAFETY | Z63.0x, T74-T76 | Safety/Violence | DV positive, elder abuse, gun in home, unsafe environment |
| EMPLOYMENT | Z56.x | Employment | unemployed, on disability, lost job, work injury, retired |
| SUBSTANCE_USE | Z72.x, F10-F19 | Substance Use | drug use, opioid use disorder, marijuana, cocaine, IV drugs |
| TOBACCO | Z72.0, F17.x | Tobacco Use | smoker, ppd, pack-years, vaping, chewing tobacco, snuff |
| ALCOHOL | Z72.1, F10.x | Alcohol Use | drinks daily, binge, social drinker, alcohol use disorder |
| VETERAN_STATUS | Z91.82 | Veteran Status | veteran, military service, deployed, combat exposure |

## Note Sections to Target

| Section | What to Extract | SDOH Signal Likelihood |
|---------|----------------|----------------------|
| Social History | Primary source for tobacco, alcohol, drugs, occupation, living situation | HIGH |
| Family History | Conditions by family member with age of onset | N/A (family only) |
| HPI | SDOH often surfaces when explaining presentation ("missed meds because can't afford") | MEDIUM |
| Plan | SDOH referrals (social work, food assistance, housing resources) | MEDIUM |
| Discharge Instructions | Transportation, follow-up barriers, medication access | MEDIUM |
| ROS | May contain social/family context | LOW |
| Nursing Notes | Safety assessments, fall risk, social work consults | MEDIUM |

## Output

Rows inserted into **SOCIAL_HISTORY_OBSERVATION** and **FAMILY_MEMBER_HISTORY** tables with promoted NLP fields.
