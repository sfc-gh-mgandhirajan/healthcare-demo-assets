---
name: extraction-safety-care-planning
description: "Extract adverse events and care plan items from clinical notes using Cortex AI. Maps to FHIR AdverseEvent and CarePlan. Covers sections: Plan, Discharge, Follow-up, Safety."
parent_skill: hcls-provider-cdata-clinical-nlp
---

# Safety & Care Planning Extraction

## Scope

| Entity Subtype | Examples | Target Table |
|---------------|----------|--------------|
| Adverse Drug Event | developed rash after starting amoxicillin | ADVERSE_EVENT |
| Procedural Complication | post-op wound infection, pneumothorax after central line | ADVERSE_EVENT |
| Fall / Safety Event | patient fell, near-miss event | ADVERSE_EVENT |
| Transfusion Reaction | febrile reaction to pRBC transfusion | ADVERSE_EVENT |
| Care Plan Goal | target HbA1c < 7%, reduce pain to 3/10 | CARE_PLAN_ITEM |
| Care Plan Action | start physical therapy, referral to cardiology | CARE_PLAN_ITEM |
| Follow-up | return in 2 weeks, repeat labs in 3 months | CARE_PLAN_ITEM |
| Discharge Instruction | activity restrictions, wound care instructions | CARE_PLAN_ITEM |

## Engine Strategy

| Pattern | Engine | Rationale |
|---------|--------|-----------|
| Adverse event extraction | **Cortex AI COMPLETE** | Causality assessment, temporal relationships, severity judgment |
| Care plan extraction | **Cortex AI COMPLETE** | Goal/action parsing, condition linkage, timeline |

## Extraction Prompt

```sql
SELECT
    d.document_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        $$You are a clinical NLP system. Extract ALL adverse events and care plan items from this clinical note.

ADVERSE EVENT RULES:
- Identify any unintended harmful outcome: drug reactions, procedural complications, falls, infections
- Link to suspected cause (medication, procedure, device)
- Assess seriousness: SERIOUS (requires hospitalization, causes disability, is life-threatening, or results in death), NON_SERIOUS, UNKNOWN
- Assess severity: MILD (no intervention needed), MODERATE (intervention needed), SEVERE (significant impact), LIFE_THREATENING
- Determine causality: CERTAIN, PROBABLE, POSSIBLE, UNLIKELY, UNASSESSABLE
- Note the temporal relationship to the suspected cause

CARE PLAN RULES:
- Extract goals with measurable targets (e.g., "HbA1c < 7%")
- Extract planned actions (medication changes, referrals, procedures, therapies)
- Extract follow-up instructions with timeline
- Extract discharge instructions if present
- Link each plan item to its target condition if stated
- Classify intent: PLAN (future), ORDER (active), PROPOSAL (suggestion)

Return ONLY valid JSON:
{
  "adverse_events": [
    {
      "event_display": "string (required, what happened)",
      "category": "DRUG_REACTION | PROCEDURAL_COMPLICATION | FALL | INFECTION | DEVICE | TRANSFUSION | OTHER",
      "seriousness": "SERIOUS | NON_SERIOUS | UNKNOWN",
      "severity": "MILD | MODERATE | SEVERE | LIFE_THREATENING | null",
      "outcome": "ONGOING | RECOVERING | RESOLVED | FATAL | UNKNOWN",
      "suspect_entity_display": "string or null (suspected cause)",
      "suspect_entity_type": "MEDICATION | PROCEDURE | DEVICE | null",
      "causality": "CERTAIN | PROBABLE | POSSIBLE | UNLIKELY | UNASSESSABLE",
      "onset_description": "string or null (when it started)",
      "context": {
        "section_found_in": ""
      }
    }
  ],
  "care_plan_items": [
    {
      "display": "string (required, the plan item)",
      "item_type": "GOAL | ACTION | REFERRAL | FOLLOW_UP | DISCHARGE_INSTRUCTION",
      "status": "ACTIVE | COMPLETED | CANCELLED | DRAFT",
      "intent": "PLAN | ORDER | PROPOSAL",
      "target_condition_display": "string or null (condition this addresses)",
      "target_value": "string or null (measurable target, e.g., 'HbA1c < 7%')",
      "timeline_value": null,
      "timeline_unit": "DAYS | WEEKS | MONTHS | null",
      "responsible_provider": "string or null",
      "context": {
        "section_found_in": ""
      }
    }
  ]
}

Clinical Note:
$$ || d.raw_text
    ) AS extracted_safety_care
FROM NOTE_DOCUMENT d
WHERE d.document_id = :document_id;
```

## Validation Regex: Follow-up Intervals

Use to **cross-check** LLM-extracted `timeline_value`/`timeline_unit` against deterministic patterns in the note:

```sql
SELECT
    REGEXP_SUBSTR_ALL(
        raw_text,
        '(?:follow[- ]?up|return|f/u|recheck|revisit)\\s+(?:in\\s+)?(\\d+)\\s*(days?|weeks?|months?)',
        1, 1, 'ie'
    ) AS followup_intervals,
    REGEXP_SUBSTR_ALL(
        raw_text,
        '(?:repeat|recheck)\\s+(?:labs?|CBC|BMP|CMP|imaging|CT|MRI)\\s+(?:in\\s+)?(\\d+)\\s*(days?|weeks?|months?)',
        1, 1, 'ie'
    ) AS repeat_test_intervals
FROM NOTE_DOCUMENT
WHERE document_id = :document_id;
```

If regex finds a follow-up interval the LLM missed, or the LLM's `timeline_value`/`timeline_unit` doesn't match the regex extraction, flag for review.

## Post-Processing

After extraction, parse JSON and insert into clinical tables with promoted NLP fields:

1. **ADVERSE_EVENT** — one row per adverse event with causality linkage, plus `evidence_text` and `extraction_confidence`
2. **CARE_PLAN_ITEM** — one row per goal/action/referral/follow-up, plus `evidence_text` and `extraction_confidence`
3. **NLP_NOTE_ENTITY_RELATION** — link adverse events to suspect entities, link care plan items to target conditions

> **NLP Layer (optional audit):** For full provenance tracing (span offsets, SERIOUSNESS/CAUSALITY/TIMELINE attributes), also write to `NLP_NOTE_ENTITY_MENTION` and `NLP_NOTE_ENTITY_ATTRIBUTE`. These are not required for typical clinical queries.

## Note Sections to Target

| Section | What to Extract |
|---------|----------------|
| Plan / Assessment & Plan | Care plan goals, actions, medication changes |
| Discharge Summary | Discharge instructions, follow-up appointments |
| Follow-up | Return visit instructions, repeat test orders |
| Complications | Procedural complications, adverse events |
| Safety / Incident | Fall events, near-misses, safety concerns |

## Output

Rows inserted into **ADVERSE_EVENT** and **CARE_PLAN_ITEM** tables with promoted NLP fields.
