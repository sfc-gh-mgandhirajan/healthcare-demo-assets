---
name: hcls-cross-aiml-industrymodels
description: "Catalog and manage fine-tuned industry models (ICD coding, clinical NER, etc.) for use across health sciences skills. Triggers: fine-tuned model, industry model, custom model, FINETUNE."
platform_affinities:
  produces:
    - fine-tuned models (via Cortex Fine-Tuning)
  benefits_from:
    - skill: machine-learning
      when: "training new fine-tuned models or evaluating model performance"
    - skill: cortex-ai-functions
      when: "using CORTEX.COMPLETE() with fine-tuned model names"
---

# Industry Models Catalog

Catalog and manage fine-tuned industry models for health sciences workflows on Snowflake. Provides discovery, inspection, availability checks, and integration patterns so other skills can consume fine-tuned models without reimplementing boilerplate.

## When to Use

Invoke this skill when:
- Listing available fine-tuned models in the account
- Inspecting a specific fine-tuned model (status, base model, training metadata)
- Checking whether a fine-tuned model exists before using it in a workflow
- Registering or creating a new fine-tuned model for an industry use case
- Understanding how to integrate a fine-tuned model into an existing skill

## Model Catalog Workflow

### List Available Fine-Tuned Models

```sql
SHOW MODELS IN DATABASE <DB>;

SELECT "name", "model_type", "database_name", "schema_name",
       "default_version_name", "created_on", "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "model_type" = 'CORTEX_FINETUNED';
```

Returns all fine-tuned model objects the current role has access to. Does not require a running warehouse.

### Inspect a Specific Model

```sql
SHOW VERSIONS IN MODEL <DB>.<SCHEMA>.<model_name>;
```

Returns version details including the default version name.

### Check Model Availability

```sql
SHOW MODELS LIKE '<model_name>' IN SCHEMA <DB>.<SCHEMA>;
```

If the query returns a row, the model is ready for use with `SNOWFLAKE.CORTEX.COMPLETE()`.

## For Consuming Skills

Skills that conditionally use fine-tuned models should implement a preflight gate asking whether a model is available, with an option to invoke this skill for creation. See `normalization-conditions-diagnostics` (Preflight: Fine-Tuned Model Gate) for the reference implementation.

## Creating a New Fine-Tuned Model

### Step 1: Gather Requirements

Ask the user:

```
To create a fine-tuned model for health sciences, I need:

1. Target task — which model type from the Supported Model Types table?
   (ICD-10-CM coding, Clinical NER, RxNorm mapping, MedDRA coding, LOINC mapping, Oncology staging)
2. Base model preference — see supported models table below
3. Clinical NLP data model location — database.schema containing CONDITION, MEDICATION_REQUEST, etc.
   (default: UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE; or custom training data location)
4. Target database/schema — where to create the model and reshaping views
   (default: same database, e.g. UNSTRUCTURED_HEALTHDATA.ML_FINETUNING)
```

Requires `SNOWFLAKE.CORTEX_USER` database role and `CREATE MODEL` privilege on the target schema.

**STOP**: Confirm requirements before proceeding.

### Step 2: Prepare Training Data

Training data is derived from existing clinical tables where codes are already populated (ground truth). Rows with `source = 'EHR_STRUCTURED'` provide verified labels. The `evidence_text` field contains the clinical context snippet that triggered extraction — this serves as the prompt, with a task instruction suffix appended. The raw code serves as the completion.

Requires `CREATE VIEW` on the target schema.

#### Source Table Mapping

| Model Type | Prompt Source | Completion Source | Filter |
|---|---|---|---|
| ICD-10-CM Coding | `CONDITION.evidence_text` | `CONDITION.code` | `code_system = 'ICD10CM'` |
| RxNorm Mapping | `MEDICATION_REQUEST.evidence_text` | `MEDICATION_REQUEST.medication_code` | `medication_system = 'RXNORM'` |
| LOINC Mapping | `OBSERVATION.evidence_text` | `OBSERVATION.code` | `code_system = 'LOINC'` |
| MedDRA Coding | `ADVERSE_EVENT.evidence_text` | `ADVERSE_EVENT.event_code` | `event_system = 'MEDDRA'` |

#### ICD-10-CM Training View

```sql
CREATE OR REPLACE VIEW <DB>.<SCHEMA>.ICD10CM_TRAINING_FT AS
SELECT
  c.evidence_text
    || ' Given this clinical text, assign the ICD10-CM diagnosis code in this format ONLY: X##.#. Do not provide explanation '
    AS prompt,
  c.code AS completion
FROM CONDITION c
WHERE c.code IS NOT NULL
  AND c.code_system = 'ICD10CM'
  AND c.source = 'EHR_STRUCTURED'
  AND c.evidence_text IS NOT NULL;
```

#### RxNorm Training View

```sql
CREATE OR REPLACE VIEW <DB>.<SCHEMA>.RXNORM_TRAINING_FT AS
SELECT
  m.evidence_text
    || ' Given this clinical text, assign the RxNorm code. Do not provide explanation '
    AS prompt,
  m.medication_code AS completion
FROM MEDICATION_REQUEST m
WHERE m.medication_code IS NOT NULL
  AND m.medication_system = 'RXNORM'
  AND m.source = 'EHR_STRUCTURED'
  AND m.evidence_text IS NOT NULL;
```

#### Train / Validation Split

Split deterministically (80/20) for the FINETUNE call:

```sql
-- Training (80%)
SELECT prompt, completion FROM <DB>.<SCHEMA>.ICD10CM_TRAINING_FT
WHERE MOD(ABS(HASH(prompt)), 5) != 0;

-- Validation (20%)
SELECT prompt, completion FROM <DB>.<SCHEMA>.ICD10CM_TRAINING_FT
WHERE MOD(ABS(HASH(prompt)), 5) = 0;
```

#### Custom / External Training Data

If the user brings training data from outside the clinical NLP data model, create a reshaping view:

```sql
CREATE OR REPLACE VIEW <DB>.<SCHEMA>.<TRAINING_VIEW_FT> AS
SELECT
  <text_column> || ' <instruction_suffix> ' AS prompt,
  <label_column> AS completion
FROM <source_table>
WHERE <label_column> IS NOT NULL;
```

The training prompt format **must match** the inference prompt format used by the consuming skill. For ICD-10-CM coding, align with the prompt structure in `normalization-conditions-diagnostics` Step 1.5A.

**STOP**: Confirm prompt format and training data source with user before proceeding.

### Step 3: Launch Fine-Tuning Job

```sql
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE',
    '<model_output_name>',
    '<base_model>',
    'SELECT prompt, completion FROM <DB>.<SCHEMA>.<training_view>',
    'SELECT prompt, completion FROM <DB>.<SCHEMA>.<validation_view>'
);
```

The call returns a `job_id` — save it for monitoring.

#### Supported Base Models

| Model | Context Window |
|-------|---------------|
| `mistral-7b` | 32k |
| `mixtral-8x7b` | 32k |
| `llama3-8b` | 8k |
| `llama3-70b` | 8k |
| `llama3.1-8b` | 24k |
| `llama3.1-70b` | 8k |

For terminology coding tasks (ICD, RxNorm, LOINC), `llama3-8b` or `llama3.1-8b` is typically sufficient. For complex extraction (NER, oncology staging), consider `llama3-70b` or `llama3.1-70b`.

### Step 4: Monitor Progress

```sql
SELECT id, status, progress
FROM TABLE(SNOWFLAKE.CORTEX.FINETUNE('SHOW'))
WHERE id = '<job_id>';
```

Typically completes in 5-30 minutes depending on dataset size. Poll periodically until `status = 'SUCCESS'`.

### Step 5: Validate and Grant Access

Test with a domain-specific prompt that matches the training format:

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    '<model_output_name>',
    'Assign the most specific ICD-10-CM code for this clinical condition. Display: "Essential hypertension"'
) AS test_output;
```

Verify the output matches expected format (e.g., `{"code": "I10", "code_system": "ICD-10-CM", "confidence": 0.98}`).

Grant access if other roles need to use the model:

```sql
GRANT USAGE ON MODEL <DB>.<SCHEMA>.<model_name> TO ROLE <role_name>;
```

**STOP**: Confirm the fine-tuned model produces correct output before registering it for use by downstream skills.

## Supported Model Types

| Model Type | Use Case | Example Model Name | Consuming Skill |
|------------|----------|-------------------|-----------------|
| ICD-10-CM Coding | Map clinical conditions to ICD-10-CM codes | `FINETUNE_llama38b_ICDCODES` | `normalization-conditions-diagnostics` |
| Clinical NER | Extract clinical entities from unstructured text | `FINETUNE_llama38b_CLINICNER` | `extraction-conditions-diagnostics` |
| RxNorm Mapping | Normalize medication mentions to RxNorm codes | `FINETUNE_llama38b_RXNORM` | `normalization-therapeutics` |
| MedDRA Coding | Map adverse events to MedDRA preferred terms | `FINETUNE_llama38b_MEDDRA` | `normalization-safety-care-planning` |
| LOINC Mapping | Normalize lab observations to LOINC codes | `FINETUNE_llama38b_LOINC` | `normalization-observations` |
| Oncology Staging | Classify tumor staging from pathology text | `FINETUNE_llama38b_STAGING` | `extraction-oncology` |

## Integration Points

### How Other Skills Consume Fine-Tuned Models

Fine-tuned models are consumed via `SNOWFLAKE.CORTEX.COMPLETE()` with the model name as the first argument:

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    $FINETUNED_MODEL_NAME,
    <prompt>
) AS result;
```

### Integration Pattern for Consuming Skills

When a consuming skill calls the fine-tuned model, the inference prompt **must match the training format** (evidence_text + instruction suffix → bare code). Use a CTE to run inference and apply results in a single statement:

```sql
WITH unmatched AS (
    SELECT entity_id, display, evidence_text
    FROM <entity_table>
    WHERE code IS NULL AND display IS NOT NULL
),
predictions AS (
    SELECT
        u.entity_id,
        TRIM(SNOWFLAKE.CORTEX.COMPLETE(
            $FINETUNED_MODEL_NAME,
            COALESCE(u.evidence_text, u.display)
                || ' <instruction suffix matching training prompt> '
        )) AS predicted_code
    FROM unmatched u
)
UPDATE <entity_table> t
SET t.code = p.predicted_code,
    t.code_system = '<target_code_system>'
FROM predictions p
WHERE t.entity_id = p.entity_id
  AND p.predicted_code IS NOT NULL
  AND p.predicted_code RLIKE '<format_regex>';
```

Use a format-appropriate regex for validation (e.g., `'^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$'` for ICD-10-CM).

This pattern is used by `normalization-conditions-diagnostics` Step 1.5 and can be adapted by any normalization sub-skill.

## Output

- List of available fine-tuned models with status and metadata
- Model availability confirmation for downstream skills
- New fine-tuning job launched and monitored to completion
- Integration guidance for consuming skills

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| "Unknown model" error in `CORTEX.COMPLETE` | Wrong model name | Run `SHOW MODELS LIKE '%<name>%'` to find the exact model name; do not use the fine-tuning job ID |
| Model produces wrong or invalid codes | Prompt format mismatch between training and inference | Ensure inference prompt uses same `evidence_text + instruction suffix` format as training views |
| Permission error on `FINETUNE('CREATE')` | Missing privileges | `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <role>; GRANT CREATE MODEL ON SCHEMA <DB>.<SCHEMA> TO ROLE <role>;` |
| Permission error on `CORTEX.COMPLETE` with fine-tuned model | Model not granted to role | `GRANT USAGE ON MODEL <DB>.<SCHEMA>.<model_name> TO ROLE <role>;` |
| Model returns extra text beyond the code | Instruction suffix not strict enough | Ensure suffix includes "Do not provide explanation" and apply `TRIM()` + regex validation on output |
| Fine-tuning job stuck at 0% | Training data issues | Verify training table has `prompt` and `completion` columns with non-null values; check row count is sufficient (minimum ~100 rows recommended) |
