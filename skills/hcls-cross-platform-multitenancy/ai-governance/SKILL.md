---
name: ai-governance
description: "AI governance for multi-tenant platforms: Cortex Guard, AI Guardrails, model access controls, AI cost attribution, data residency for AI workloads, responsible AI, LLM safety, prompt injection prevention, content filtering, model registry access. Triggers: AI governance, Cortex Guard, AI Guardrails, model access, AI cost, data residency, responsible AI, LLM safety, prompt injection, content filtering, model registry, Cortex functions access, AI audit."
platform_affinities:
  produces:
    - AI guardrail stored procedures
    - model access control grants
    - AI cost attribution queries
  benefits_from:
    - skill: cortex-ai-functions
      when: "always — Cortex Guard, AI_COMPLETE, and AI function governance are the core platform capability here"
    - skill: data-governance
      when: "PHI or PII may flow through AI prompts and requires masking before LLM processing"
---

# AI Governance & Cortex Guard

## When to Use

**Load** this skill when the customer uses or plans to use Snowflake Cortex AI features (LLM functions, ML models, embeddings, Cortex Search, Cortex Agents) in a multi-tenant platform and needs governance controls.

---

## Step 1: Understand AI Usage

Gather:

| Input | Description |
|-------|-------------|
| Which Cortex features are used? | AI_COMPLETE, AI_EXTRACT, AI_CLASSIFY, Cortex Search, Cortex Agents, ML Functions |
| AI workload types | Text analytics, document processing, embeddings, fine-tuning, custom models |
| Tenant access to AI | All tenants, premium tier only, platform-internal only |
| Sensitive data in AI prompts? | PII, financial data, trade secrets passed to LLMs |
| Compliance for AI | FDA, HIPAA, GDPR AI Act, SOC2 for AI outputs |
| AI cost concerns | Need to attribute AI costs per tenant |

---

## Step 2: Cortex Guard — Content Safety

Cortex Guard provides content safety filtering for LLM inputs and outputs.

### Enable Cortex Guard

Use `SNOWFLAKE.CORTEX.GUARD` to filter unsafe content.

**Note**: The guard model name (e.g., `meta-llama/llama-guard-4-12b`) may vary by region. Consult [Snowflake Cortex Guard documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-guard) for supported models in your region.

```sql
SELECT
    {{INPUT_COLUMN}} AS original_text,
    SNOWFLAKE.CORTEX.GUARD(
        '{{GUARD_MODEL}}',
        [{'role': 'user', 'content': {{INPUT_COLUMN}}}]
    ) AS safety_result
FROM {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}};
```

### Cortex Guard Categories

Cortex Guard evaluates content against safety categories:
- **S1**: Violent crimes
- **S2**: Non-violent crimes
- **S3**: Sex-related crimes
- **S4**: Child sexual exploitation
- **S5**: Defamation
- **S6**: Specialized advice (legal, medical, financial)
- **S7**: Privacy violations
- **S8**: Intellectual property
- **S9**: Indiscriminate weapons
- **S10**: Hate speech
- **S11**: Suicide/self-harm
- **S12**: Sexual content
- **S13**: Elections
- **S14**: Code interpreter abuse

### Implementing Guard in AI Pipelines

Create a wrapper stored procedure that enforces Cortex Guard before/after LLM calls:

```sql
CREATE OR REPLACE PROCEDURE {{DATA_DB}}.{{SCHEMA}}.SAFE_AI_COMPLETE(
    model_name VARCHAR,
    prompt VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
BEGIN
    LET guard_input VARIANT := (
        SELECT SNOWFLAKE.CORTEX.GUARD(
            '{{GUARD_MODEL}}',
            [{'role': 'user', 'content': :prompt}]
        )
    );
    IF (:guard_input:safe = TRUE) THEN
        LET response VARCHAR := (
            SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(:model_name, :prompt)
        );
        LET guard_output VARIANT := (
            SELECT SNOWFLAKE.CORTEX.GUARD(
                '{{GUARD_MODEL}}',
                [{'role': 'assistant', 'content': :response}]
            )
        );
        IF (:guard_output:safe = TRUE) THEN
            RETURN OBJECT_CONSTRUCT('response', :response, 'safe', TRUE);
        ELSE
            RETURN OBJECT_CONSTRUCT('response', 'Output flagged by safety filter', 'safe', FALSE, 'categories', :guard_output:categories);
        END IF;
    ELSE
        RETURN OBJECT_CONSTRUCT('response', 'Input flagged by safety filter', 'safe', FALSE, 'categories', :guard_input:categories);
    END IF;
END;
```

---

## Step 3: Model Access Controls

### RBAC for Cortex Functions

Cortex built-in AI functions are accessed via the `SNOWFLAKE.CORTEX_USER` database role. Grant this to tenant roles:

```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE {{TENANT_ROLE}};
```

This grants access to all Cortex AI functions (AI_COMPLETE, AI_EXTRACT, AI_CLASSIFY, AI_SUMMARIZE, etc.). To restrict which Cortex features a tenant can use, enforce at the application layer via wrapper procedures that check the tenant's service tier before calling the underlying Cortex function.

### Restrict AI Model Access by Tier

| Service Tier | Allowed Models | Cortex Features |
|-------------|---------------|-----------------|
| Basic | Smaller models (e.g., llama3.1-8b) | AI_COMPLETE, AI_EXTRACT |
| Premium | Mid-range models (e.g., llama3.1-70b) | + Cortex Search, AI_CLASSIFY |
| Enterprise | All models (e.g., claude-3.5-sonnet) | + Cortex Agents, Fine-tuning |

Enforce via application-layer controls or wrapper functions that check the calling role's tier.

### Model Registry Access

If using Snowflake Model Registry for custom models:

```sql
GRANT USAGE ON MODEL {{ML_DB}}.{{SCHEMA}}.{{MODEL_NAME}}
    TO ROLE {{AUTHORIZED_ROLE}};
```

---

## Step 4: AI Cost Attribution

### Tag AI Warehouses

```sql
ALTER WAREHOUSE {{AI_WAREHOUSE}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.COST_CENTER = 'AI_WORKLOADS';
ALTER WAREHOUSE {{AI_WAREHOUSE}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.TENANT_ID = '{{TENANT_ID}}';
```

### Track Cortex AI Consumption

```sql
SELECT
    start_time::DATE AS usage_date,
    service_type,
    SUM(credits_used) AS ai_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type IN ('AI_SERVICES', 'SNOWPARK_CONTAINER_SERVICES')
    AND start_time >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
GROUP BY usage_date, service_type
ORDER BY usage_date DESC;
```

### Per-Tenant AI Cost Allocation

Use **query tags** to attribute AI costs to tenants:

```sql
ALTER SESSION SET QUERY_TAG = '{"tenant_id": "{{TENANT_ID}}", "workload": "ai"}';

SELECT SNOWFLAKE.CORTEX.AI_COMPLETE('{{MODEL}}', {{PROMPT}});
```

Then attribute costs:

```sql
SELECT
    PARSE_JSON(query_tag):tenant_id::VARCHAR AS tenant_id,
    SUM(credits_used_cloud_services) AS ai_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE PARSE_JSON(query_tag):workload::VARCHAR = 'ai'
    AND start_time >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
GROUP BY tenant_id
ORDER BY ai_credits DESC;
```

---

## Step 5: Data Residency for AI

### Regional Considerations

- Cortex LLM functions process data in the region where the Snowflake account resides
- Cross-region AI calls may have compliance implications (GDPR, data sovereignty)
- Use **database replication** to keep data and AI processing in the required region

### Data Residency Controls

```sql
CREATE TAG IF NOT EXISTS {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_RESIDENCY
    ALLOWED_VALUES 'US', 'EU', 'APAC', 'RESTRICTED'
    COMMENT = 'Data residency region for compliance';

ALTER TABLE {{DATA_DB}}.{{SCHEMA}}.{{TABLE_NAME}}
    SET TAG {{GOVERNANCE_DB}}.{{TAG_SCHEMA}}.DATA_RESIDENCY = '{{REGION}}';
```

---

## Step 6: AI Audit & Compliance

### Audit AI Usage

```sql
SELECT
    user_name,
    role_name,
    query_id,
    start_time,
    SUBSTR(query_text, 1, 200) AS query_preview,
    total_elapsed_time / 1000 AS elapsed_sec,
    credits_used_cloud_services
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE (query_text ILIKE '%CORTEX%' OR query_text ILIKE '%AI_COMPLETE%'
    OR query_text ILIKE '%AI_EXTRACT%' OR query_text ILIKE '%AI_CLASSIFY%')
    AND start_time >= DATEADD('DAY', -{{DAYS}}, CURRENT_TIMESTAMP)
ORDER BY start_time DESC;
```

---

## Output

**MANDATORY STOPPING POINT**: Present the AI governance plan for customer approval.

Deliver:
1. **Cortex Guard integration plan** — where to apply input/output safety filters
2. **Model access control matrix** — which roles get access to which models/functions
3. **AI cost attribution model** — tag strategy and attribution queries
4. **Data residency documentation** — region mapping for AI workloads
5. **AI audit queries** — ready-to-run monitoring for Cortex usage
6. **Responsible AI policy** — guidelines for LLM usage in multi-tenant context
7. **Edition requirements** — see table below

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Cortex AI functions (AI_COMPLETE, etc.) | Standard+ |
| Cortex Guard | Standard+ (model availability varies by region) |
| Cortex Search Service | Standard+ |
| Model Registry | Standard+ |
| Snowpark Container Services (custom models) | Standard+ (with compute pool) |
| METERING_DAILY_HISTORY for AI cost tracking | **Enterprise** (ACCOUNT_USAGE) |

After approval, route to:
- `cortex-agents-multitenancy` if using Cortex Agents
- `cost-attribution` for integrated FinOps
