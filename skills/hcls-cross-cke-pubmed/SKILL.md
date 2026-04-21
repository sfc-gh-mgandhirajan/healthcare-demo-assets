---
name: hcls-cross-cke-pubmed
description: "Cortex Knowledge Extension: PubMed Biomedical Research Corpus. RAG-based semantic search across PubMed biomedical literature via Snowflake Marketplace shared Cortex Search Service. Triggers: PubMed, biomedical literature, drug mechanism, clinical evidence, research papers, medical literature, literature review, biomedical research, drug-event association, radiology research."
platform_affinities:
  produces: [cortex_search_service]
  benefits_from: []
---

# CKE: PubMed Biomedical Research Corpus

This skill provides access to the **PubMed Biomedical Research Corpus** Cortex Knowledge Extension (CKE) from the Snowflake Marketplace. It is a shared Cortex Search Service that enables RAG-based semantic search across PubMed biomedical literature directly in Snowflake -- no data is copied into your account.

## Service Discovery (REQUIRED -- Run Before Any Query)

Before executing any PubMed search, run the following anonymous block to dynamically discover the imported database and Cortex Search Service name. This is role-scoped -- it only finds databases the current role has privileges on.

```sql
DECLARE
  res RESULTSET;                       -- Reusable resultset for EXECUTE IMMEDIATE calls
  db_name VARCHAR DEFAULT NULL;        -- Will hold the imported database name if found
  service_fqn VARCHAR DEFAULT NULL;    -- Will hold the fully qualified Cortex Search Service name
BEGIN
  -- Step 1: List all databases visible to the current role
  -- SHOW DATABASES is role-scoped, so it only returns databases the user has privileges on
  res := (EXECUTE IMMEDIATE 'SHOW DATABASES IN ACCOUNT');

  -- Step 2: Try to find the PubMed CKE imported database by matching the origin
  -- If no matching row exists (not imported or no privileges), the SELECT INTO will fail
  BEGIN
    SELECT "name" INTO :db_name
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
      WHERE "kind" = 'IMPORTED DATABASE'
      AND "origin" LIKE '%PUBMED_CKE_DB%'
      LIMIT 1;
  EXCEPTION
    -- Catch the error when no rows are returned and keep db_name as NULL
    WHEN OTHER THEN
      db_name := NULL;
  END;

  IF (db_name IS NOT NULL) THEN
    -- Step 3: Database found — list all Cortex Search Services within it
    res := (EXECUTE IMMEDIATE 'SHOW CORTEX SEARCH SERVICES IN DATABASE ' || db_name);

    -- Step 4: Build the fully qualified name (database.schema.service) from the first result
    BEGIN
      SELECT "database_name" || '.' || "schema_name" || '.' || "name" INTO :service_fqn
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        LIMIT 1;
    EXCEPTION
      -- No Cortex Search Service exists in this database
      WHEN OTHER THEN
        service_fqn := NULL;
    END;

    IF (service_fqn IS NOT NULL) THEN
      -- Step 5a: Cortex Search Service found — return its fully qualified name
      res := (EXECUTE IMMEDIATE 'SELECT \'' || service_fqn || '\' AS CORTEX_SEARCH_SERVICE');
      LET c CURSOR FOR SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
      OPEN c;
      RETURN TABLE(c);
    ELSE
      -- Step 5b: Database exists but contains no Cortex Search Service
      res := (EXECUTE IMMEDIATE 'SELECT \'' || db_name || '\' AS NAME, \'No Cortex Search Service found in this database.\' AS STATUS');
      LET c2 CURSOR FOR SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
      OPEN c2;
      RETURN TABLE(c2);
    END IF;
  ELSE
    -- Step 3 (alternate): Database not found — either not imported or user lacks privileges
    -- SHOW DATABASES is role-scoped, so a missing result means one of these two scenarios
    LET msg CURSOR FOR
      SELECT NULL AS NAME, FALSE AS AVAILABLE_TO_YOU,
             'The PubMed Biomedical Research Corpus Cortex Knowledge Extension is either not imported or your current role (' || CURRENT_ROLE() || ') does not have privileges to see it.' AS ERROR_MESSAGE,
             'Ask your account administrator (ACCOUNTADMIN) to either: (1) Import this free listing via Snowsight > Data Products > Marketplace > Search "PubMed Biomedical Research Corpus" > Click "Get", or (2) If already imported, grant access with: GRANT IMPORTED PRIVILEGES ON DATABASE <your_pubmed_database> TO ROLE ' || CURRENT_ROLE() || ';' AS GUIDANCE;
    OPEN msg;
    RETURN TABLE(msg);
  END IF;
END;
```

| Result | Status | Action |
|--------|--------|--------|
| Returns `CORTEX_SEARCH_SERVICE` column with a fully qualified name | READY | Use the returned value as the service name in all query patterns below |
| Returns `NAME` + `STATUS` columns ("No Cortex Search Service found") | PARTIAL | Database is imported but service is missing -- contact your account administrator |
| Returns `AVAILABLE_TO_YOU = FALSE` with `ERROR_MESSAGE` and `GUIDANCE` | MISSING | Follow the guidance to install the listing or grant privileges |

### Fallback (When MISSING)

If the listing is not installed and the user cannot install it now:
- **Inform the user**: "PubMed CKE is not available in this account. Biomedical literature search is unavailable."
- **Continue without PubMed enrichment** -- domain skills should still function for their primary task
- **Suggest alternative**: "You can search PubMed manually at https://pubmed.ncbi.nlm.nih.gov/ and paste relevant abstracts into the conversation"

### Auto-Detection for Domain Skills

When a domain skill (pharmacovigilance, clinical-nlp, etc.) wants to invoke this CKE:
1. Run the Service Discovery block above
2. If READY -- use the returned service name to execute CKE queries and enrich the domain result
3. If MISSING or PARTIAL -- skip enrichment, log a note: "PubMed CKE not available -- skipping literature enrichment"
4. Never fail the parent skill just because a CKE is unavailable

## Marketplace Details

| Field | Value |
|-------|-------|
| **Listing ID** | `GZSTZ67BY9OQW` |
| **Service Name** | Returned by Service Discovery block as `CORTEX_SEARCH_SERVICE` |
| **Type** | Shared Cortex Search Service |
| **Columns** | `chunk`, `document_title`, `source_url` |

## Setup (One-Time)

1. Navigate to **Snowflake Marketplace** and search for `PubMed Biomedical Research Corpus` (or listing `GZSTZ67BY9OQW`)
2. Click **Get** to install -- no data is copied; a shared Cortex Search Service appears in your account
3. Run the Service Discovery block above to confirm the service is available and get its fully qualified name

## Query Patterns

> In the patterns below, `<SERVICE_FQN>` is a placeholder for the fully qualified Cortex Search Service name returned by the Service Discovery block above (e.g., `PUBMED_BIOMEDICAL_RESEARCH_CORPUS.OA_COMM.PUBMED_OA_CKE_SEARCH_SERVICE`).

### Basic Search (SQL)

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  '<SERVICE_FQN>',
  '{"query": "<natural language question>", "columns": ["chunk", "document_title", "source_url"]}'
);
```

### Cortex Agent API Tool Spec

```json
{
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "pubmed_search",
        "spec": {
          "service_name": "<SERVICE_FQN>",
          "max_results": 5,
          "title_column": "document_title",
          "id_column": "source_url"
        }
      }
    }
  ]
}
```

## Use Cases by Domain Skill

This CKE is designed to be invoked on-demand by domain skills when evidence grounding adds value. The calling skill decides when and what to query.

| Domain Skill | When to Invoke `$cke-pubmed` | Example Query |
|---|---|---|
| `$pharmacovigilance` | After signal detection (PRR/ROR), search for published drug-event associations and mechanism evidence | `"{drug_name} {reaction} adverse event mechanism"` |
| `$healthcare-imaging` (dicom-analytics) | When enriching imaging analytics with radiology research context, imaging biomarkers, diagnostic criteria | `"pulmonary nodule CT screening Lung-RADS classification"` |
| `$clinical-nlp` | Entity disambiguation, terminology validation, grounding LLM prompts with biomedical context | `"drug interaction classification clinical text"` |
| `$scientific-problem-selection` | Literature landscape review, novelty assessment, gap identification for research ideas | `"CRISPR base editing sickle cell disease clinical outcomes"` |

## Integration Patterns

### Pattern 1: Signal Enrichment (Pharmacovigilance)

Annotate FAERS safety signals with published literature evidence:

```sql
WITH faers_signals AS (
  SELECT drug_name, reaction_pt, prr, ror
  FROM drug_safety_signals
  WHERE prr > 2 AND ror > 2
),
literature_evidence AS (
  SELECT
    s.drug_name,
    s.reaction_pt,
    s.prr,
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      '<SERVICE_FQN>',
      '{"query": "' || s.drug_name || ' ' || s.reaction_pt || ' adverse event mechanism", "columns": ["chunk", "document_title", "source_url"]}'
    ) AS pubmed_evidence
  FROM faers_signals s
)
SELECT * FROM literature_evidence;
```

### Pattern 2: LLM Prompt Grounding (Clinical NLP)

Ground Cortex AI extraction prompts with biomedical context:

```sql
WITH pubmed_context AS (
  SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<SERVICE_FQN>',
    '{"query": "drug interaction classification clinical text", "columns": ["chunk"]}'
  ) AS literature_context
)
SELECT
  n.note_id,
  SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-70b',
    'Using this biomedical reference context: ' || p.literature_context::STRING ||
    ' Extract medications and potential drug interactions from this clinical note: ' || n.note_text
  ) AS enriched_extraction
FROM clinical_notes n CROSS JOIN pubmed_context p
LIMIT 10;
```

### Pattern 3: Imaging Research Context (DICOM Analytics)

Enrich radiology findings with published evidence:

```sql
SELECT
  r.study_uid,
  r.key_findings,
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    '<SERVICE_FQN>',
    '{"query": "' || r.key_findings::STRING || ' radiology evidence", "columns": ["chunk", "document_title", "source_url"]}'
  ) AS literature_context
FROM radiology_findings r
WHERE r.critical_findings IS NOT NULL
LIMIT 10;
```

### Pattern 4: Research Landscape Survey (Scientific Problem Selection)

When evaluating a research idea, survey the literature:

1. Search for the core topic to gauge publication volume and recency
2. Search for the specific approach/method to assess novelty
3. Search for competing approaches to understand alternatives
4. Use results to inform problem evaluation and risk assessment
