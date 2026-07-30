-- =============================================================================
-- Cortex Search Service: DICOM Medical Image Search (BYO Vectors - MedSigLIP)
-- =============================================================================
-- Creates a Cortex Search service over the DICOM embeddings table using the
-- bring-your-own-vectors feature. Supports multi-index queries combining:
--   - Text index on LABEL (keyword search over series descriptions)
--   - Vector index on EMBEDDING (1152-dim MedSigLIP image embeddings)
--
-- The MedSigLIP SPCS service generates query vectors at search time via
-- embed_text(), which are passed via the multi_index_query parameter.
--
-- Source table: SF_CLINICAL_DB.UTILS.DICOM_EMBEDDINGS (~23k DICOM slices)
-- Vector dim:   1152 (MedSigLIP-448 image/text dual encoder)
-- Primary key:  FILE_NAME (unique per DICOM file)
-- Refresh lag:  1 day (picks up new embeddings from incremental loads)
--
-- Dependencies:
--   - SPCS service: MEDSIGLIP_448_SVC (MedSigLIP dual encoder on GPU)
--   - Source table: SF_CLINICAL_DB.UTILS.DICOM_EMBEDDINGS with VECTOR(FLOAT, 1152)
-- =============================================================================

USE ROLE SYSADMIN;
USE SCHEMA SF_CLINICAL_DB.UTILS;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- Step 1: Create Cortex Search Service with BYO Vector Index
-- =============================================================================
-- VECTOR INDEXES (EMBEDDING) tells Cortex Search to use our pre-computed
-- MedSigLIP embeddings rather than generating its own. At search time, we
-- pass a query vector via multi_index_query to search the vector index.
-- The LABEL column provides a text index for hybrid text+vector queries.
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE SF_CLINICAL_DB.UTILS.DICOM_IMAGE_SEARCH_SVC
  VECTOR INDEXES (EMBEDDING)
  PRIMARY KEY (FILE_NAME)
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 day'
AS (
  SELECT
    FILE_NAME,
    FILE_SIZE,
    SERIES_UUID,
    LABEL,
    COLLECTION,
    EMBEDDING
  FROM SF_CLINICAL_DB.UTILS.DICOM_EMBEDDINGS
);

-- =============================================================================
-- Step 2: Stored Procedure - Semantic image search using MedSigLIP + Cortex Search
-- =============================================================================
-- Flow:
--   1. Encode the user's natural language query into a 1152-dim vector
--      via the MedSigLIP SPCS service (embed_text endpoint)
--   2. Pass the vector to Cortex Search multi_index_query which searches
--      the EMBEDDING vector index
--   3. Return top K matching DICOM images with metadata
--
-- Example queries:
--   CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('cardiac CT with calcified aortic valve');
--   CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('chest CT showing pleural effusion');
--   CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('contrast-enhanced coronary angiography');
-- =============================================================================

CREATE OR REPLACE PROCEDURE SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES(
    QUERY_TEXT VARCHAR,
    TOP_K INT DEFAULT 10
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'snowflake')
HANDLER = 'run'
AS
$$
import json
from snowflake.core import Root

def run(session, query_text, top_k):
    # Encode the query text via MedSigLIP SPCS service
    safe_query = query_text.replace("'", "''")
    result = session.sql(
        f"SELECT MEDSIGLIP_448_SVC!embed_text('{safe_query}'):EMBEDDING AS vec"
    ).collect()
    query_vector = json.loads(result[0]["VEC"])

    # Search via Cortex Search multi-index query (vector search)
    root = Root(session)
    svc = (root
        .databases["SF_CLINICAL_DB"]
        .schemas["UTILS"]
        .cortex_search_services["DICOM_IMAGE_SEARCH_SVC"]
    )

    resp = svc.search(
        multi_index_query={
            "EMBEDDING": [{"vector": query_vector}]
        },
        columns=["FILE_NAME", "SERIES_UUID", "LABEL", "COLLECTION"],
        limit=top_k
    )
    return resp.results
$$;

CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('cardiac CT angiography with calcified aortic valve');

-- =============================================================================
-- Step 3: Cortex Agent - Natural language interface to DICOM image search
-- =============================================================================
-- Wraps the SEARCH_DICOM_IMAGES procedure as a tool in a Cortex Agent.
-- The agent:
--   - Receives natural language questions about cardiac/chest imaging
--   - Routes queries to the search tool (MedSigLIP encode + Cortex Search)
--   - Returns relevant DICOM files with series metadata and similarity context
--
-- Example prompts:
--   "Find CT scans showing aortic valve calcification"
--   "Show me contrast-enhanced cardiac angiography images"
--   "Which scans look like they have pleural effusion?"
-- =============================================================================

CREATE OR REPLACE AGENT SF_CLINICAL_DB.UTILS.DICOM_IMAGE_SEARCH_AGENT
  COMMENT =
$$
# DICOM Medical Image Search Agent

## Overview
A Cortex Agent that performs semantic search over cardiac and chest CT DICOM images using MedSigLIP vision-language embeddings and Cortex Search.

## How It Works
1. Accepts natural language descriptions of imaging findings or anatomy
2. Encodes the query into a 1152-dim vector via MedSigLIP (text encoder)
3. Searches pre-computed DICOM image embeddings via Cortex Search vector index
4. Returns the most visually similar DICOM files with series metadata

## Capabilities
- **Text-to-image retrieval**: Describe what you're looking for in plain English
- **Anatomy search**: Find scans by body region, structure, or orientation
- **Finding-based search**: Search by clinical finding (calcification, effusion, etc.)
- **Protocol search**: Find images by acquisition protocol or imaging phase
- **Similar-case lookup**: Describe a clinical scenario to find matching cases

## Dataset
- 10 cardiac/chest CT series (~23k DICOM slices) from public collections
- Collections: NSCLC Radiogenomics, RIDER Lung PET-CT, COVID-19 NY SBU, NLST, VAREPOP Apollo
- Embedded with google/medsiglip-448 (trained on CT, MRI, CXR, pathology, derm)

## Limitations
- Returns file-level matches, not pixel-level localization
- Operates on 2D slices (no volumetric reasoning across a full scan)
- Similarity is semantic, not diagnostic - results require clinical interpretation
$$
  PROFILE = '{"display_name": "DICOM Image Search Agent (MedSigLIP)"}'
  FROM SPECIFICATION
  $$
{
    "models": {
        "orchestration": "auto"
    },
    "orchestration": {},
    "instructions": {
        "response": "You are a medical imaging search assistant. When a user describes a clinical finding, anatomy, or imaging protocol, search the DICOM image library and return relevant results. For each result, include the LABEL (series description), COLLECTION (source dataset), and FILE_NAME. Group results by series when multiple slices from the same series are returned. Explain why the results are relevant to the user's query.",
        "orchestration": "Always use dicom_image_search for any question about medical images, CT scans, cardiac imaging, chest imaging, or clinical findings. Pass the user's description directly as the search query. If the query is vague, ask for clarification about the anatomy, finding, or protocol of interest."
    },
    "tools": [
        {
            "tool_spec": {
                "type": "generic",
                "name": "dicom_image_search",
                "description": "Semantic search over DICOM cardiac and chest CT images using MedSigLIP vision-language embeddings. Accepts a natural language description of imaging findings, anatomy, or protocols and returns the most visually similar DICOM files from the image library. Uses a dual-encoder architecture where text queries and images are embedded in the same 1152-dim vector space, enabling cross-modal retrieval without exact keyword matching.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query_text": {
                            "type": "string",
                            "description": "Natural language description of the imaging finding, anatomy, or protocol to search for"
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Number of results to return (default 10)",
                            "default": 10
                        }
                    },
                    "required": ["query_text"]
                }
            }
        }
    ],
    "skills": [],
    "tool_resources": {
        "dicom_image_search": {
            "execution_environment": {
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "identifier": "SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES",
            "name": "SEARCH_DICOM_IMAGES(VARCHAR, INT)",
            "type": "procedure"
        }
    }
}
  $$;

-- =============================================================================
-- Step 4: Quick validation
-- =============================================================================

-- Test procedure directly
CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('cardiac CT angiography with contrast');
CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('chest CT scan lung');
CALL SF_CLINICAL_DB.UTILS.SEARCH_DICOM_IMAGES('ECG-gated cardiac CT diastolic phase');

-- Test agent (run in Snowsight or via Cortex Agent API)
-- "Find scans showing aortic valve calcification"
-- "Show me contrast-enhanced coronary CT images"
-- "Which patients have chest CT scans from the NLST collection?"
SELECT
  resp.value:type::VARCHAR AS CONTENT_TYPE,
  LEFT(resp.value:text::VARCHAR, 2000) AS TEXT_CONTENT
FROM TABLE(FLATTEN(
  TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
      'SF_CLINICAL_DB.UTILS.DICOM_IMAGE_SEARCH_AGENT',
      $${ "messages": [{"role": "user", "content": [{"type": "text", "text": "Find CT scans showing aortic valve calcification"}]}] }$$,
      TRUE
    )
  ):content
)) resp;