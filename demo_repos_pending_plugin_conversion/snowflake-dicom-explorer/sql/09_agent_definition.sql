-- =============================================================================
-- 09: Agent Definition
-- DICOM_IMAGING_AGENT with dual tools:
--   1. cortex_search → text metadata search (DICOM_STUDY_SEARCH)
--   2. dicom_image_search → MedSigLIP vision-language search (SEARCH_DICOM_IMAGES SP)
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE EW_IMAGING_DB;
USE SCHEMA EXPLORER;
USE WAREHOUSE COMPUTE_WH;

-- Recreate SEARCH_DICOM_IMAGES in EW_IMAGING_DB.EXPLORER (self-contained demo)
CREATE OR REPLACE PROCEDURE EW_IMAGING_DB.EXPLORER.SEARCH_DICOM_IMAGES(
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
    safe_query = query_text.replace("'", "''")
    result = session.sql(
        f"SELECT SF_CLINICAL_DB.UTILS.MEDSIGLIP_448_SVC!embed_text('{safe_query}'):EMBEDDING AS vec"
    ).collect()
    query_vector = json.loads(result[0]["VEC"])

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

-- Create the agent with both text search and image search tools
CREATE OR REPLACE AGENT EW_IMAGING_DB.EXPLORER.DICOM_IMAGING_AGENT
  COMMENT = 'Cardiac imaging intelligence agent with text metadata search and MedSigLIP vision-language image search.'
  PROFILE = '{"display_name": "DICOM Imaging Agent"}'
  FROM SPECIFICATION
  $$
{
    "models": {
        "orchestration": "auto"
    },
    "orchestration": {},
    "instructions": {
        "response": "You are a cardiac imaging data analyst for a TAVR (transcatheter aortic valve replacement) program. You have access to 2,848 DICOM files across 10 CT series, 10 patients, and 5 collections. Answer questions concisely with specific numbers and file references. When showing search results, group by series and include the collection name.",
        "orchestration": "Route queries based on intent: Use dicom_metadata_search for metadata questions (file counts, manufacturers, protocols, slice thickness, modality). Use dicom_image_search for visual/clinical finding queries (calcification, effusion, anatomy, contrast-enhanced). If ambiguous, prefer dicom_image_search for clinical terms and dicom_metadata_search for technical/protocol terms."
    },
    "tools": [
        {
            "tool_spec": {
                "type": "cortex_search",
                "name": "dicom_metadata_search"
            }
        },
        {
            "tool_spec": {
                "type": "generic",
                "name": "dicom_image_search",
                "description": "Semantic search over DICOM cardiac and chest CT images using MedSigLIP vision-language embeddings. Finds images by visual similarity to natural language descriptions of clinical findings, anatomy, or imaging protocols. Returns the most similar DICOM files with their series label and collection.",
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
        "dicom_metadata_search": {
            "search_service": "EW_IMAGING_DB.EXPLORER.DICOM_STUDY_SEARCH"
        },
        "dicom_image_search": {
            "execution_environment": {
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "identifier": "EW_IMAGING_DB.EXPLORER.SEARCH_DICOM_IMAGES",
            "name": "SEARCH_DICOM_IMAGES(VARCHAR, INT)",
            "type": "procedure"
        }
    }
}
  $$;
