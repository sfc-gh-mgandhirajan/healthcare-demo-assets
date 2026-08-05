-- ============================================================
-- MedGemma 4B Deployment Prerequisites
-- ============================================================
-- Run this BEFORE setup_data.sql to create the infrastructure
-- required to import MedGemma from Hugging Face and deploy it
-- to Snowpark Container Services (SPCS).
--
-- After running this script, use the Snowsight UI to import
-- and deploy MedGemma (see README.md for step-by-step guide).
-- ============================================================

-- >>> CONFIGURE THESE FOR YOUR ENVIRONMENT <<<
SET MY_DB = 'DEMO_DB';
SET MY_SCHEMA = 'HIMSS_DEMO';

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- 1. COMPUTE POOL (GPU)
-- ============================================================
-- GPU_NV_S provides 1 NVIDIA GPU — sufficient for MedGemma 4B.
-- Adjust MIN/MAX_NODES based on your concurrency needs.

CREATE COMPUTE POOL IF NOT EXISTS MEDGEMMA_GPU_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = GPU_NV_S
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 600;

GRANT USAGE ON COMPUTE POOL MEDGEMMA_GPU_POOL TO ROLE SYSADMIN;

-- ============================================================
-- 2. HUGGING FACE TOKEN SECRET (for gated model access)
-- ============================================================
-- MedGemma is a gated model on Hugging Face. You must:
--   a) Accept the license at https://huggingface.co/google/medgemma-4b-it
--   b) Generate a Hugging Face access token at https://huggingface.co/settings/tokens
--   c) Replace 'hf_YOUR_TOKEN_HERE' below with your actual token

USE DATABASE IDENTIFIER($MY_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($MY_SCHEMA);
USE SCHEMA IDENTIFIER($MY_SCHEMA);

CREATE SECRET IF NOT EXISTS HF_TOKEN_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'hf_YOUR_TOKEN_HERE';    -- <-- REPLACE with your HF token

-- ============================================================
-- 3. PAT SECRET (for stored procedure → SPCS authentication)
-- ============================================================
-- The MedGemma stored procedure authenticates to the SPCS
-- service endpoint using a Snowflake PAT (Programmatic Access Token).
-- Generate one at: Account Menu → Security → Programmatic Access Tokens
--
-- Replace 'your_snowflake_pat_here' with your actual PAT.

CREATE SECRET IF NOT EXISTS MEDGEMMA_PAT_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'your_snowflake_pat_here';    -- <-- REPLACE with your PAT

-- ============================================================
-- 4. NETWORK RULE + EXTERNAL ACCESS INTEGRATION
-- ============================================================
-- The stored procedure calls the SPCS REST endpoint from within
-- a warehouse execution context. This requires an EAI.
--
-- The VALUE_LIST should include your SPCS service endpoint domain.
-- After deploying MedGemma (Step 6 below), run:
--   SHOW ENDPOINTS IN SERVICE <your_service_name>;
-- Then update the network rule with the actual ingress_url domain.

CREATE OR REPLACE NETWORK RULE MEDGEMMA_SPCS_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('0.0.0.0:443', '0.0.0.0:80');    -- Permissive initially; tighten after deployment

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MEDGEMMA_SPCS_EAI
    ALLOWED_NETWORK_RULES = (MEDGEMMA_SPCS_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (MEDGEMMA_PAT_SECRET)  -- Resolved from current DB.SCHEMA context
    ENABLED = TRUE;

-- ============================================================
-- 5. IMAGE STAGE (for medical images)
-- ============================================================
-- The stored procedure reads medical images from this stage.
-- Upload your images here after creating the stage.

CREATE DATABASE IF NOT EXISTS MEDGEMMA_DEMO;
CREATE SCHEMA IF NOT EXISTS MEDGEMMA_DEMO.PUBLIC;

CREATE STAGE IF NOT EXISTS MEDGEMMA_DEMO.PUBLIC.ECG_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Upload sample images (from himss-physician-app/public/images/dummy/):
--   PUT file:///path/to/dummy/*.png @MEDGEMMA_DEMO.PUBLIC.ECG_STAGE/dummy/;

-- ============================================================
-- 6. IMPORT & DEPLOY MedGemma via Snowsight UI
-- ============================================================
-- This is the easiest path. Follow these steps in Snowsight:
--
--   a) Navigate to: AI & ML → Models → "Import model"
--   b) Model handle:  google/medgemma-4b-it
--   c) Task:          text-generation (or visual-question-answering)
--   d) Check "Trust remote code" (required for MedGemma)
--   e) HF token secret: <YOUR_DB>.<YOUR_SCHEMA>.HF_TOKEN_SECRET
--   f) Model name:     MEDGEMMA_4B
--   g) Version name:   v1
--   h) Database/Schema: <YOUR_DB>.<YOUR_SCHEMA>
--   i) Click "Continue to deployment"
--   j) Service name:   MEDGEMMA_SERVICE
--   k) Check "Create REST API endpoint"
--   l) Compute pool:   MEDGEMMA_GPU_POOL
--   m) GPU: 1
--   n) Click "Deploy"
--
-- Deployment takes ~10-15 minutes. Monitor progress at:
--   Monitoring → Services & jobs → Jobs tab
--
-- ============================================================
-- 7. RETRIEVE YOUR SPCS ENDPOINT URL
-- ============================================================
-- After deployment completes, run:

-- SHOW ENDPOINTS IN SERVICE <YOUR_DB>.<YOUR_SCHEMA>.MEDGEMMA_SERVICE;

-- Copy the ingress_url value. It will look like:
--   https://<unique-id>-<org>-<account>.snowflakecomputing.app
--
-- You will need this URL for two places:
--   1. setup_data.sql → MEDGEMMA_REST_URL variable in the stored procedure
--   2. Optionally tighten the network rule:
--
-- ALTER NETWORK RULE MEDGEMMA_SPCS_NETWORK_RULE
--     SET VALUE_LIST = ('<your-endpoint-domain>:443');

-- ============================================================
-- 8. BIND SERVICE ENDPOINT (required for public ingress)
-- ============================================================
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE SYSADMIN;

-- ============================================================
-- VERIFICATION
-- ============================================================
-- After deployment, verify everything is running:

-- SHOW COMPUTE POOLS LIKE 'MEDGEMMA%';
-- SHOW SERVICES IN COMPUTE POOL MEDGEMMA_GPU_POOL;
-- SHOW ENDPOINTS IN SERVICE <YOUR_DB>.<YOUR_SCHEMA>.MEDGEMMA_SERVICE;
-- SELECT SYSTEM$GET_SERVICE_STATUS('<YOUR_DB>.<YOUR_SCHEMA>.MEDGEMMA_SERVICE');
