-- ============================================================
-- HLS SKILL TEST HARNESS — Runner Script
-- Tests the customer-dataproducts-multi-tenant skillset
-- against 12 Healthcare & Life Sciences prompts
-- ============================================================
-- 
-- OBJECTS DEPLOYED:
--   {{GOVERNANCE_DB}}.TEST_HARNESS.TEST_PROMPTS       — 12 HLS prompts with metadata
--   {{GOVERNANCE_DB}}.TEST_HARNESS.EVAL_CRITERIA      — 12 evaluation criteria (weighted)
--   {{GOVERNANCE_DB}}.TEST_HARNESS.TEST_RUNS          — Test run registry
--   {{GOVERNANCE_DB}}.TEST_HARNESS.PROMPT_SCORES      — Individual scores per prompt per criterion
--   {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SCORECARD — Detailed per-criterion view
--   {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SUMMARY   — Per-prompt weighted score + grade
--   {{GOVERNANCE_DB}}.TEST_HARNESS.V_RUN_DASHBOARD    — Run-level pass/fail summary
--   {{GOVERNANCE_DB}}.TEST_HARNESS.V_CATEGORY_BREAKDOWN — Score by category (ACCURACY, HLS_SPECIFIC, etc.)
--   {{GOVERNANCE_DB}}.TEST_HARNESS.V_WEAKEST_CRITERIA — Lowest-scoring criteria for improvement
--
-- PROCEDURES:
--   START_TEST_RUN(run_name)               — Creates a new test run, returns RUN_ID
--   SCORE_PROMPT(run_id, prompt_id, C01..C12) — Bulk-score a prompt (0-5 per criterion)
--   RECORD_SCORE(run_id, prompt_id, criterion_id, score, evidence) — Score one criterion
--
-- ============================================================

-- ============================================================
-- STEP 1: START A NEW TEST RUN
-- ============================================================
-- Run this once to create a test run. Save the returned RUN_ID.

CALL {{GOVERNANCE_DB}}.TEST_HARNESS.START_TEST_RUN('HLS Skill Validation - Initial');
-- Returns: RUN_20260427_HHMMSS (save this value)


-- ============================================================
-- STEP 2: VIEW ALL PROMPTS TO TEST
-- ============================================================
-- Lists all 12 HLS prompts ordered by difficulty.
-- Copy the PROMPT_TEXT and paste it into a conversation
-- with the customer-dataproducts-multi-tenant skill loaded.

SELECT
    PROMPT_ID,
    DIFFICULTY,
    SCENARIO_TITLE,
    JOURNEY_TYPE,
    ARRAY_SIZE(EXPECTED_SUBSKILLS) AS EXPECTED_SKILL_COUNT,
    PROMPT_TEXT
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.TEST_PROMPTS
ORDER BY
    CASE DIFFICULTY WHEN 'SIMPLE' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'HARD' THEN 3 END,
    PROMPT_ID;


-- ============================================================
-- STEP 3: VIEW SCORING CRITERIA
-- ============================================================
-- Review what you are scoring before you start.
-- Each criterion has a 0-5 scale and a weight.
-- Not all criteria apply to all difficulty levels.

SELECT
    CRITERION_ID,
    CRITERION_NAME,
    CATEGORY,
    WEIGHT,
    APPLIES_TO,
    SCORING_GUIDANCE
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.EVAL_CRITERIA
ORDER BY CRITERION_ID;


-- ============================================================
-- STEP 4: TEST EACH PROMPT AND SCORE IT
-- ============================================================
-- For each prompt:
--   1. Copy the PROMPT_TEXT from Step 2
--   2. Paste it into a new conversation with the skill
--   3. Evaluate the skill output against each criterion (0-5)
--   4. Record the scores using SCORE_PROMPT
--
-- SCORING SCALE:
--   0 = Missing / Not addressed
--   1 = Poor — barely addressed
--   2 = Below average — partial coverage
--   3 = Average — adequate but room for improvement
--   4 = Good — strong output with minor gaps
--   5 = Excellent — production-ready, comprehensive
--
-- Criteria that do not apply to SIMPLE difficulty are ignored
-- automatically (C05 Approval Checkpoint, C07 Trade-offs, C11 Sub-Skill Routing).

-- === SIMPLE PROMPTS (D1, D2, E1, E2) ===
-- 10 criteria apply: C01-C04, C06, C08-C10, C12 (skip C05, C07, C11)
-- Pass C05, C07, C11 as 0 — they are filtered out by difficulty.

-- Example: Score D1 (replace RUN_ID with your actual value)
-- CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
--     'RUN_20260427_HHMMSS',  -- your RUN_ID
--     'D1',                    -- prompt ID
--     4,  -- C01: Clarifying Questions
--     5,  -- C02: Entity Name Usage
--     4,  -- C03: Parameterized SQL
--     5,  -- C04: Snowflake Doc Grounding
--     0,  -- C05: Approval Checkpoint (N/A for SIMPLE)
--     3,  -- C06: Edition Requirements
--     0,  -- C07: Trade-offs (N/A for SIMPLE)
--     4,  -- C08: Actionable Deliverables
--     5,  -- C09: Compliance Coverage
--     5,  -- C10: PHI/PII Handling
--     0,  -- C11: Sub-Skill Routing (N/A for SIMPLE)
--     4   -- C12: Completeness vs Difficulty
-- );

-- === MEDIUM PROMPTS (D3, D4, E3, E4) ===
-- All 12 criteria apply.

-- === HARD PROMPTS (D5, D6, E5, E6) ===
-- All 12 criteria apply.

-- ────────────────────────────────────────
-- GREENFIELD SIMPLE
-- ────────────────────────────────────────

-- D1: Digital Health Clinic Tenancy
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D1',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0  -- replace 0s with your scores
);

-- D2: PHI Classification and Masking
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D2',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- ────────────────────────────────────────
-- GREENFIELD MEDIUM
-- ────────────────────────────────────────

-- D3: Multi-Tenant EHR Analytics
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D3',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- D4: Clinical Trial CRO Platform
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D4',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- ────────────────────────────────────────
-- GREENFIELD HARD
-- ────────────────────────────────────────

-- D5: Health System Data Mesh with AI
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D5',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- D6: Genomics Precision Medicine Platform
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'D6',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- ────────────────────────────────────────
-- BROWNFIELD SIMPLE
-- ────────────────────────────────────────

-- E1: HIPAA Audit Readiness Check
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E1',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- E2: Clinical Warehouse Cost Review
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E2',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- ────────────────────────────────────────
-- BROWNFIELD MEDIUM
-- ────────────────────────────────────────

-- E3: Payer-Provider Data Exchange Modernization
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E3',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- E4: Pharma RWE Platform Optimization
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E4',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- ────────────────────────────────────────
-- BROWNFIELD HARD
-- ────────────────────────────────────────

-- E5: Health Information Exchange Modernization
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E5',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);

-- E6: Biotech Multi-Account Consolidation
CALL {{GOVERNANCE_DB}}.TEST_HARNESS.SCORE_PROMPT(
    '<YOUR_RUN_ID>', 'E6',
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
);


-- ============================================================
-- STEP 5: VIEW RESULTS
-- ============================================================

-- 5A: Per-prompt grades
SELECT PROMPT_ID, DIFFICULTY, SCENARIO_TITLE, JOURNEY_TYPE,
       TOTAL_WEIGHTED_SCORE, MAX_POSSIBLE_SCORE, SCORE_PCT, GRADE
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SUMMARY
WHERE RUN_ID = '<YOUR_RUN_ID>'
ORDER BY SCORE_PCT DESC;

-- 5B: Overall run dashboard
SELECT * FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_RUN_DASHBOARD
WHERE RUN_ID = '<YOUR_RUN_ID>';

-- 5C: Score by category (which areas are strong/weak)
SELECT * FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_CATEGORY_BREAKDOWN
WHERE RUN_ID = '<YOUR_RUN_ID>'
ORDER BY CATEGORY_SCORE_PCT ASC;

-- 5D: Weakest criteria (where to improve the skill)
SELECT * FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_WEAKEST_CRITERIA
WHERE RUN_ID = '<YOUR_RUN_ID>'
ORDER BY AVG_SCORE ASC
LIMIT 5;

-- 5E: Detailed scorecard for a specific prompt
SELECT CRITERION_NAME, CATEGORY, WEIGHT, SCORE, WEIGHTED_SCORE, MAX_WEIGHTED_SCORE, EVIDENCE
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SCORECARD
WHERE RUN_ID = '<YOUR_RUN_ID>' AND PROMPT_ID = 'D5'
ORDER BY CRITERION_NAME;

-- 5F: Greenfield vs Brownfield comparison
SELECT
    JOURNEY_TYPE,
    ROUND(AVG(SCORE_PCT), 1) AS AVG_SCORE_PCT,
    COUNT(*) AS PROMPTS_TESTED
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SUMMARY
WHERE RUN_ID = '<YOUR_RUN_ID>'
GROUP BY JOURNEY_TYPE;

-- 5G: Difficulty tier comparison
SELECT
    DIFFICULTY,
    ROUND(AVG(SCORE_PCT), 1) AS AVG_SCORE_PCT,
    COUNT(*) AS PROMPTS_TESTED
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_PROMPT_SUMMARY
WHERE RUN_ID = '<YOUR_RUN_ID>'
GROUP BY DIFFICULTY
ORDER BY CASE DIFFICULTY WHEN 'SIMPLE' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'HARD' THEN 3 END;


-- ============================================================
-- STEP 6: COMPARE ACROSS RUNS (after multiple test sessions)
-- ============================================================

SELECT
    RUN_NAME,
    RUN_DATE,
    PROMPTS_TESTED,
    AVG_SCORE_PCT,
    EXCELLENT_COUNT,
    GOOD_COUNT,
    BELOW_THRESHOLD_COUNT,
    OVERALL_VERDICT
FROM {{GOVERNANCE_DB}}.TEST_HARNESS.V_RUN_DASHBOARD
ORDER BY RUN_DATE DESC;
