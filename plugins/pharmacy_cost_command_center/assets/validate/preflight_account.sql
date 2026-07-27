-- Advisory account-capability preflight for the Pharmacy Cost Command Center accelerator.
-- Run this once against a NEW target account (via `snow sql -c <conn>`) before Phase 1.
-- These are BEST-EFFORT checks: SQL cannot verify everything (external access
-- integrations, the region's full Cortex model list, App Runtime enablement), so
-- treat any failure as a WARN and confirm the rest against the README
-- "Per-account prerequisites" checklist. Run the statements one at a time and
-- interpret the results; nothing here changes state.

-- 1. Session context (INFO) - who/where you are running as.
SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_REGION() AS REGION,
       CURRENT_ROLE() AS ROLE, CURRENT_WAREHOUSE() AS WAREHOUSE;

-- 2. A warehouse is set for the session (needed for dynamic-table refresh + app queries).
--    WARN if NULL -> `USE WAREHOUSE <wh>` or set it in your connection / config.json.
SELECT IFF(CURRENT_WAREHOUSE() IS NOT NULL, 'PASS', 'WARN') AS WAREHOUSE_SET,
       COALESCE(CURRENT_WAREHOUSE(), 'no active warehouse') AS DETAIL;

-- 3. Cortex generation smoke (Phase 3 MDL_TAB_INSIGHTS uses AI_COMPLETE).
--    If THIS statement ERRORS, the model is unavailable in this region ->
--    set "insight_format_model" in config.json to a model that is available.
--    (Runs a tiny 1-token prompt; returns 'PASS'.)
SELECT IFF(LENGTH(SNOWFLAKE.CORTEX.COMPLETE('claude-opus-4-8', 'ok')) >= 0, 'PASS', 'PASS')
         AS CORTEX_COMPLETE_OK;

-- 4. Compute pools (Phase 4 Snowflake App Runtime runs on SPCS). An EMPTY result
--    means no compute pools are visible -> App Runtime may not be enabled for this
--    account/role. A non-empty result is a good sign (still confirm an external
--    access integration exists for the container build - not checkable here).
SHOW COMPUTE POOLS;

-- NOT checkable in SQL (confirm via README checklist): external access integration
-- for the app build, `snow app` CLI version, and the full region Cortex model list.
