-- Phase 3 post-build validation for the Pharmacy Cost Command Center accelerator.
-- STRUCTURAL PASS/FAIL over the deployed derived objects (+ WARN for soft signals).
-- No magic-number FAILs: value-range checks are WARN only, so a legitimately
-- unusual customer dataset is never rejected. Every row:
--   CHECK_NAME | STATUS ('PASS' | 'FAIL' | 'WARN') | DETAIL
-- Flexibility rule: FAIL-if-empty applies only to REQUIRED-backed views (Rx +
-- member-month + NDC reference). Optional-backed views (PA, rebate, formulary,
-- adherence, waste, bridge) WARN-if-empty, since the customer may lack that
-- source and the template present()-gates that arm off by design.
-- Parameterized: replace {{TARGET_DATABASE}}.{{TARGET_SCHEMA}} at run time.

WITH c AS (
  SELECT
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_KPI_SUMMARY)          AS kpi,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_PMPM_TREND)           AS trend,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_SPEND_BY_BENEFIT)     AS benefit,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_HIGH_COST_CLASSES)    AS hicost,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_TREND_PVM)            AS pvm,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_TREND_CONTRIBUTORS)   AS contrib,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_PA_OUTCOMES)          AS pa,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_REBATE_YIELD)         AS rebate,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_FORMULARY_LEAKAGE)    AS offform,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_ADHERENCE_BY_CLASS)   AS adherence,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_WASTE_CONCENTRATION)  AS waste,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_ADDRESSABLE_BRIDGE)   AS bridge,
    (SELECT COUNT(*) FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.MDL_TAB_INSIGHTS)        AS insights,
    (SELECT NET_PMPM_CURRENT FROM {{TARGET_DATABASE}}.{{TARGET_SCHEMA}}.VW_KPI_SUMMARY LIMIT 1) AS pmpm
)
-- Required-backed views: FAIL if empty
SELECT 'VW_KPI_SUMMARY_NONEMPTY'      AS CHECK_NAME, IFF(kpi     > 0,'PASS','FAIL') AS STATUS, 'rows='||kpi     AS DETAIL FROM c
UNION ALL SELECT 'VW_PMPM_TREND_NONEMPTY',        IFF(trend   > 0,'PASS','FAIL'), 'rows='||trend   FROM c
UNION ALL SELECT 'VW_SPEND_BY_BENEFIT_NONEMPTY',  IFF(benefit > 0,'PASS','FAIL'), 'rows='||benefit FROM c
UNION ALL SELECT 'VW_HIGH_COST_CLASSES_NONEMPTY', IFF(hicost  > 0,'PASS','FAIL'), 'rows='||hicost  FROM c
UNION ALL SELECT 'VW_TREND_PVM_NONEMPTY',         IFF(pvm     > 0,'PASS','FAIL'), 'rows='||pvm      FROM c
UNION ALL SELECT 'VW_TREND_CONTRIBUTORS_NONEMPTY',IFF(contrib > 0,'PASS','FAIL'), 'rows='||contrib  FROM c
-- KPI positivity: structural FAIL (a KPI book with null/non-positive PMPM is broken)
UNION ALL SELECT 'NET_PMPM_PRESENT_POSITIVE',     IFF(pmpm IS NOT NULL AND pmpm > 0,'PASS','FAIL'), 'net_pmpm_current='||COALESCE(TO_VARCHAR(pmpm),'NULL') FROM c
-- Insights: exactly 4 rows (the 4 accelerator dashboard bands)
UNION ALL SELECT 'MDL_TAB_INSIGHTS_FOUR_ROWS',    IFF(insights = 4,'PASS','FAIL'), 'rows='||insights||' (expected 4)' FROM c
-- Optional-backed views: WARN (never FAIL) if empty
UNION ALL SELECT 'VW_PA_OUTCOMES_POPULATED',        IFF(pa        > 0,'PASS','WARN'), 'rows='||pa        ||' (optional: PRIOR_AUTH)'      FROM c
UNION ALL SELECT 'VW_REBATE_YIELD_POPULATED',       IFF(rebate    > 0,'PASS','WARN'), 'rows='||rebate    ||' (optional: REBATE_SOURCE)'   FROM c
UNION ALL SELECT 'VW_FORMULARY_LEAKAGE_POPULATED',  IFF(offform   > 0,'PASS','WARN'), 'rows='||offform   ||' (optional: FORMULARY_SOURCE)' FROM c
UNION ALL SELECT 'VW_ADHERENCE_BY_CLASS_POPULATED', IFF(adherence > 0,'PASS','WARN'), 'rows='||adherence FROM c
UNION ALL SELECT 'VW_WASTE_CONCENTRATION_POPULATED',IFF(waste     > 0,'PASS','WARN'), 'rows='||waste     FROM c
UNION ALL SELECT 'VW_ADDRESSABLE_BRIDGE_POPULATED', IFF(bridge    > 0,'PASS','WARN'), 'rows='||bridge    FROM c
-- Value sanity: WARN only (advisory band, never a FAIL)
UNION ALL SELECT 'NET_PMPM_IN_ADVISORY_BAND',     IFF(pmpm IS NULL OR (pmpm BETWEEN 1 AND 5000),'PASS','WARN'), 'net_pmpm_current='||COALESCE(TO_VARCHAR(pmpm),'NULL')||' (advisory band 1..5000)' FROM c
ORDER BY DECODE(STATUS,'FAIL',0,'WARN',1,'PASS',2), CHECK_NAME;
