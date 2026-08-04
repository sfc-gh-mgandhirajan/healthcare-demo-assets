-- =====================================================================
-- tre-builder · seed 05 · In-perimeter Agent + Secure Share
-- =====================================================================
-- Safe Settings, continued: a READ-ONLY Cortex Agent for natural-language
-- exploration that lives INSIDE the perimeter. Masking and row-access
-- policies apply to the agent exactly as to the querying researcher, so
-- it physically cannot see masked PHI or out-of-protocol rows. It has NO
-- write tools (read-only by architecture) and it routes any export
-- request to the airlock.
--
-- Also creates a suppressed aggregate SECURE VIEW as the shareable output
-- surface (Safe Outputs at the sharing boundary) + a Direct Share template.
--
-- Parameter: {{TRE}}
-- Run after 02/03 (data + policies must exist).
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- ---------- Semantic view for NL exploration --------------------------
CREATE OR REPLACE SEMANTIC VIEW TRE_{{TRE}}_DB.OMOP_CDM.OMOP_SV
  TABLES (
    person     AS TRE_{{TRE}}_DB.OMOP_CDM.PERSON            PRIMARY KEY (PERSON_ID),
    feats      AS TRE_{{TRE}}_DB.OMOP_CDM.PATIENT_FEATURES  PRIMARY KEY (PERSON_ID),
    conditions AS TRE_{{TRE}}_DB.OMOP_CDM.CONDITION_OCCURRENCE,
    visits     AS TRE_{{TRE}}_DB.OMOP_CDM.VISIT_OCCURRENCE
  )
  RELATIONSHIPS (
    feats_to_person AS feats (PERSON_ID)      REFERENCES person (PERSON_ID),
    cond_to_person  AS conditions (PERSON_ID) REFERENCES person (PERSON_ID),
    visit_to_person AS visits (PERSON_ID)     REFERENCES person (PERSON_ID)
  )
  DIMENSIONS (
    person.gender        AS person.GENDER_SOURCE_VALUE,
    person.birth_year    AS person.YEAR_OF_BIRTH,
    person.ses_childhood AS person.SES_CHILDHOOD,
    feats.is_high_cost   AS feats.IS_HIGH_COST,
    feats.died           AS feats.DIED
  )
  METRICS (
    person.patient_count     AS COUNT(DISTINCT person.PERSON_ID),
    feats.avg_pace_of_aging  AS AVG(feats.PACE_OF_AGING),
    feats.avg_aces           AS AVG(feats.ACES_SCORE),
    conditions.condition_count AS COUNT(conditions.PERSON_ID),
    visits.visit_count       AS COUNT(visits.PERSON_ID)
  )
  COMMENT = 'TRE OMOP cohort semantic view — in-perimeter NL exploration (policies apply)';

GRANT SELECT ON SEMANTIC VIEW TRE_{{TRE}}_DB.OMOP_CDM.OMOP_SV TO ROLE TRE_{{TRE}}_ANALYST;

-- ---------- Read-only, guarded Cortex Agent ---------------------------
CREATE OR REPLACE AGENT TRE_{{TRE}}_DB.OMOP_CDM.TRE_EXPLORER_AGENT
FROM SPECIFICATION $$
{
  "models": {"orchestration": "auto"},
  "instructions": {
    "response": "Answer only from the semantic view, with aggregates. This is a Trusted Research Environment: never try to identify an individual, never reveal masked values, and never produce a row-level export. If asked to export or release data, tell the user it must go through the airlock (AIRLOCK.REQUEST_EGRESS) and be dual-reviewed.",
    "orchestration": "You are the TRE Explorer, a READ-ONLY assistant helping researchers explore an OMOP cohort INSIDE a Trusted Research Environment. You can only query the OMOP semantic view via the analyst tool. Masking and row-access policies apply to you exactly as they do to the querying researcher: you physically cannot see masked PHI (dates, cause of death, socioeconomic status) or persons outside an active protocol cohort, and you must never attempt to circumvent that. You have NO write tools and cannot create, modify, or export anything. For any data egress, instruct the user to submit the aggregate through AIRLOCK.REQUEST_EGRESS for statistical disclosure control and dual sign-off."
  },
  "tools": [
    {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "omop_query", "description": "Query the OMOP cohort semantic view for counts, distributions, and aggregate cohort facts. Aggregates only; policies enforce masking and row scope."}}
  ],
  "tool_resources": {
    "omop_query": {"execution_environment": {"type": "warehouse", "warehouse": "TRE_{{TRE}}_WH"}, "semantic_view": "TRE_{{TRE}}_DB.OMOP_CDM.OMOP_SV"}
  }
}
$$;

GRANT USAGE ON AGENT TRE_{{TRE}}_DB.OMOP_CDM.TRE_EXPLORER_AGENT TO ROLE TRE_{{TRE}}_RESEARCHER;

-- ---------- Secure share surface (Safe Outputs at the boundary) -------
-- Aggregate-only, small-cell-suppressed. Safe to publish via a Direct
-- Share, Data Clean Room, or Reader Account.
CREATE OR REPLACE SECURE VIEW TRE_{{TRE}}_DB.OMOP_CDM.COHORT_SUMMARY_SECURE AS
  SELECT p.GENDER_SOURCE_VALUE AS gender,
         f.IS_HIGH_COST        AS is_high_cost,
         COUNT(*)              AS n
  FROM TRE_{{TRE}}_DB.OMOP_CDM.PERSON p
  JOIN TRE_{{TRE}}_DB.OMOP_CDM.PATIENT_FEATURES f ON f.PERSON_ID = p.PERSON_ID
  GROUP BY 1, 2
  HAVING COUNT(*) >= 11;   -- small-cell suppression baked in

GRANT SELECT ON VIEW TRE_{{TRE}}_DB.OMOP_CDM.COHORT_SUMMARY_SECURE TO ROLE TRE_{{TRE}}_ANALYST;

-- Direct Share template (uncomment + set a consumer account to publish):
--   CREATE SHARE IF NOT EXISTS TRE_{{TRE}}_SHARE;
--   GRANT USAGE ON DATABASE TRE_{{TRE}}_DB TO SHARE TRE_{{TRE}}_SHARE;
--   GRANT USAGE ON SCHEMA TRE_{{TRE}}_DB.OMOP_CDM TO SHARE TRE_{{TRE}}_SHARE;
--   GRANT SELECT ON VIEW TRE_{{TRE}}_DB.OMOP_CDM.COHORT_SUMMARY_SECURE TO SHARE TRE_{{TRE}}_SHARE;
--   ALTER SHARE TRE_{{TRE}}_SHARE ADD ACCOUNTS = <consumer_account>;

MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
  USING (SELECT 'phase' AS KEY, '4-agent' AS VALUE) s ON t.KEY = s.KEY
  WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

SELECT 'Agent + secure share ready: OMOP_SV, TRE_EXPLORER_AGENT (read-only), COHORT_SUMMARY_SECURE' AS status;
