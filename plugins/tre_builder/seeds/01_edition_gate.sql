-- =====================================================================
-- tre-builder · seed 01 · Edition Gate (fail loud)
-- =====================================================================
-- A TRE handling PHI under HIPAA requires Snowflake BUSINESS CRITICAL
-- edition (Tri-Secret Secure / customer-managed keys, PrivateLink).
-- This gate REFUSES to proceed in HIPAA_MODE unless the edition is
-- positively attested — it must never silently deploy something that
-- looks compliant but is not.
--
-- Why attestation (not auto-detect): reliable edition detection requires
-- ORGADMIN (SHOW ORGANIZATION ACCOUNTS), which most deployers lack. So
-- the tre-deploy skill collects an explicit acknowledgement and this gate
-- validates + records it. (An ORGADMIN can pre-verify out of band.)
--
-- Parameters:
--   {{TRE}}          environment slug
--   {{HIPAA_MODE}}   TRUE | FALSE   (TRUE = PHI in scope, BC required)
--   {{EDITION_ACK}}  attested edition, e.g. BUSINESS_CRITICAL | VPS
--                    (in DEMO_MODE / HIPAA_MODE=FALSE this may be STANDARD/ENTERPRISE)
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

EXECUTE IMMEDIATE $$
DECLARE
  hipaa        BOOLEAN := {{HIPAA_MODE}};
  edition_ack  STRING  := UPPER('{{EDITION_ACK}}');
  not_business_critical EXCEPTION (-20101,
    'EDITION GATE FAILED: HIPAA_MODE=TRUE requires Business Critical (or VPS). Attested edition did not qualify. Deploy refused — set the account to Business Critical or deploy with HIPAA_MODE=FALSE (labeled DEMO_MODE).');
  no_attestation EXCEPTION (-20102,
    'EDITION GATE FAILED: HIPAA_MODE=TRUE requires a positive edition attestation (EDITION_ACK). None provided. Deploy refused.');
BEGIN
  IF (hipaa) THEN
     IF (edition_ack = '' OR edition_ack IS NULL) THEN
        RAISE no_attestation;
     END IF;
     IF (edition_ack NOT IN ('BUSINESS_CRITICAL','VPS')) THEN
        RAISE not_business_critical;
     END IF;
  END IF;

  -- Record the gate outcome to the deploy config (audit trail).
  MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
    USING (SELECT 'hipaa_mode' AS KEY, IFF(:hipaa,'TRUE','FALSE') AS VALUE) s ON t.KEY = s.KEY
    WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);
  MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
    USING (SELECT 'edition_attested' AS KEY, IFF(:edition_ack='','(none)',:edition_ack) AS VALUE) s ON t.KEY = s.KEY
    WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

  RETURN 'Edition gate passed. hipaa_mode=' || IFF(:hipaa,'TRUE','FALSE')
      || ' edition=' || IFF(:edition_ack='','(demo, unattested)',:edition_ack)
      || IFF(:hipaa,'', '  [DEMO_MODE — not for real PHI]');
END;
$$;
