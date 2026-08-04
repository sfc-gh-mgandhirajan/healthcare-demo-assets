-- =====================================================================
-- tre-builder · seed 04 · Output Airlock  (the hero feature)
-- =====================================================================
-- Safe Outputs. Nothing leaves the TRE without passing statistical
-- disclosure control (SDC) AND dual, independent sign-off — with a full,
-- append-only audit trail. This is the control most TREs under-serve.
--
-- Flow:
--   RESEARCHER  -> builds a scoped aggregate in WORKSPACE (runs under their
--                  own role, so row-access + masking already applied), then
--                  REQUEST_EGRESS(source_object)  (SDC = min cell-count check)
--   PI / ADMIN  -> REVIEW_EGRESS   (APPROVE x2 by DIFFERENT reviewers, or REJECT)
--   PI / ADMIN  -> RELEASE_EGRESS  (only after two approvals; marks RELEASED)
--
-- WHY submit an OBJECT (not run a query inside the proc): the proc runs
-- EXECUTE AS OWNER for ledger/audit integrity (researchers cannot write the
-- ledger/audit or the RESULT_ snapshot directly). Because the owner's read is
-- NOT the researcher's, the proc HARD-CONSTRAINS the source to a WORKSPACE
-- BASE TABLE (validated via INFORMATION_SCHEMA; views + OMOP_CDM + other schemas
-- are rejected) that the researcher already materialized under THEIR masked/
-- row-scoped role -- so it is masked-at-rest and the owner snapshot is faithful
-- REGARDLESS of which role deployed the TRE. SDC (min cell-count) runs BEFORE any
-- snapshot, so a failed request leaves nothing behind. Identifiers are validated
-- and quoted (no raw interpolation of caller input).
--
-- Parameter: {{TRE}}
-- =====================================================================

USE ROLE {{DEPLOY_ROLE}};

-- ---------- Ledger + append-only audit --------------------------------
CREATE TABLE IF NOT EXISTS TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS (
  REQUEST_ID    STRING,
  PROTOCOL_ID   STRING,
  DESCRIPTION   STRING,
  REQUESTED_BY  STRING,
  REQUESTED_AT  TIMESTAMP_NTZ,
  RESULT_TABLE  STRING,
  ROW_COUNT     NUMBER,
  MIN_CELL      NUMBER,
  THRESHOLD     NUMBER,
  SDC_PASS      BOOLEAN,
  STATUS        STRING,          -- SDC_FAIL | PENDING_REVIEW | APPROVED_1 | APPROVED_2 | RELEASED | REJECTED
  APPROVER_1    STRING,
  APPROVED_1_AT TIMESTAMP_NTZ,
  APPROVER_2    STRING,
  APPROVED_2_AT TIMESTAMP_NTZ,
  RELEASED_AT   TIMESTAMP_NTZ,
  REVIEW_NOTES  STRING
);

CREATE TABLE IF NOT EXISTS TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (
  AUDIT_ID   NUMBER AUTOINCREMENT,
  REQUEST_ID STRING,
  ACTION     STRING,
  ACTOR      STRING,
  DETAIL     STRING,
  TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================
-- REQUEST_EGRESS — researcher submits a WORKSPACE object (a scoped
-- aggregate they already built) for review. The object must contain a
-- cell-count column (default 'N'); SDC passes only if every cell >= P_THRESHOLD.
-- =====================================================================
CREATE OR REPLACE PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS(
  P_PROTOCOL_ID STRING, P_DESCRIPTION STRING, P_SOURCE_OBJECT STRING,
  P_COUNT_COL STRING DEFAULT 'N', P_THRESHOLD FLOAT DEFAULT 11)
RETURNS STRING LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  rid   STRING;
  rtbl  STRING;
  parts ARRAY;
  np    NUMBER;
  db    STRING;
  sch   STRING;
  nm    STRING;
  ucol  STRING;
  fq    STRING;
  qcol  STRING;
  cnt   NUMBER := 0;
  ccnt  NUMBER := 0;
  rc    NUMBER := 0;
  mc    NUMBER := 0;
  ok    BOOLEAN := FALSE;
  st    STRING;
  bad_source      EXCEPTION (-20201, 'REQUEST_EGRESS: P_SOURCE_OBJECT must be a WORKSPACE base table -- pass a bare NAME or TRE_<tre>_DB.WORKSPACE.NAME. Build a scoped aggregate in WORKSPACE first.');
  not_ws_table    EXCEPTION (-20207, 'REQUEST_EGRESS: source is not a WORKSPACE BASE TABLE. Only researcher-built WORKSPACE tables may be submitted; OMOP_CDM, views, and other schemas are rejected (a view would re-resolve unmasked under the owner).');
  bad_count_col   EXCEPTION (-20208, 'REQUEST_EGRESS: P_COUNT_COL must be an existing NUMERIC column of that WORKSPACE table (the per-cell count).');
  snapshot_failed EXCEPTION (-20209, 'REQUEST_EGRESS: snapshot creation failed; no result table retained.');
BEGIN
  rid  := 'EGR-' || TO_VARCHAR(CURRENT_TIMESTAMP(),'YYYYMMDDHH24MISSFF3');
  rtbl := 'TRE_{{TRE}}_DB.AIRLOCK.RESULT_' || REPLACE(:rid,'-','_');  -- hyphen illegal in identifier

  -- ---- Validate the source: WORKSPACE base table ONLY (deploy-role-independent safety).
  parts := SPLIT(:P_SOURCE_OBJECT, '.');
  np := ARRAY_SIZE(:parts);
  IF (np = 1) THEN
    db := 'TRE_{{TRE}}_DB'; sch := 'WORKSPACE'; nm := UPPER(TRIM(GET(:parts,0)::STRING));
  ELSEIF (np = 3) THEN
    db := UPPER(TRIM(GET(:parts,0)::STRING)); sch := UPPER(TRIM(GET(:parts,1)::STRING)); nm := UPPER(TRIM(GET(:parts,2)::STRING));
  ELSE
    RAISE bad_source;
  END IF;
  IF (db <> 'TRE_{{TRE}}_DB' OR sch <> 'WORKSPACE') THEN RAISE bad_source; END IF;
  IF (NOT (:nm RLIKE '^[A-Z_][A-Z0-9_$]*$')) THEN RAISE bad_source; END IF;

  SELECT COUNT(*) INTO :cnt FROM TRE_{{TRE}}_DB.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'WORKSPACE' AND TABLE_NAME = :nm AND TABLE_TYPE = 'BASE TABLE';
  IF (:cnt = 0) THEN RAISE not_ws_table; END IF;

  ucol := UPPER(TRIM(:P_COUNT_COL));
  IF (NOT (:ucol RLIKE '^[A-Z_][A-Z0-9_$]*$')) THEN RAISE bad_count_col; END IF;
  SELECT COUNT(*) INTO :ccnt FROM TRE_{{TRE}}_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'WORKSPACE' AND TABLE_NAME = :nm AND COLUMN_NAME = :ucol
      AND DATA_TYPE IN ('NUMBER','DECIMAL','NUMERIC','INT','INTEGER','BIGINT','SMALLINT','TINYINT','BYTEINT','FLOAT','FLOAT4','FLOAT8','DOUBLE','DOUBLE PRECISION','REAL');
  IF (:ccnt = 0) THEN RAISE bad_count_col; END IF;

  fq   := 'TRE_{{TRE}}_DB.WORKSPACE."' || :nm || '"';
  qcol := '"' || :ucol || '"';

  -- ---- SDC FIRST (no snapshot yet): count rows + min cell from the validated table.
  EXECUTE IMMEDIATE 'SELECT COUNT(*) AS RC, MIN(' || :qcol || ') AS MC FROM ' || :fq;
  SELECT RC, MC INTO :rc, :mc FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  ok := (:mc >= :P_THRESHOLD);
  st := IFF(:ok, 'PENDING_REVIEW', 'SDC_FAIL');

  -- ---- Snapshot ONLY on SDC pass; on failure drop it so nothing unreviewed persists.
  IF (:ok) THEN
    BEGIN
      EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || :rtbl || ' AS SELECT * FROM ' || :fq;
    EXCEPTION WHEN OTHER THEN
      BEGIN EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || :rtbl; EXCEPTION WHEN OTHER THEN NULL; END;
      RAISE snapshot_failed;
    END;
  ELSE
    rtbl := NULL;
  END IF;

  INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS
    (REQUEST_ID, PROTOCOL_ID, DESCRIPTION, REQUESTED_BY, REQUESTED_AT, RESULT_TABLE,
     ROW_COUNT, MIN_CELL, THRESHOLD, SDC_PASS, STATUS)
  SELECT :rid, :P_PROTOCOL_ID, :P_DESCRIPTION, CURRENT_USER(), CURRENT_TIMESTAMP(), :rtbl,
     :rc, :mc, :P_THRESHOLD, :ok, :st;

  INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (REQUEST_ID, ACTION, ACTOR, DETAIL)
  SELECT :rid, 'REQUEST', CURRENT_USER(),
     'source=TRE_{{TRE}}_DB.WORKSPACE.' || :nm || ' rows=' || :rc || ' min_cell=' || :mc || ' threshold=' || :P_THRESHOLD || ' sdc=' || IFF(:ok,'PASS','FAIL');

  RETURN :rid || ' -> ' || :st || ' (min_cell=' || :mc || ', threshold=' || :P_THRESHOLD || ')';
END;
$$;

-- =====================================================================
-- REVIEW_EGRESS — dual sign-off. Two DIFFERENT reviewers must APPROVE.
-- =====================================================================
CREATE OR REPLACE PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.REVIEW_EGRESS(
  P_REQUEST_ID STRING, P_DECISION STRING, P_NOTE STRING DEFAULT '')
RETURNS STRING LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  cur_status STRING;
  sdc BOOLEAN;
  appr1 STRING;
  reqby STRING;
  me STRING;
  not_found EXCEPTION (-20202, 'REVIEW_EGRESS: request not found.');
  sdc_failed EXCEPTION (-20203, 'REVIEW_EGRESS: cannot approve — SDC did not pass. Reject or resubmit.');
  same_reviewer EXCEPTION (-20204, 'REVIEW_EGRESS: dual control — the second approver must differ from the first.');
  bad_state EXCEPTION (-20205, 'REVIEW_EGRESS: request is not in a reviewable state.');
  self_approve EXCEPTION (-20210, 'REVIEW_EGRESS: separation of duties — the requester cannot approve their own egress.');
BEGIN
  me := CURRENT_USER();
  SELECT STATUS, SDC_PASS, APPROVER_1, REQUESTED_BY INTO :cur_status, :sdc, :appr1, :reqby
    FROM TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS WHERE REQUEST_ID = :P_REQUEST_ID;
  IF (:cur_status IS NULL) THEN RAISE not_found; END IF;

  IF (UPPER(:P_DECISION) = 'REJECT') THEN
    UPDATE TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS
      SET STATUS='REJECTED', REVIEW_NOTES = COALESCE(REVIEW_NOTES,'') || ' | REJECT by ' || :me || ': ' || :P_NOTE
      WHERE REQUEST_ID = :P_REQUEST_ID;
    INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (REQUEST_ID, ACTION, ACTOR, DETAIL)
      SELECT :P_REQUEST_ID, 'REJECT', :me, :P_NOTE;
    RETURN :P_REQUEST_ID || ' -> REJECTED';
  END IF;

  -- APPROVE path
  IF (NOT :sdc) THEN RAISE sdc_failed; END IF;
  IF (:me = :reqby) THEN RAISE self_approve; END IF;  -- separation of duties

  IF (:cur_status = 'PENDING_REVIEW') THEN
    UPDATE TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS
      SET STATUS='APPROVED_1', APPROVER_1=:me, APPROVED_1_AT=CURRENT_TIMESTAMP(),
          REVIEW_NOTES = COALESCE(REVIEW_NOTES,'') || ' | APPROVE_1 by ' || :me || ': ' || :P_NOTE
      WHERE REQUEST_ID = :P_REQUEST_ID;
    INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (REQUEST_ID, ACTION, ACTOR, DETAIL)
      SELECT :P_REQUEST_ID, 'APPROVE_1', :me, :P_NOTE;
    RETURN :P_REQUEST_ID || ' -> APPROVED_1 (needs a second, different approver)';
  ELSEIF (:cur_status = 'APPROVED_1') THEN
    IF (:appr1 = :me) THEN RAISE same_reviewer; END IF;
    UPDATE TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS
      SET STATUS='APPROVED_2', APPROVER_2=:me, APPROVED_2_AT=CURRENT_TIMESTAMP(),
          REVIEW_NOTES = COALESCE(REVIEW_NOTES,'') || ' | APPROVE_2 by ' || :me || ': ' || :P_NOTE
      WHERE REQUEST_ID = :P_REQUEST_ID;
    INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (REQUEST_ID, ACTION, ACTOR, DETAIL)
      SELECT :P_REQUEST_ID, 'APPROVE_2', :me, :P_NOTE;
    RETURN :P_REQUEST_ID || ' -> APPROVED_2 (ready to release)';
  ELSE
    RAISE bad_state;
  END IF;
END;
$$;

-- =====================================================================
-- RELEASE_EGRESS — only after two independent approvals.
-- =====================================================================
CREATE OR REPLACE PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.RELEASE_EGRESS(P_REQUEST_ID STRING)
RETURNS STRING LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  cur_status STRING;
  rtbl STRING;
  not_ready EXCEPTION (-20206, 'RELEASE_EGRESS: request is not APPROVED_2 — two independent approvals are required before release.');
BEGIN
  SELECT STATUS, RESULT_TABLE INTO :cur_status, :rtbl
    FROM TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS WHERE REQUEST_ID = :P_REQUEST_ID;
  IF (:cur_status IS DISTINCT FROM 'APPROVED_2') THEN RAISE not_ready; END IF;

  UPDATE TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS
    SET STATUS='RELEASED', RELEASED_AT=CURRENT_TIMESTAMP() WHERE REQUEST_ID = :P_REQUEST_ID;
  INSERT INTO TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT (REQUEST_ID, ACTION, ACTOR, DETAIL)
    SELECT :P_REQUEST_ID, 'RELEASE', CURRENT_USER(), 'released table ' || :rtbl;

  RETURN :P_REQUEST_ID || ' -> RELEASED. Approved output: ' || :rtbl;
END;
$$;

-- ---------- Tiered access to the airlock procedures -------------------
-- Researchers may request; PIs (and ADMIN, by inheritance) review + release.
GRANT USAGE ON PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.REQUEST_EGRESS(STRING,STRING,STRING,STRING,FLOAT) TO ROLE TRE_{{TRE}}_RESEARCHER;
GRANT USAGE ON PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.REVIEW_EGRESS(STRING,STRING,STRING) TO ROLE TRE_{{TRE}}_PI;
GRANT USAGE ON PROCEDURE TRE_{{TRE}}_DB.AIRLOCK.RELEASE_EGRESS(STRING) TO ROLE TRE_{{TRE}}_PI;

-- Reviewers can read the ledger + audit.
GRANT SELECT ON TABLE TRE_{{TRE}}_DB.AIRLOCK.EGRESS_REQUESTS TO ROLE TRE_{{TRE}}_RESEARCHER;
GRANT SELECT ON TABLE TRE_{{TRE}}_DB.AIRLOCK.AIRLOCK_AUDIT   TO ROLE TRE_{{TRE}}_RESEARCHER;

-- The airlock procedures run EXECUTE AS OWNER (the deploy role) so they
-- snapshot + log with integrity. They must be able to READ the researcher-built
-- WORKSPACE objects submitted for egress, so the deploy role gets SELECT on
-- WORKSPACE objects (it owns the procs).
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRE_{{TRE}}_DB.WORKSPACE TO ROLE {{DEPLOY_ROLE}};
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA TRE_{{TRE}}_DB.WORKSPACE TO ROLE {{DEPLOY_ROLE}};
GRANT SELECT ON ALL TABLES    IN SCHEMA TRE_{{TRE}}_DB.WORKSPACE TO ROLE {{DEPLOY_ROLE}};
GRANT SELECT ON ALL VIEWS     IN SCHEMA TRE_{{TRE}}_DB.WORKSPACE TO ROLE {{DEPLOY_ROLE}};

MERGE INTO TRE_{{TRE}}_DB.GOVERNANCE.TRE_CONFIG t
  USING (SELECT 'phase' AS KEY, '3-airlock' AS VALUE) s ON t.KEY = s.KEY
  WHEN MATCHED THEN UPDATE SET VALUE = s.VALUE, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES (s.KEY, s.VALUE);

SELECT 'Output airlock ready: REQUEST_EGRESS (researcher) + REVIEW_EGRESS/RELEASE_EGRESS (PI), SDC + dual sign-off + audit' AS status;
