---
name: gate-connection
description: Verify Snowflake connection and target database exist
parent_skill: edi-extend
gate_id: G1
---

# Gate: Connection Verification

## Purpose
Verify that the user's Snowflake connection is active and the target database infrastructure exists (or get approval to create it).

## Steps

1. **Test connection**: Run `SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()`
2. **Check database exists**: Run `SHOW DATABASES LIKE 'X12_EDI_AI'`
   - If exists: report schemas and proceed
   - If not: ask user if they want to create it (show the DDL from `config/edi_format_specs.yaml` defaults)
3. **Check Cortex AI access**: Run `SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', 'test') AS test`
   - If fails: warn that Gold layer AI enrichment won't work, but proceed (not blocking)
4. **Check warehouse**: Verify configured warehouse exists and is accessible

## Pass Criteria
- Snowflake connection is active
- Target database exists OR user approves creation
- User has CREATE TABLE, CREATE DYNAMIC TABLE privileges in target database

## Failure Handling
- Connection failure: ask user to verify their Snowflake connection in CoCo settings
- Missing privileges: suggest role requirements (SYSADMIN or custom role with grants)
