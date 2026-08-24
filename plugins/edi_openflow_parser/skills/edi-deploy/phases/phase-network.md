---
name: phase-network
description: Verify and update network policy for SPCS/Openflow container IPs
parent_skill: edi-deploy
phase_id: DEPLOY_P3
---

# Phase: Network Policy Verification

## Purpose
Ensure SPCS container IPs (used by Openflow runtime) are allowed through the account's network policy.

## Background

Openflow runs on SPCS (Snowpark Container Services). The containers use IPs that rotate within a CIDR range. If the account has a network policy that restricts login IPs, the PutSnowpipeStreaming processor will get blocked with auth failures.

## Steps

1. **Identify current network policy**:
   ```sql
   SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;
   ```

2. **Get allowed IPs**:
   ```sql
   DESCRIBE NETWORK POLICY {policy_name};
   ```

3. **Check Openflow SPCS IPs**:
   - Check LOGIN_HISTORY for recent blocked attempts from SPCS:
   ```sql
   SELECT CLIENT_IP, ERROR_CODE, ERROR_MESSAGE, EVENT_TIMESTAMP
   FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
   WHERE IS_SUCCESS = 'NO'
     AND ERROR_CODE = 390146
     AND EVENT_TIMESTAMP > DATEADD('hour', -24, CURRENT_TIMESTAMP())
   ORDER BY EVENT_TIMESTAMP DESC
   LIMIT 20;
   ```

4. **If blocked IPs found**:
   - Identify the CIDR range (typically /24)
   - Present the ALTER NETWORK POLICY statement
   - **REQUIRE EXPLICIT USER CONFIRMATION** before executing

   ```sql
   -- WARNING: This modifies your network policy. Review carefully.
   ALTER NETWORK POLICY {policy_name} SET
       ALLOWED_IP_LIST = ('{existing_ips}', '{new_spcs_cidr}/24');
   ```

## Critical Safety Notes

- **Never auto-execute network policy changes** — wrong modifications can lock out the entire account
- Use /24 CIDR for SPCS IPs (they rotate within the range)
- The blocked IP pattern from our experience: `153.45.59.0/24` (but varies by region/deployment)
- Always show the full before/after allowed IP list for user review

## Pass Criteria
- No blocked SPCS IPs in LOGIN_HISTORY, OR
- User has reviewed and approved the network policy update

## Output
- Current network policy status
- Any recommended changes (with full ALTER statement)
- Confirmation that PutSnowpipeStreaming auth should succeed
