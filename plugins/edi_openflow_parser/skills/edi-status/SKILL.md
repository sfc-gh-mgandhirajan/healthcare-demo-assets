---
name: edi-status
description: Check EDI pipeline health — DT refresh, row counts, errors, streaming lag
parent_skill: edi-router
tools: [snowflake_sql_execute, ask_user_question]
---

# EDI Status — Pipeline Health Monitor

You provide pipeline health information for the EDI ingestion system.

## Health Checks

Run these queries and present a summary dashboard. The canonical copies live in
`scripts/verify_pipeline.sql` — keep the two in step if you change either.

### 1. Dynamic Table Status

```sql
SELECT
    NAME,
    STATE,
    STATE_MESSAGE,
    REFRESH_ACTION,
    DATA_TIMESTAMP,
    DATEDIFF('minute', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) AS LAG_MINUTES
FROM TABLE(X12_EDI_AI.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE DATABASE_NAME = 'X12_EDI_AI'
ORDER BY DATA_TIMESTAMP DESC
LIMIT 20;
```

For scheduling state (ACTIVE vs SUSPENDED), which is not exposed by the refresh
history table function, use:

```sql
SHOW DYNAMIC TABLES IN DATABASE X12_EDI_AI;
```

### 2. Landing Table Row Counts

```sql
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    LAST_ALTERED
FROM X12_EDI_AI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME LIKE 'LANDING_%'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```

### 3. Recent Ingestion Activity

Snowpipe Streaming is not tracked by `COPY_HISTORY` (that view covers file-based
`COPY INTO` only), so check landing-table freshness instead:

```sql
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    LAST_ALTERED,
    DATEDIFF('minute', LAST_ALTERED, CURRENT_TIMESTAMP()) AS MINUTES_SINCE_WRITE
FROM X12_EDI_AI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME LIKE 'LANDING_%'
ORDER BY LAST_ALTERED DESC;
```

### 4. Task Status

Only relevant if the deployment schedules its own tasks; the Openflow path does
not create any.

```sql
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(X12_EDI_AI.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
WHERE NAME LIKE '%EDI%'
ORDER BY SCHEDULED_TIME DESC;
```

### 5. Error Summary

```sql
-- Check for Dynamic Table refresh failures
SELECT 
    NAME,
    STATE,
    STATE_MESSAGE,
    REFRESH_START_TIME,
    REFRESH_END_TIME
FROM TABLE(X12_EDI_AI.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE STATE = 'FAILED'
  AND REFRESH_START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY REFRESH_START_TIME DESC;
```

## Output Format

Present results as a health summary:

```
EDI Pipeline Status
===================
Database: X12_EDI_AI
Checked: {timestamp}

Landing Tables:
  CLAIMS.LANDING_837_CLAIMS       | 1,234,567 rows | Last updated: 5 min ago
  ENROLLMENTS.LANDING_834_ENROLL  |   456,789 rows | Last updated: 12 min ago
  REMITTANCES.LANDING_835_REMIT   |   891,234 rows | Last updated: 8 min ago

Gold Dynamic Tables:
  GOLD.GOLD_CLAIMS                | ACTIVE | Lag: 3 min | Last refresh: SUCCESS
  GOLD.GOLD_ENROLLMENTS           | ACTIVE | Lag: 7 min | Last refresh: SUCCESS
  GOLD.GOLD_REMITTANCES           | ACTIVE | Lag: 5 min | Last refresh: SUCCESS

Errors (last 24h): 0
```

## Troubleshooting

If issues found, suggest remediation:
- DT suspended → `ALTER DYNAMIC TABLE ... RESUME`
- High lag → check warehouse size, consider reducing target_lag
- Auth failures → run `/edi:deploy` network phase
- Task suspended → `ALTER TASK ... RESUME`
