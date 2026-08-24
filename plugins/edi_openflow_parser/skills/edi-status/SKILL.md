---
name: edi-status
description: Check EDI pipeline health — DT refresh, row counts, errors, streaming lag
parent_skill: edi-router
tools: [snowflake_sql_execute, ask_user_question]
---

# EDI Status — Pipeline Health Monitor

You provide pipeline health information for the EDI ingestion system.

## Health Checks

Run these queries and present a summary dashboard:

### 1. Dynamic Table Status

```sql
SELECT 
    NAME,
    SCHEDULING_STATE,
    LAST_COMPLETED_REFRESH_STATE,
    LAST_COMPLETED_REFRESH_STATE_MESSAGE,
    DATA_TIMESTAMP,
    DATEDIFF('minute', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) AS LAG_MINUTES
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME LIKE '%GOLD%' OR NAME LIKE '%LANDING%'
ORDER BY DATA_TIMESTAMP DESC;
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

```sql
SELECT 
    TABLE_NAME,
    COUNT(*) AS RECORDS_LAST_HOUR
FROM X12_EDI_AI.INFORMATION_SCHEMA.TABLES t
JOIN TABLE(X12_EDI_AI.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => t.TABLE_NAME,
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
)) ch ON TRUE
GROUP BY TABLE_NAME;
```

### 4. Task Status (Lite Path)

```sql
SELECT 
    NAME,
    STATE,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON,
    SCHEDULE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE '%EDI%'
ORDER BY COMPLETED_TIME DESC
LIMIT 10;
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
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
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

Task (Lite Path): RUNNING | Last run: 3 min ago | Next: 2 min
```

## Troubleshooting

If issues found, suggest remediation:
- DT suspended → `ALTER DYNAMIC TABLE ... RESUME`
- High lag → check warehouse size, consider reducing target_lag
- Auth failures → run `/edi:deploy` network phase
- Task suspended → `ALTER TASK ... RESUME`
