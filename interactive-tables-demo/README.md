# ER Command Center — Snowflake Interactive Tables & Warehouses Demo

A healthcare demo simulating **Emergency Room patient admissions** powered by [Snowflake Interactive Tables](https://docs.snowflake.com/en/user-guide/interactive-tables) and [Interactive Warehouses](https://docs.snowflake.com/en/user-guide/interactive-warehouses) for sub-second analytics, with a live Streamlit dashboard.

## Architecture

```
ER_ADMISSIONS_SOURCE (standard table)
        │
        │  TARGET_LAG = 1 minute
        ▼
ER_ADMISSIONS_IT (interactive table, CLUSTER BY 8 columns)
        │
        │  sub-second queries
        ▼
ER_INTERACTIVE_WH (interactive warehouse)
        │
        ▼
Streamlit Dashboard (ER Command Center)
```

**Key Snowflake Features Demonstrated:**

| Feature | Description |
|---------|-------------|
| Interactive Tables | Low-latency, high-concurrency tables with mandatory `CLUSTER BY` |
| Dynamic Interactive Tables | Auto-refresh from source via `TARGET_LAG` (like dynamic tables) |
| Interactive Warehouses | Dedicated compute for sub-second SELECT on interactive tables |
| Streamlit in Snowflake | Native dashboard consuming interactive warehouse queries |
| Live Refresh | Insert into source &rarr; refresh interactive table &rarr; see updates instantly |

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role (or equivalent privileges)
- Interactive Tables and Interactive Warehouses enabled (contact your account team if needed)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index) (`snow`) installed locally for Streamlit deployment

## Quick Start

### Step 1: Create Database, Schema, and Seed Data

Run [`01_setup.sql`](01_setup.sql) in a Snowflake worksheet. This creates:
- Database `ER_INTERACTIVE_DEMO` and schema `ER_DATA`
- Standard warehouse `ER_STANDARD_WH` (XS)
- Source table `ER_ADMISSIONS_SOURCE` with 500 synthetic ER admissions

### Step 2: Create Interactive Objects

Run [`02_interactive_objects.sql`](02_interactive_objects.sql). This creates:
- Interactive table `ER_ADMISSIONS_IT` clustered by 8 dimension columns, with `TARGET_LAG = '1 minute'`
- Interactive warehouse `ER_INTERACTIVE_WH` (XS)
- Adds the table to the warehouse

### Step 3: Run Demo Queries

Open [`03_demo_queries.sql`](03_demo_queries.sql) and run queries interactively to see sub-second response times:
- ER census by status
- Triage distribution and avg wait times
- Facility performance comparison
- Top chief complaints
- Physician workload
- Critical patients (Triage 1-2)

### Step 4: Deploy the Streamlit Dashboard

```bash
cd interactive-tables-demo
snow streamlit deploy --replace -c <your_connection_name>
```

The app will be available at:
`https://app.snowflake.com/<org>/<account>/#/streamlit-apps/ER_INTERACTIVE_DEMO.ER_DATA.ER_COMMAND_CENTER`

### Step 5: Demo Live Refresh

**Option A — From the Streamlit UI:**
Click **"Simulate New Admissions"** in the sidebar, then **"Refresh Dashboard"** to see new patients appear.

**Option B — From SQL:**
Run [`04_live_refresh_demo.sql`](04_live_refresh_demo.sql) to INSERT new patients into the source table, trigger a refresh, and verify the updated count through the interactive warehouse.

### Step 6: Cleanup

Run [`06_cleanup.sql`](06_cleanup.sql) to drop all demo objects.

## File Structure

```
interactive-tables-demo/
├── 01_setup.sql                 # Database, schema, source table, seed data (500 rows)
├── 02_interactive_objects.sql   # Interactive table + interactive warehouse
├── 03_demo_queries.sql          # 8 demo queries for sub-second analytics
├── 04_live_refresh_demo.sql     # Live refresh: insert → refresh → verify
├── 06_cleanup.sql               # Drop all demo objects
├── streamlit_app.py             # Streamlit in Snowflake dashboard
├── snowflake.yml                # Snowflake CLI deployment config
├── pyproject.toml               # Python project metadata
└── README.md
```

## Key Design Decisions

### Cluster Key Breadth
The interactive table clusters by **8 columns** — not just `ADMISSION_TIME`. Interactive warehouses require GROUP BY columns to be part of the `CLUSTER BY` clause, so all analytical dimension columns are included:

```sql
CLUSTER BY (ADMISSION_TIME, STATUS, TRIAGE_LEVEL, TRIAGE_LABEL,
            CHIEF_COMPLAINT, FACILITY, ARRIVAL_MODE, DEPARTMENT)
```

### Warehouse Routing
- **Interactive warehouse** (`ER_INTERACTIVE_WH`): All dashboard queries — counts, aggregations, lookups
- **Standard warehouse** (`ER_STANDARD_WH`): Data loading (INSERT) and interactive table management (CREATE, ALTER, REFRESH)

### Streamlit Runtime
Uses the **native SiS runtime** (not SPCS container runtime) for simplicity. The app uses `snowflake.snowpark.context.get_active_session()` for the Snowpark session.

## Important Notes

- Interactive warehouses can **only query interactive tables** — standard tables are not accessible
- Interactive warehouses have a **5-second SELECT timeout**
- The `TARGET_LAG = '1 minute'` means the interactive table auto-refreshes within 1 minute of source changes; use `ALTER INTERACTIVE TABLE ... REFRESH` for immediate refresh during demos
- Interactive warehouses have a minimum auto-suspend of **24 hours** — remember to run cleanup when done to avoid costs
