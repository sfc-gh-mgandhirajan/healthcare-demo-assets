---
name: secure-sharing
description: "Secure data sharing for multi-tenant platforms: Snowflake shares, declarative sharing, Marketplace listings (private/public), reader accounts, data clean rooms, cross-region replication for sharing, database roles in shares. Triggers: secure sharing, data sharing, share, marketplace, listing, reader account, data clean room, cross-account, declarative sharing, data exchange, private listing, public listing."
platform_affinities:
  produces:
    - shares
    - marketplace listings
    - reader accounts
  benefits_from:
    - skill: declarative-sharing
      when: "customer wants to manage shares as code via declarative sharing syntax"
    - skill: data-cleanrooms
      when: "privacy-preserving joint analysis across tenants or external parties is needed"
---

# Secure Sharing & Distribution

## When to Use

**Load** this skill when the customer needs to share data products with tenants, external consumers, or the Snowflake Marketplace.

---

## Step 1: Understand Sharing Requirements

Gather:

| Input | Description |
|-------|-------------|
| Who are the consumers? | Internal tenants, external Snowflake accounts, non-Snowflake consumers |
| What data is shared? | Raw data, curated views, aggregated metrics, ML model outputs |
| Sharing direction | Provider → Consumer, Bidirectional, Marketplace |
| Consumer Snowflake accounts? | Yes (direct share) or No (reader accounts needed) |
| Cross-region sharing needed? | Same region or multi-region distribution |
| Monetization intent? | Free, paid listing, tiered pricing |
| Privacy requirements? | Full data, aggregated only, clean room needed |

---

## Step 2: Sharing Patterns

### Pattern A: Direct Shares (Account-to-Account)

**MTT: Share via Secure Views (RLAP-enforced)**

```sql
CREATE OR REPLACE SECURE VIEW {{DATA_DB}}.{{SCHEMA}}.V_{{DATA_PRODUCT_NAME}}
AS
SELECT {{COLUMNS}}
FROM {{DATA_DB}}.{{SCHEMA}}.{{SOURCE_TABLE}};

CREATE SHARE IF NOT EXISTS SHARE_{{DATA_PRODUCT_NAME}};
GRANT USAGE ON DATABASE {{DATA_DB}} TO SHARE SHARE_{{DATA_PRODUCT_NAME}};
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{SCHEMA}} TO SHARE SHARE_{{DATA_PRODUCT_NAME}};
GRANT SELECT ON VIEW {{DATA_DB}}.{{SCHEMA}}.V_{{DATA_PRODUCT_NAME}}
    TO SHARE SHARE_{{DATA_PRODUCT_NAME}};

ALTER SHARE SHARE_{{DATA_PRODUCT_NAME}} ADD ACCOUNTS = {{CONSUMER_ACCOUNT}};
```

**OPT: Share via Schema**

```sql
CREATE SHARE IF NOT EXISTS SHARE_{{TENANT_ID}};
GRANT USAGE ON DATABASE {{DATA_DB}} TO SHARE SHARE_{{TENANT_ID}};
GRANT USAGE ON SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}} TO SHARE SHARE_{{TENANT_ID}};
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}}
    TO SHARE SHARE_{{TENANT_ID}};
GRANT SELECT ON ALL VIEWS IN SCHEMA {{DATA_DB}}.{{TENANT_SCHEMA}}
    TO SHARE SHARE_{{TENANT_ID}};

ALTER SHARE SHARE_{{TENANT_ID}} ADD ACCOUNTS = {{CONSUMER_ACCOUNT}};
```

### Pattern B: Declarative Sharing (Recommended for Versioned Data Products)

Use **Application Packages with TYPE=DATA** for versioned, managed data distribution:

```sql
CREATE APPLICATION PACKAGE IF NOT EXISTS {{APP_PACKAGE_NAME}}
    DATA_RETENTION_TIME_IN_DAYS = 1;

CREATE SCHEMA IF NOT EXISTS {{APP_PACKAGE_NAME}}.{{CONTENT_SCHEMA}};

CREATE OR REPLACE VIEW {{APP_PACKAGE_NAME}}.{{CONTENT_SCHEMA}}.V_{{DATA_PRODUCT}}
AS SELECT {{COLUMNS}}
FROM {{DATA_DB}}.{{SCHEMA}}.{{SOURCE_TABLE}};

GRANT USAGE ON SCHEMA {{APP_PACKAGE_NAME}}.{{CONTENT_SCHEMA}}
    TO SHARE IN APPLICATION PACKAGE {{APP_PACKAGE_NAME}};
GRANT SELECT ON VIEW {{APP_PACKAGE_NAME}}.{{CONTENT_SCHEMA}}.V_{{DATA_PRODUCT}}
    TO SHARE IN APPLICATION PACKAGE {{APP_PACKAGE_NAME}};
```

### Pattern C: Reader Accounts (Non-Snowflake Consumers)

```sql
CREATE MANAGED ACCOUNT {{CONSUMER_NAME}}_READER
    ADMIN_NAME = '{{admin_user}}'
    ADMIN_PASSWORD = '{{secure_password}}'
    TYPE = READER;

ALTER SHARE SHARE_{{DATA_PRODUCT_NAME}} ADD ACCOUNTS = {{reader_account_locator}};
```

### Pattern D: Database Roles in Shares

```sql
CREATE DATABASE ROLE IF NOT EXISTS {{DATA_DB}}.DR_{{DATA_PRODUCT}}_READ;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{DATA_DB}}.{{SCHEMA}}
    TO DATABASE ROLE {{DATA_DB}}.DR_{{DATA_PRODUCT}}_READ;
GRANT DATABASE ROLE {{DATA_DB}}.DR_{{DATA_PRODUCT}}_READ TO SHARE {{SHARE_NAME}};
```

---

## Step 3: Snowflake Marketplace

### Private Listings (Internal Distribution)

For distributing curated data products to known tenants/partners:

1. Create a **Private Data Exchange** for the organization
2. List data products as **Private Listings** visible only to invited consumers
3. Consumers discover and request access through the Marketplace UI
4. No data duplication — consumers query provider data directly

### Public Listings (Revenue Stream)

For monetizing aggregated, anonymized data products:

1. Create aggregated, fully de-identified datasets
2. Apply k-anonymity or differential privacy techniques
3. List on Snowflake Marketplace with appropriate usage terms and pricing
4. Use **auto-fulfillment** for frictionless consumer onboarding

### Cross-Region Listing Replication

For sharing data across regions, use **database replication** behind the listing:

```sql
ALTER DATABASE {{DATA_DB}} ENABLE REPLICATION TO ACCOUNTS {{TARGET_ACCOUNT}};
```

---

## Step 4: Data Clean Rooms

For cross-tenant or cross-organization analysis without exposing raw data:

```
Provider data  ──┐
                   ├──→ Data Clean Room ──→ Aggregated insights only
Consumer data  ──┘        (no raw data exposed)
```

Snowflake Data Clean Rooms enable:
- **Overlap analysis** between tenant populations
- **Aggregated benchmarking** with minimum group sizes enforced
- **Privacy-preserving joins** — intersection without revealing non-matching rows
- **Custom analysis templates** — provider controls what computations consumers can run

---

## Step 5: Interoperability Considerations

| Scenario | Approach |
|----------|----------|
| Share with non-Snowflake consumer | Reader Account or API endpoint via Snowpark Container Services |
| Share with cloud-native tools | Iceberg Tables for open format interoperability |
| Cross-cloud sharing | Database replication + Listings |
| Programmatic access | Snowflake REST API or Snowpark-based API service |

---

## Output

**MANDATORY STOPPING POINT**: Present the sharing strategy for customer approval before generating scripts.

Deliver:
1. **Sharing architecture diagram** — which data products to which consumers via which method
2. **Share/listing configuration scripts** — parameterized SQL
3. **Reader account setup** — if non-Snowflake consumers exist
4. **Marketplace listing plan** — private vs public, pricing model
5. **Data clean room recommendation** — if cross-tenant analytics needed
6. **Interoperability plan** — if non-Snowflake integration needed
7. **Edition requirements** — see table below

### Edition Requirements

| Feature | Minimum Edition |
|---------|----------------|
| Secure Data Sharing (CREATE SHARE) | Standard+ |
| Reader Accounts | Standard+ |
| Database Roles in Shares | Standard+ |
| Private Marketplace Listings | Standard+ |
| Data Clean Rooms | **Enterprise** |
| Cross-region database replication | **Business Critical** |
| Iceberg Tables | Standard+ |

After approval, route to:
- `implementation` for executable scripts
- `governance-security` for ensuring shared data is properly governed
