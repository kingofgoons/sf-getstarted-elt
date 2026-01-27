# SE Guide - Running the Trading Lab Demo

**Audience**: Snowflake Solution Engineers  
**Duration**: ~55 minutes  
**Purpose**: Step-by-step guide for SEs to run this demo with customers

---

## Pre-Demo Checklist

### 1. Environment Setup (Do Before Customer Meeting)

- [ ] Clone this repo to your local machine
- [ ] Verify Python 3.10+ installed
- [ ] Verify DBT installed (`pip install dbt-snowflake`)
- [ ] Test Snowflake connectivity
- [ ] Upload sample data to S3 (see below)

### 2. Snowflake Account Requirements

- ACCOUNTADMIN role access (for setup scripts)
- Existing storage integration for S3 (`S3_INT` or similar)
- Trial account OK - all features used are available in trial

### 3. S3 Data Setup

Upload sample data to your S3 bucket:

```bash
cd sf-getstarted-elt
aws s3 cp data-samples/trades.csv s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/market_events.json s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/positions.parquet s3://YOUR-BUCKET/finserv-getting-started/
```

### 4. Update SQL Files

Edit `sql/01_stages_formats.sql` line ~89 to use your S3 bucket:

```sql
CREATE OR REPLACE STAGE RAW_S3_STAGE
    STORAGE_INTEGRATION = S3_INT  -- Use your integration name
    URL = 's3://YOUR-BUCKET/finserv-getting-started/'
    ...
```

### 5. Configure DBT Profile

Create `dbt/profiles.yml` with your credentials:

```yaml
trading_lab:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      password: YOUR_PASSWORD  # Or use key pair auth
      role: DBT_TRADING_ROLE
      warehouse: TRADING_ANALYTICS_WH
      database: TRADING_LAB_DB
      schema: ANALYTICS
      threads: 4
```

---

## Demo Flow

### Phase 1: Foundation (5 min)

**Talking Points:**
- Medallion architecture (RAW → STAGE → CURATED → ANALYTICS)
- Role-based access control
- Right-sized warehouses for different workloads

**Execute:**
```sql
-- Run in Snowsight as ACCOUNTADMIN
-- Execute: sql/00_setup.sql
```

**Verify:**
```sql
SHOW WAREHOUSES LIKE 'TRADING%';
SHOW SCHEMAS IN DATABASE TRADING_LAB_DB;
```

### Phase 2: Data Ingestion (10 min)

**Talking Points:**
- External stages with storage integrations (no credentials in SQL)
- File formats for CSV, JSON, Parquet
- COPY INTO for bulk loading

**Execute:**
```sql
-- Execute: sql/01_stages_formats.sql
```

**Verify:**
```sql
LIST @RAW_S3_STAGE;
SELECT COUNT(*) FROM TRADES_RAW;
```

### Phase 3: CDC Pipeline (15 min)

**Talking Points:**
- Streams for change data capture (no external CDC tools needed)
- Tasks for orchestration (serverless scheduling)
- Task chaining with AFTER clause
- WHEN clause for conditional execution

**Execute:**
```sql
-- Execute: sql/02_streams_tasks.sql
```

**Demo the CDC:**
```sql
-- Insert new data
INSERT INTO TRADES_RAW (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES ('DEMO-001', 'AAPL', 'BUY', 100, 185.50, CURRENT_TIMESTAMP(), 'DEMO-ACCT', 'NYSE');

-- Check stream has data
SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM');

-- Wait for task or execute manually
EXECUTE TASK TASK_TRANSFORM_TRADES;
```

### Phase 4: DBT Analytics (15 min)

**Talking Points:**
- DBT for analytics engineering
- Staging → Intermediate → Marts pattern
- Version-controlled transformations
- Built-in testing and documentation

**Execute:**
```bash
cd dbt
export DBT_PROFILES_DIR=$(pwd)
dbt run
dbt test
```

**Show Results:**
```sql
SELECT * FROM ANALYTICS.FCT_DAILY_PNL LIMIT 10;
SELECT * FROM ANALYTICS.FCT_ACCOUNT_SUMMARY;
```

### Phase 5: Cost Control (5 min)

**Talking Points:**
- Resource monitors for budget control
- Auto-suspend and auto-resume
- Per-warehouse cost tracking

**Execute:**
```sql
-- Execute: sql/04_cost_monitoring.sql (first part only for demo)
SHOW RESOURCE MONITORS;
```

### Phase 6: Wrap-up (5 min)

**Key Messages:**
1. **Native CDC** - Streams eliminate need for external CDC tools
2. **Serverless Orchestration** - Tasks run without managing infrastructure
3. **DBT Integration** - Industry-standard analytics engineering
4. **Cost Control** - Built-in resource monitors and metering

---

## Common Issues & Fixes

### "Insufficient privileges" error
```sql
-- Grant missing permissions
GRANT CREATE SCHEMA ON DATABASE TRADING_LAB_DB TO ROLE DBT_TRADING_ROLE;
```

### S3 "access denied" error
1. Check storage integration: `DESC INTEGRATION S3_INT;`
2. Verify IAM role trust policy includes Snowflake's external ID
3. Confirm bucket path matches STORAGE_ALLOWED_LOCATIONS

### Task not running
```sql
-- Check task state
SHOW TASKS IN SCHEMA TRADING_LAB_DB.STAGE;

-- Verify stream has data
SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM');

-- Resume if suspended
ALTER TASK TASK_TRANSFORM_TRADES RESUME;
```

### DBT connection failed
```bash
# Verify profile
dbt debug

# Check DBT_PROFILES_DIR is set
echo $DBT_PROFILES_DIR
```

---

## Cleanup

After demo, clean up to avoid charges:

```sql
-- Suspend tasks first
ALTER TASK TRADING_LAB_DB.STAGE.TASK_PUBLISH_CURATED SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES SUSPEND;

-- Drop all objects
DROP DATABASE IF EXISTS TRADING_LAB_DB;
DROP WAREHOUSE IF EXISTS TRADING_INGEST_WH;
DROP WAREHOUSE IF EXISTS TRADING_TRANSFORM_WH;
DROP WAREHOUSE IF EXISTS TRADING_ANALYTICS_WH;
DROP ROLE IF EXISTS TRADING_LAB_ROLE;
DROP ROLE IF EXISTS DBT_TRADING_ROLE;
DROP RESOURCE MONITOR IF EXISTS TRADING_LAB_MONITOR;
```

---

## Customer Follow-up Materials

Share these with the customer after the demo:

1. **This Repository** - They can clone and run themselves
2. **Snowflake Documentation**:
   - [Streams](https://docs.snowflake.com/en/user-guide/streams)
   - [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
   - [Storage Integrations](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
3. **DBT Resources**:
   - [DBT + Snowflake Setup](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)

