# Trading Lab - Hands-On Lab Guide

**Duration**: ~45 minutes  
**Audience**: Data engineers learning Snowflake ELT patterns  
**Prerequisites**: 
- Snowflake account (trial OK)
- AWS S3 bucket with storage integration configured
- Basic SQL knowledge

---

## What You'll Build

A complete ELT pipeline for financial trading data:

```
S3 → RAW Tables → Streams (CDC) → Tasks → Enriched Data → DBT → Analytics
```

---

## Step 0: Upload Data to S3 (5 min)

Upload sample data to your S3 bucket:

```bash
aws s3 cp data-samples/trades.csv s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/market_events.json s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/positions.parquet s3://YOUR-BUCKET/finserv-getting-started/

# Verify
aws s3 ls s3://YOUR-BUCKET/finserv-getting-started/
```

---

## Step 1: Setup (5 min)

**Open Snowsight** → Worksheets → + New Worksheet

**Copy and paste** the entire contents of `sql/00_setup.sql` and run it.

**Verify:**
```sql
SHOW SCHEMAS IN DATABASE TRADING_LAB_DB;
-- Should show: RAW, STAGE, CURATED, ANALYTICS
```

---

## Step 2: Load Data from S3 (5 min)

**Before running:** Edit `sql/01_stages_formats.sql` to use your S3 bucket URL.

**Copy and paste** the entire contents and run it.

**Verify:**
```sql
SELECT COUNT(*) FROM TRADING_LAB_DB.RAW.TRADES_RAW;
-- Should return ~55 rows
```

---

## Step 3: Learn Streams - CDC (5 min)

**Copy and paste** `sql/02a_streams_demo.sql` and run **step by step**.

This script teaches you:
- How to create a stream
- How streams capture INSERT changes
- How `SYSTEM$STREAM_HAS_DATA()` works
- How streams are "consumed" when you read them

**Key Insight:** After this step, you'll understand that streams track changes automatically - no polling needed!

---

## Step 4: Build Transformations (5 min)

**Copy and paste** `sql/02b_transform_demo.sql` and run **step by step**.

This script:
- Creates destination tables (STAGE and CURATED)
- Shows the transformation logic
- Lets you manually run a transformation
- Demonstrates the enrichment (notional value, P&L)

**Key Insight:** You'll see exactly what data flows from RAW → STAGE → CURATED.

---

## Step 5: Automate with Tasks (5 min)

**Copy and paste** `sql/02c_tasks_demo.sql` and run **step by step**.

This script:
- Creates a task that runs WHEN stream has data
- Creates a chained task (AFTER clause)
- Enables the tasks
- Lets you test the automation

**Key Insight:** Insert a trade, watch it flow through automatically!

```sql
-- Insert a test trade
INSERT INTO RAW.TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('TEST-001', 'TSLA', 'BUY', 25, 245.00, CURRENT_TIMESTAMP(), 'ACCT-LAB', 'NASDAQ');

-- Execute task immediately (or wait 1 minute)
EXECUTE TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES;

-- Check results
SELECT * FROM STAGE.TRADES_ENRICHED WHERE TRADE_ID = 'TEST-001';
SELECT * FROM CURATED.TRADE_METRICS WHERE ACCOUNT_ID = 'ACCT-LAB';
```

---

## Step 5b: Snowpark Python (Optional - 5 min)

Want to use Python instead of SQL for transformations? Run `sql/02d_snowpark_procedure.sql`.

This demonstrates:
- Creating a Snowpark stored procedure
- Python transformation logic
- Using fully qualified table names (required in procedures)

```sql
-- Test the procedure
INSERT INTO RAW.TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('PY-001', 'META', 'BUY', 30, 505.00, CURRENT_TIMESTAMP(), 'ACCT-PY', 'NASDAQ');

CALL SP_TRANSFORM_TRADES();
-- Returns: "SP_TRANSFORM_TRADES: 1 rows processed"

SELECT * FROM STAGE.TRADES_ENRICHED WHERE TRADE_ID = 'PY-001';
```

**When to use Snowpark:**
- Complex business logic
- Need Python libraries (pandas, numpy)
- Data science team contributions
- Want unit-testable code

---

## Step 6: Set Up DBT (5 min)

**Copy and paste** `sql/03_dbt_setup.sql` and run it.

**In your terminal:**

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your Snowflake credentials
export DBT_PROFILES_DIR=$(pwd)
dbt debug
```

**Expected:** "All checks passed!"

---

## Step 7: Run DBT Models (5 min)

```bash
dbt run
```

**Expected:**
```
Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```

**Query Results:**
```sql
-- Daily P&L
SELECT * FROM TRADING_LAB_DB.ANALYTICS.FCT_DAILY_PNL;

-- Account performance
SELECT * FROM TRADING_LAB_DB.ANALYTICS.FCT_ACCOUNT_SUMMARY;
```

---

## Pipeline Architecture

```
                         SNOWFLAKE
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   S3 Files                                                      │
│      │                                                          │
│      │ COPY INTO                                                │
│      ▼                                                          │
│   ┌─────────────┐                                               │
│   │  RAW Tables │ ─── Stream tracks changes                     │
│   └──────┬──────┘                                               │
│          │                                                      │
│          │  Task: WHEN stream has data                          │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │ STAGE Tables│ ─── Enriched (notional, P&L)                  │
│   └──────┬──────┘                                               │
│          │                                                      │
│          │  Task: AFTER transform                               │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │CURATED Table│ ─── Daily aggregates                          │
│   └──────┬──────┘                                               │
│          │                                                      │
│          │  DBT                                                 │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │  ANALYTICS  │ ─── Facts & Dimensions                        │
│   └─────────────┘                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Takeaways

| Concept | Snowflake Feature | Benefit |
|---------|-------------------|---------|
| CDC | Streams | No external CDC tools needed |
| Orchestration | Tasks | Serverless, no infrastructure |
| Conditional Execution | WHEN clause | Only run when there's work |
| Chaining | AFTER clause | Automatic dependencies |
| Analytics | DBT | Version-controlled, tested SQL |

---

## Cleanup

When done:

```sql
-- Suspend tasks first
ALTER TASK TRADING_LAB_DB.STAGE.TASK_AGGREGATE_METRICS SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_UPDATE_POSITIONS SUSPEND;

-- Drop everything
DROP DATABASE IF EXISTS TRADING_LAB_DB;
DROP WAREHOUSE IF EXISTS TRADING_INGEST_WH;
DROP WAREHOUSE IF EXISTS TRADING_TRANSFORM_WH;
DROP WAREHOUSE IF EXISTS TRADING_ANALYTICS_WH;
DROP ROLE IF EXISTS TRADING_LAB_ROLE;
DROP ROLE IF EXISTS DBT_TRADING_ROLE;
```

---

## Troubleshooting

### S3 Stage: "Access Denied"
```sql
DESC INTEGRATION S3_INT;
-- Check STORAGE_ALLOWED_LOCATIONS includes your bucket
```

### Task Not Running
```sql
SHOW TASKS IN SCHEMA TRADING_LAB_DB.STAGE;
-- STATE should be 'started'

-- If suspended, resume:
ALTER TASK TASK_TRANSFORM_TRADES RESUME;
```

### Stream Has No Data
```sql
-- Insert test data to trigger stream
INSERT INTO RAW.TRADES_RAW (...) VALUES (...);
SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRADES_RAW_STREAM');
```

### DBT Connection Failed
```bash
echo $DBT_PROFILES_DIR  # Should be the dbt/ directory
dbt debug              # Shows detailed connection info
```
