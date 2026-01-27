# Snowflake Trading Lab - ELT Patterns

Hands-on lab demonstrating modern ELT patterns in Snowflake using financial services data.

## What You'll Learn

| Concept | Snowflake Feature |
|---------|-------------------|
| Data Ingestion | External Stages, Storage Integrations, COPY INTO |
| Change Data Capture | Streams |
| Orchestration | Tasks, Task Chaining |
| Analytics | DBT (staging → marts) |
| Cost Control | Resource Monitors |

## Architecture

```
AWS S3 ─────────────────────────────────────────────────────────────────────────┐
│  trades.csv, market_events.json, positions.parquet                            │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │ COPY INTO
                                    ▼
┌─────────────────────────────── SNOWFLAKE ─────────────────────────────────────┐
│                                                                               │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌───────────┐   │
│  │   RAW   │───▶│  STAGE  │───▶│ CURATED │───▶│ STAGING │───▶│ ANALYTICS │   │
│  │ tables  │    │ tables  │    │  tables │    │  (DBT)  │    │   (DBT)   │   │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘    └───────────┘   │
│       │              ▲              ▲                                         │
│       │   Stream     │    Task      │                                         │
│       └──────────────┴──────────────┘                                         │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Snowflake account (trial OK)
- AWS S3 bucket with [storage integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- Python 3.10+ and DBT (`pip install dbt-snowflake`)

## Quick Start

### 1. Upload Data to S3

```bash
aws s3 cp data-samples/trades.csv s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/market_events.json s3://YOUR-BUCKET/finserv-getting-started/
aws s3 cp data-samples/positions.parquet s3://YOUR-BUCKET/finserv-getting-started/
```

### 2. Run SQL Scripts (in order)

In Snowsight, run each script as `ACCOUNTADMIN`:

| # | Script | Duration | What It Does |
|---|--------|----------|--------------|
| 0 | `00_setup.sql` | 2 min | Database, schemas, warehouses, roles |
| 1 | `01_stages_formats.sql` | 3 min | S3 stage, file formats, COPY INTO |
| 2a | `02a_streams_demo.sql` | 5 min | Learn how Streams work (CDC) |
| 2b | `02b_transform_demo.sql` | 5 min | Build the transformation logic (SQL) |
| 2c | `02c_tasks_demo.sql` | 5 min | Automate with Tasks |
| 2d | `02d_snowpark_procedure.sql` | 5 min | **Optional:** Python with Snowpark |
| 3 | `03_dbt_setup.sql` | 2 min | DBT role and permissions |

> ⚠️ **Edit `01_stages_formats.sql`** to use your S3 bucket URL before running.

### 3. Run DBT

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your credentials
export DBT_PROFILES_DIR=$(pwd)
dbt run
dbt test
```

## Lab Flow

The lab is designed to be **interactive** - each script demonstrates a concept and verifies it works before moving on:

```
02a: Create stream → Insert data → See stream capture it! ✓
02b: Run transformation manually → Verify enriched data ✓  
02c: Create tasks → Insert data → Watch automation work! ✓
```

## Documentation

| Document | Audience |
|----------|----------|
| **[docs/lab-guide.md](docs/lab-guide.md)** | Step-by-step hands-on lab |
| **[docs/SE-GUIDE.md](docs/SE-GUIDE.md)** | Demo prep for Snowflake SEs |

## Repository Structure

```
├── sql/                          # Run these in Snowsight (in order)
│   ├── 00_setup.sql              # Foundation
│   ├── 01_stages_formats.sql     # Ingestion (edit S3 URL)
│   ├── 02a_streams_demo.sql      # Learn: Streams (CDC)
│   ├── 02b_transform_demo.sql    # Learn: Transformations (SQL)
│   ├── 02c_tasks_demo.sql        # Learn: Task automation
│   ├── 02d_snowpark_procedure.sql # Optional: Python transforms
│   ├── 03_dbt_setup.sql          # DBT permissions
│   ├── 04_cost_monitoring.sql    # Resource monitors
│   └── 05_advanced_optional.sql  # Time travel, governance
│
├── dbt/                          # Analytics layer
│   ├── models/staging/           # Views on CURATED
│   ├── models/marts/             # Facts & dimensions
│   └── profiles.yml.example      # Copy to profiles.yml
│
├── data-samples/                 # Upload to S3
└── docs/                         # Documentation
```

## Cleanup

```sql
-- Suspend tasks first
ALTER TASK TRADING_LAB_DB.STAGE.TASK_AGGREGATE_METRICS SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_UPDATE_POSITIONS SUSPEND;

-- Drop objects
DROP DATABASE IF EXISTS TRADING_LAB_DB;
DROP WAREHOUSE IF EXISTS TRADING_INGEST_WH;
DROP WAREHOUSE IF EXISTS TRADING_TRANSFORM_WH;
DROP WAREHOUSE IF EXISTS TRADING_ANALYTICS_WH;
DROP ROLE IF EXISTS TRADING_LAB_ROLE;
DROP ROLE IF EXISTS DBT_TRADING_ROLE;
```

## Resources

- [Snowflake Streams](https://docs.snowflake.com/en/user-guide/streams)
- [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Storage Integrations](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [DBT + Snowflake](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)
