# Snowflake Demo Lab Plan

## Goal
- Hands-on lab to showcase Snowpark Python transformations, Streams/Tasks ELT, Dynamic Tables, Snowpipe, and multi-format ingestion (CSV/JSON/Parquet) with live cost/compute visibility.

## Assumptions
- Trial account OK to create account-level objects; use `ACCOUNTADMIN` for setup.
- Demo role `DEMO_LAB_ROLE`.
- Database `DEMO_LAB_DB`, schemas `RAW`, `STAGE`, `CURATED`.
- Python 3.10+ with `snowflake-snowpark-python` available locally for quick tests; primary execution in Snowflake.
- Warehouses: `LAB_INGEST_WH` (S), `LAB_TRANSFORM_WH` (M), `LAB_DEMO_WH` (L for scale-up moment).

## Confirmed Choices
- Task scheduling: after-stream (tasks chained off stream readiness).
- External stage: include S3 example with AWS IAM setup for data landing + Snowflake external stage.

## Demo Flow (matches user steps)
1) Load raw data (multiple formats) into `RAW` using Snowflake stages.
2) Real-time transform with Snowpark Python (table pipeline example).
3) Automate continuous processing with Tasks; Streams for CDC.
4) Live scale-up/down of warehouse to show performance difference.
5) Show cost/credit consumption using `SNOWFLAKE.ACCOUNT_USAGE` views.

## Scenario Outline
- Dataset theme: “Orders & Clickstream”
  - `orders.csv` (structured), `events.json` (semi-structured), `inventory.parquet` (columnar).
- Landing:
  - Internal named stage `@raw_stage`.
  - External S3 stage `@raw_ext_stage` for demoing cloud landing → Snowflake ingest.
  - `COPY INTO RAW.ORDERS`, `RAW.EVENTS`, `RAW.INVENTORY` using file-format per type from internal or external stage.
- Snowpark Transform (real-time-ish):
  - Snowpark Python procedure `sp_transform_orders()` reading from `RAW.*`, performing joins/aggregations, writing to `STAGE.ORDERS_ENRICHED`.
  - Optionally demonstrate DataFrame UDF for custom scoring.
- Streams/Tasks pipeline:
  - Streams on `RAW` tables to capture new/changed data.
  - Task chain: `task_transform_stage` (AFTER stream) → `task_publish_curated` (AFTER previous task); no cron.
  - Uses `CALL sp_transform_orders()` inside tasks for orchestration.
- Curated outputs:
  - `CURATED.ORDER_METRICS`, `CURATED.EVENT_FACTS`.

## Multi-Format Handling
- File formats:
  - `ff_csv_orders`: CSV with header, auto type detection.
  - `ff_json_events`: JSON, strip outer array if present.
  - `ff_parquet_inventory`: PARQUET (snappy).
- COPY examples will be part of notebooks/scripts.

## Snowpipe vs COPY INTO
- **COPY INTO** (batch):
  - Best for: scheduled bulk loads, backfills, infrequent file arrivals, cost-sensitive workloads.
  - Cost: pay only for warehouse compute when COPY runs.
  - Latency: depends on schedule (minutes to hours).
- **Snowpipe** (continuous):
  - Best for: near-real-time ingestion, frequent small files, event-driven architectures.
  - Cost: serverless compute (per-file overhead ~0.06 credits/1000 files); no warehouse needed.
  - Latency: typically < 1 minute after file lands.
- **Guidance**:
  - Use COPY INTO when files arrive in large batches or on a predictable schedule.
  - Use Snowpipe when files trickle in continuously and low latency matters.
  - For very high file counts with small files, Snowpipe serverless cost can add up—batch with COPY may be cheaper.
- **Demo**: Create a Snowpipe on `@raw_ext_stage` for `orders/` prefix; show AUTO_INGEST with S3 event notifications.

## Dynamic Tables
- Alternative to Streams+Tasks for declarative, incremental transformations.
- Define transformation SQL once; Snowflake auto-refreshes based on `TARGET_LAG`.
- **Benefits**:
  - Simpler to manage than task DAGs.
  - Built-in incremental refresh (no manual stream consumption).
  - Automatic dependency tracking across chained dynamic tables.
- **Trade-offs**:
  - Less control over exact execution timing vs. tasks.
  - Serverless or warehouse compute; cost depends on refresh frequency.
- **Demo**: Create `STAGE.ORDERS_ENRICHED_DT` as a Dynamic Table sourcing from `RAW.ORDERS_RAW` + `RAW.INVENTORY_RAW` with `TARGET_LAG = '1 minute'`.

## AWS S3 External Stage Setup (example)
- AWS
  - Create bucket `s3://demo-lab-landing` with prefix `raw/`.
  - Create IAM role `SnowflakeExternalStageRole` with trust to Snowflake AWS account `<snowflake_aws_account_id>` and external ID from `DESCRIBE INTEGRATION` (placeholder `<integration_external_id>`).
  - Attach policy:
    - `s3:ListBucket` on the bucket with prefix condition `raw/`.
    - `s3:GetObject` on `demo-lab-landing/raw/*`.
    - Optional for unload/backfill: `s3:PutObject`, `s3:DeleteObject` on the same prefix.
  - Land files via `aws s3 cp data-samples/orders.csv s3://demo-lab-landing/raw/orders.csv` (repeat for JSON/Parquet).
- Snowflake
  - `CREATE OR REPLACE STORAGE INTEGRATION lab_s3_int TYPE=EXTERNAL_STAGE STORAGE_PROVIDER=S3 ENABLED=TRUE STORAGE_AWS_ROLE_ARN='<iam_role_arn>' STORAGE_ALLOWED_LOCATIONS=('s3://demo-lab-landing/raw/');`
  - `DESC INTEGRATION lab_s3_int;` to retrieve `EXTERNAL_ID` for IAM trust.
  - `CREATE OR REPLACE STAGE raw_ext_stage URL='s3://demo-lab-landing/raw/' STORAGE_INTEGRATION=lab_s3_int;`
  - Use per-format file formats in `COPY INTO` calls (CSV/JSON/Parquet) from `@raw_ext_stage`.

## Compute & Cost Demonstration
- Show `ALTER WAREHOUSE LAB_TRANSFORM_WH SET WAREHOUSE_SIZE = 'LARGE';` then back to `SMALL`.
- Query credit usage:
  - `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` filtered to demo warehouses.
  - `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for task/proc queries.
- Optional quick dashboard: simple worksheet charts or Streamlit-in-Snowflake snippet to visualize credits by time.

## Repo Structure (proposed)
- `notebooks/` — step-by-step lab notebooks (setup, ingestion, transformation, automation, cost).
- `sql/` — DDL/DML (db/role/warehouse, stages, file formats, streams, tasks).
- `python/` — Snowpark Python procedure code and helper scripts.
- `data-samples/` — small example CSV/JSON/Parquet for local upload.
- `docs/` — lab guide and walkthrough.

## Deliverables (initial)
- Setup script: create role/user/warehouses/db/schemas/stage/file-formats.
- Sample data files (CSV/JSON/Parquet).
- Snowpark Python stored procedure + optional UDF example.
- Streams + Tasks SQL for ELT chain.
- Snowpipe example with S3 event notification setup guidance.
- Dynamic Table example as alternative transformation approach.
- Notebook/guide to run demo end-to-end, including scale-up/down and cost queries.

## Open Questions
- None — preferences supplied (after-stream tasks, include S3 external stage, account-level objects allowed).


