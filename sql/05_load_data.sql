-- Load sample data into RAW tables
-- Run after uploading files to S3: aws s3 cp data-samples/<file> s3://demo-lab-landing/raw/
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA RAW;
USE WAREHOUSE LAB_INGEST_WH;

-- ============================================================
-- Option A: Load from EXTERNAL S3 stage (@raw_ext_stage)
-- ============================================================

-- CSV: orders
COPY INTO ORDERS_RAW
  FROM @raw_ext_stage/orders.csv
  FILE_FORMAT = ff_csv_orders;

-- JSON: events (flatten array, match by column name)
COPY INTO EVENTS_RAW
  FROM @raw_ext_stage/events.json
  FILE_FORMAT = ff_json_events
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Parquet: inventory (after generating and uploading inventory.parquet)
COPY INTO INVENTORY_RAW
  FROM @raw_ext_stage/inventory.parquet
  FILE_FORMAT = ff_parquet_inventory
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================
-- Option B: Load from INTERNAL stage (@raw_stage)
-- Use if skipping S3 setup; upload via PUT first:
--   PUT file:///path/to/data-samples/orders.csv @raw_stage AUTO_COMPRESS=FALSE;
--   PUT file:///path/to/data-samples/events.json @raw_stage AUTO_COMPRESS=FALSE;
--   PUT file:///path/to/data-samples/inventory.parquet @raw_stage AUTO_COMPRESS=FALSE;
-- ============================================================

-- COPY INTO ORDERS_RAW FROM @raw_stage/orders.csv FILE_FORMAT = ff_csv_orders;
-- COPY INTO EVENTS_RAW FROM @raw_stage/events.json FILE_FORMAT = ff_json_events MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
-- COPY INTO INVENTORY_RAW FROM @raw_stage/inventory.parquet FILE_FORMAT = ff_parquet_inventory MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================
-- Verify streams detected the inserts
-- ============================================================
SELECT 'ORDERS_RAW_STREAM' AS STREAM, SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.ORDERS_RAW_STREAM') AS HAS_DATA
UNION ALL
SELECT 'EVENTS_RAW_STREAM', SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.EVENTS_RAW_STREAM')
UNION ALL
SELECT 'INVENTORY_RAW_STREAM', SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.INVENTORY_RAW_STREAM');

-- ============================================================
-- (Optional) Manually trigger the task instead of waiting 1 minute
-- ============================================================
-- EXECUTE TASK DEMO_LAB_DB.STAGE.task_transform_stage;


