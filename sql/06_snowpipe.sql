-- Snowpipe for continuous ingestion from S3
-- Run after 01_stages_formats.sql (requires external stage and storage integration)
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA RAW;

-- ============================================================
-- Snowpipe vs COPY INTO: When to Use Each
-- ============================================================
-- COPY INTO (batch):
--   - Best for: scheduled bulk loads, backfills, infrequent file arrivals.
--   - Cost: warehouse compute only when COPY runs.
--   - Latency: depends on schedule (minutes to hours).
--
-- Snowpipe (continuous):
--   - Best for: near-real-time ingestion, frequent small files, event-driven.
--   - Cost: serverless (~0.06 credits per 1000 files); no warehouse needed.
--   - Latency: typically < 1 minute after file lands.
--
-- Guidance:
--   - High file volume with small files? Snowpipe cost can add up; consider batching.
--   - Predictable large batches? COPY INTO is cheaper.
--   - Continuous trickle + low latency? Snowpipe wins.

-- ============================================================
-- Create Snowpipe for orders (CSV)
-- Note: MATCH_BY_COLUMN_NAME requires PARSE_HEADER=TRUE for CSV.
-- Since our CSV column order matches the table, we omit it here.
-- ============================================================
CREATE OR REPLACE PIPE orders_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.ORDERS_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage/orders/
  FILE_FORMAT = (FORMAT_NAME = 'ff_csv_orders');

-- ============================================================
-- Create Snowpipe for events (JSON)
-- ============================================================
CREATE OR REPLACE PIPE events_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.EVENTS_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage/events/
  FILE_FORMAT = (FORMAT_NAME = 'ff_json_events')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================
-- Create Snowpipe for inventory (Parquet)
-- ============================================================
CREATE OR REPLACE PIPE inventory_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.INVENTORY_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage/inventory/
  FILE_FORMAT = (FORMAT_NAME = 'ff_parquet_inventory')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================
-- Get SQS ARN for S3 Event Notifications
-- ============================================================
-- After creating pipes, retrieve the notification channel (SQS ARN):
SHOW PIPES;
-- Or for a specific pipe:
-- SELECT SYSTEM$PIPE_STATUS('orders_pipe');

-- The "notification_channel" column contains the SQS ARN.
-- Use this ARN to configure S3 bucket event notifications.

-- ============================================================
-- AWS S3 Event Notification Setup (summary)
-- ============================================================
-- 1) In AWS S3 Console, go to your bucket -> Properties -> Event notifications.
-- 2) Create event notification:
--      - Name: e.g., "snowpipe-orders"
--      - Prefix: "raw/orders/" (match the pipe's stage path)
--      - Event types: "All object create events" (s3:ObjectCreated:*)
--      - Destination: SQS queue
--      - SQS ARN: paste the notification_channel from SHOW PIPES
-- 3) Repeat for events/ and inventory/ prefixes if using separate pipes.
--
-- For detailed steps, see:
-- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

--      - In the bucket, Properties -> Create event notifications (make 3):
--          - 'lab-events-notification', with prefix 'raw/' and with suffix '.json' for the events_pipe
--          - 'lab-inventory-notification', with prefix 'raw/' and  with suffix '.parquet' for the inventory_pipe
--          - 'lab-orders-notification' with prefix 'raw/', and with suffix '.csv' for the orders_pipe.

-- ============================================================
-- Monitor Snowpipe
-- ============================================================
-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('orders_pipe');

-- View recent copy history for a pipe:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'ORDERS_RAW',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- View pipe usage (credits consumed):
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE PIPE_NAME IN ('ORDERS_PIPE', 'EVENTS_PIPE', 'INVENTORY_PIPE')
ORDER BY START_TIME DESC
LIMIT 50;

-- ============================================================
-- Manual refresh (useful for testing or backfill)
-- ============================================================
-- ALTER PIPE orders_pipe REFRESH;


