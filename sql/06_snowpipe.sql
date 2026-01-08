-- Snowpipe for continuous ingestion from S3
-- Run after 01_stages_formats.sql (requires external stage and storage integration)
-- Financial Services theme: trades, market events, positions
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
-- Create Snowpipe for trades (CSV)
-- Note: MATCH_BY_COLUMN_NAME requires PARSE_HEADER=TRUE for CSV.
-- Since our CSV column order matches the table, we omit it here.
-- ============================================================
CREATE OR REPLACE PIPE trades_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.TRADES_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage
  FILE_FORMAT = (FORMAT_NAME = 'ff_csv_trades')
  PATTERN = '.*trades.*[.]csv';

-- ============================================================
-- Create Snowpipe for market events (JSON)
-- ============================================================
CREATE OR REPLACE PIPE market_events_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.MARKET_EVENTS_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage
  FILE_FORMAT = (FORMAT_NAME = 'ff_json_events')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  PATTERN = '.*events.*[.]json';

-- ============================================================
-- Create Snowpipe for positions (Parquet)
-- ============================================================
CREATE OR REPLACE PIPE positions_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO DEMO_LAB_DB.RAW.POSITIONS_RAW
  FROM @DEMO_LAB_DB.RAW.raw_ext_stage
  FILE_FORMAT = (FORMAT_NAME = 'ff_parquet_positions')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  PATTERN = '.*positions.*[.]parquet';

-- ============================================================
-- Get SQS ARN for S3 Event Notifications
-- ============================================================
-- After creating pipes, retrieve the notification channel (SQS ARN):
SHOW PIPES;
-- Or for a specific pipe:
-- SELECT SYSTEM$PIPE_STATUS('trades_pipe');

-- The "notification_channel" column contains the SQS ARN.
-- Use this ARN to configure S3 bucket event notifications.

-- ============================================================
-- AWS S3 Event Notification Setup (summary)
-- ============================================================
-- 1) In AWS S3 Console, go to your bucket -> Properties -> Event notifications.
-- 2) Create event notification:
--      - Name: e.g., "snowpipe-trades"
--      - Prefix: "raw/" (all files land in raw/)
--      - Event types: "All object create events" (s3:ObjectCreated:*)
--      - Destination: SQS queue
--      - SQS ARN: paste the notification_channel from SHOW PIPES
--
-- Option: Create separate notifications per file type:
--      - 'lab-trades-notification': prefix 'raw/', suffix '.csv' -> trades_pipe
--      - 'lab-events-notification': prefix 'raw/', suffix '.json' -> market_events_pipe
--      - 'lab-positions-notification': prefix 'raw/', suffix '.parquet' -> positions_pipe
--
-- For detailed steps, see:
-- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

-- ============================================================
-- Generate & Upload Test Data for Snowpipe Demo
-- ============================================================
-- Use the data generator to create connected FinServ sample files:
--
--   cd data-samples
--   pip install pandas pyarrow
--   python generate_all.py --batch 1 --trades 20 --events 50 --positions 30
--
-- Files are created in the generated/ subdirectory by default:
--   - generated/trades_YYYYMMDD_HHMMSS_batch001.csv       (trade orders with symbols, accounts)
--   - generated/events_YYYYMMDD_HHMMSS_batch001.json      (market events linked to trades/symbols)
--   - generated/positions_YYYYMMDD_HHMMSS_batch001.parquet (holdings by account/symbol)
--
-- Upload to S3 to trigger Snowpipe AUTO_INGEST:
--
--   aws s3 cp generated/trades_*.csv s3://demo-lab-landing/raw/
--   aws s3 cp generated/events_*.json s3://demo-lab-landing/raw/
--   aws s3 cp generated/positions_*.parquet s3://demo-lab-landing/raw/
--
-- Generate additional batches to simulate continuous data flow:
--
--   python generate_all.py --batch 2
--   python generate_all.py --batch 3
--   # Upload each batch to see Snowpipe process them
--
-- Wait ~1 minute, then check the monitoring queries below.

-- ============================================================
-- Monitor Snowpipe
-- ============================================================
-- Check pipe status:
SELECT SYSTEM$PIPE_STATUS('trades_pipe');
SELECT SYSTEM$PIPE_STATUS('market_events_pipe');
SELECT SYSTEM$PIPE_STATUS('positions_pipe');

-- View recent copy history for a pipe:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'TRADES_RAW',
  --TABLE_NAME => 'MARKET_EVENTS_RAW',
  --TABLE_NAME => 'POSITIONS_RAW',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- View pipe usage (credits consumed):
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE PIPE_NAME IN ('TRADES_PIPE', 'MARKET_EVENTS_PIPE', 'POSITIONS_PIPE')
ORDER BY START_TIME DESC
LIMIT 50;

-- ============================================================
-- Manual refresh (useful for testing or backfill)
-- ============================================================
-- ALTER PIPE trades_pipe REFRESH;
-- ALTER PIPE market_events_pipe REFRESH;
-- ALTER PIPE positions_pipe REFRESH;
