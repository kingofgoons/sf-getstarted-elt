-- Load sample data into RAW tables (batch COPY INTO)
-- Financial Services theme: trades, market events, positions
-- Run after uploading files to S3: aws s3 cp data-samples/generated/<file> s3://demo-lab-landing/raw/
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA RAW;
USE WAREHOUSE LAB_INGEST_WH;

-- ============================================================
-- Option A: Load from EXTERNAL S3 stage (@raw_ext_stage)
-- Use pattern matching for timestamped files from generator
-- ============================================================

-- CSV: trades
COPY INTO TRADES_RAW
  FROM @raw_ext_stage
  FILE_FORMAT = ff_csv_trades
  PATTERN = '.*trades.*[.]csv';

-- JSON: market events (match by column name)
COPY INTO MARKET_EVENTS_RAW
  FROM @raw_ext_stage
  FILE_FORMAT = ff_json_events
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  PATTERN = '.*events.*[.]json';

-- Parquet: positions (match by column name)
COPY INTO POSITIONS_RAW
  FROM @raw_ext_stage
  FILE_FORMAT = ff_parquet_positions
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  PATTERN = '.*positions.*[.]parquet';

-- ============================================================
-- Option B: Load from INTERNAL stage (@raw_stage)
-- Use if skipping S3 setup; upload via PUT first:
--   PUT file:///path/to/data-samples/generated/trades_*.csv @raw_stage AUTO_COMPRESS=FALSE;
--   PUT file:///path/to/data-samples/generated/events_*.json @raw_stage AUTO_COMPRESS=FALSE;
--   PUT file:///path/to/data-samples/generated/positions_*.parquet @raw_stage AUTO_COMPRESS=FALSE;
-- ============================================================

-- COPY INTO TRADES_RAW FROM @raw_stage FILE_FORMAT = ff_csv_trades PATTERN = '.*trades.*[.]csv';
-- COPY INTO MARKET_EVENTS_RAW FROM @raw_stage FILE_FORMAT = ff_json_events MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PATTERN = '.*events.*[.]json';
-- COPY INTO POSITIONS_RAW FROM @raw_stage FILE_FORMAT = ff_parquet_positions MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PATTERN = '.*positions.*[.]parquet';

-- ============================================================
-- Verify streams detected the inserts
-- ============================================================
SELECT 'TRADES_RAW_STREAM' AS STREAM, SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.TRADES_RAW_STREAM') AS HAS_DATA
UNION ALL
SELECT 'MARKET_EVENTS_RAW_STREAM', SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.MARKET_EVENTS_RAW_STREAM')
UNION ALL
SELECT 'POSITIONS_RAW_STREAM', SYSTEM$STREAM_HAS_DATA('DEMO_LAB_DB.RAW.POSITIONS_RAW_STREAM');

-- ============================================================
-- (Optional) Manually trigger the task instead of waiting 1 minute
-- ============================================================
-- EXECUTE TASK DEMO_LAB_DB.STAGE.task_enrich_trades;
