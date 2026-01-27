-- =============================================================================
-- 02a_streams_demo.sql - Understanding Streams (CDC)
-- =============================================================================
-- 
-- This script demonstrates how Snowflake STREAMS work for Change Data Capture.
-- We'll create a stream, insert data, and see how the stream tracks changes.
--
-- DURATION: ~5 minutes
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;
USE WAREHOUSE TRADING_TRANSFORM_WH;
USE SCHEMA RAW;

-- =============================================================================
-- STEP 1: Check how many rows we have in TRADES_RAW
-- =============================================================================

SELECT COUNT(*) AS total_trades FROM TRADES_RAW;
-- You should see ~55 rows from the initial data load

-- =============================================================================
-- STEP 2: Create a STREAM on TRADES_RAW
-- =============================================================================
-- A stream tracks changes to a table. Think of it as a "diff" or "changelog".
-- APPEND_ONLY = TRUE means we only care about INSERTs (not updates/deletes).

CREATE OR REPLACE STREAM TRADES_RAW_STREAM 
    ON TABLE TRADES_RAW
    APPEND_ONLY = TRUE
    COMMENT = 'Tracks new trades for CDC processing';

-- Verify it was created
SHOW STREAMS LIKE 'TRADES_RAW%';

-- =============================================================================
-- STEP 3: Check if the stream has any data
-- =============================================================================
-- Right after creation, the stream is "empty" - it starts tracking from NOW.

SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM') AS has_new_data;
-- Should return: FALSE

-- You can also query the stream directly (it returns 0 rows initially)
SELECT COUNT(*) AS rows_in_stream FROM TRADES_RAW_STREAM;

-- =============================================================================
-- STEP 4: Insert a new trade and watch the stream capture it!
-- =============================================================================

INSERT INTO TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('DEMO-001', 'NVDA', 'BUY', 50, 875.00, CURRENT_TIMESTAMP(), 'ACCT-DEMO', 'NASDAQ');

-- Now check the stream again
SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM') AS has_new_data;
-- Should return: TRUE

-- Query the stream to see the new record
SELECT 
    TRADE_ID,
    SYMBOL,
    SIDE,
    QUANTITY,
    PRICE,
    ACCOUNT_ID,
    -- These are stream metadata columns:
    METADATA$ACTION,      -- 'INSERT' for new rows
    METADATA$ISUPDATE,    -- FALSE for inserts
    METADATA$ROW_ID       -- Unique row identifier
FROM TRADES_RAW_STREAM;

-- =============================================================================
-- STEP 5: Key Insight - Streams are "consumed" when you read them in DML
-- =============================================================================
-- When you use a stream in a DML statement (INSERT, MERGE, etc.),
-- the records are "consumed" and removed from the stream.
--
-- Let's simulate this by creating a simple target table and inserting:

CREATE OR REPLACE TEMPORARY TABLE TEMP_PROCESSED_TRADES AS
SELECT TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, ACCOUNT_ID
FROM TRADES_RAW_STREAM;

-- Check the stream again - it should be empty now!
SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM') AS has_new_data;
-- Should return: FALSE

SELECT COUNT(*) AS rows_in_stream FROM TRADES_RAW_STREAM;
-- Should return: 0

-- But our temp table has the data
SELECT * FROM TEMP_PROCESSED_TRADES;

-- =============================================================================
-- STEP 6: Insert another trade to verify the stream keeps working
-- =============================================================================

INSERT INTO TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('DEMO-002', 'AAPL', 'SELL', 100, 189.50, CURRENT_TIMESTAMP(), 'ACCT-DEMO', 'NYSE');

-- Stream should have data again
SELECT SYSTEM$STREAM_HAS_DATA('TRADES_RAW_STREAM') AS has_new_data;
-- Should return: TRUE

SELECT * FROM TRADES_RAW_STREAM;

-- =============================================================================
-- SUMMARY: What we learned about Streams
-- =============================================================================
-- 1. Streams track changes to tables automatically
-- 2. SYSTEM$STREAM_HAS_DATA() tells you if there are pending changes
-- 3. Reading a stream in DML consumes the records
-- 4. This enables efficient, incremental processing (no need to scan full table)
--
-- NEXT: Run 02b_transform_demo.sql to see how we transform this data
-- =============================================================================

