-- =============================================================================
-- 02c_tasks_demo.sql - Automating with Tasks
-- =============================================================================
-- 
-- Now we'll automate the transformation pipeline using TASKS.
-- Tasks run on a schedule and can be chained together.
--
-- DURATION: ~5 minutes
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;
USE WAREHOUSE TRADING_TRANSFORM_WH;
USE SCHEMA STAGE;

-- =============================================================================
-- STEP 1: Create the Transform Task
-- =============================================================================
-- This task runs every minute, but ONLY if the stream has data.
-- The WHEN clause prevents unnecessary compute costs.

CREATE OR REPLACE TASK TASK_TRANSFORM_TRADES
    WAREHOUSE = TRADING_TRANSFORM_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TRADING_LAB_DB.RAW.TRADES_RAW_STREAM')
AS
    -- Same transformation we ran manually in 02b
    INSERT INTO TRADING_LAB_DB.STAGE.TRADES_ENRICHED (
        TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, NOTIONAL_VALUE,
        EXECUTION_TS, EXECUTION_DATE, ACCOUNT_ID, VENUE, TRADER_ID, ORDER_ID,
        POSITION_QTY, AVG_COST, REALIZED_PNL, IS_CLOSING
    )
    SELECT 
        t.TRADE_ID, t.SYMBOL, t.SIDE, t.QUANTITY, t.PRICE,
        t.QUANTITY * t.PRICE AS NOTIONAL_VALUE,
        t.EXECUTION_TS,
        DATE(t.EXECUTION_TS) AS EXECUTION_DATE,
        t.ACCOUNT_ID, t.VENUE, t.TRADER_ID, t.ORDER_ID,
        p.QUANTITY AS POSITION_QTY,
        p.AVG_COST,
        CASE 
            WHEN t.SIDE = 'SELL' AND p.QUANTITY > 0 THEN 
                (t.PRICE - p.AVG_COST) * LEAST(t.QUANTITY, p.QUANTITY)
            WHEN t.SIDE = 'BUY' AND p.QUANTITY < 0 THEN 
                (p.AVG_COST - t.PRICE) * LEAST(t.QUANTITY, ABS(p.QUANTITY))
            ELSE 0
        END AS REALIZED_PNL,
        CASE 
            WHEN (t.SIDE = 'SELL' AND p.QUANTITY > 0) OR 
                 (t.SIDE = 'BUY' AND p.QUANTITY < 0) THEN TRUE
            ELSE FALSE
        END AS IS_CLOSING
    FROM TRADING_LAB_DB.RAW.TRADES_RAW_STREAM t
    LEFT JOIN TRADING_LAB_DB.RAW.POSITIONS_RAW p 
        ON t.ACCOUNT_ID = p.ACCOUNT_ID AND t.SYMBOL = p.SYMBOL;

-- =============================================================================
-- STEP 2: Create the Aggregation Task (runs AFTER transform)
-- =============================================================================
-- Task chaining! This task automatically runs after TASK_TRANSFORM_TRADES.

CREATE OR REPLACE TASK TASK_AGGREGATE_METRICS
    WAREHOUSE = TRADING_TRANSFORM_WH
    AFTER TASK_TRANSFORM_TRADES
AS
    MERGE INTO TRADING_LAB_DB.CURATED.TRADE_METRICS tgt
    USING (
        SELECT 
            SYMBOL,
            EXECUTION_DATE AS METRIC_DATE,
            ACCOUNT_ID,
            SUM(CASE WHEN SIDE = 'BUY' THEN QUANTITY ELSE 0 END) AS BUY_QUANTITY,
            SUM(CASE WHEN SIDE = 'SELL' THEN QUANTITY ELSE 0 END) AS SELL_QUANTITY,
            SUM(NOTIONAL_VALUE) AS TOTAL_NOTIONAL,
            SUM(COALESCE(REALIZED_PNL, 0)) AS REALIZED_PNL,
            COUNT(*) AS TRADE_COUNT
        FROM TRADING_LAB_DB.STAGE.TRADES_ENRICHED
        GROUP BY SYMBOL, EXECUTION_DATE, ACCOUNT_ID
    ) src
    ON tgt.METRIC_DATE = src.METRIC_DATE 
       AND tgt.ACCOUNT_ID = src.ACCOUNT_ID 
       AND tgt.SYMBOL = src.SYMBOL
    WHEN MATCHED THEN UPDATE SET
        BUY_QUANTITY = src.BUY_QUANTITY,
        SELL_QUANTITY = src.SELL_QUANTITY,
        TOTAL_NOTIONAL = src.TOTAL_NOTIONAL,
        REALIZED_PNL = src.REALIZED_PNL,
        TRADE_COUNT = src.TRADE_COUNT,
        _UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        SYMBOL, METRIC_DATE, ACCOUNT_ID, 
        BUY_QUANTITY, SELL_QUANTITY, TOTAL_NOTIONAL, REALIZED_PNL, TRADE_COUNT
    ) VALUES (
        src.SYMBOL, src.METRIC_DATE, src.ACCOUNT_ID,
        src.BUY_QUANTITY, src.SELL_QUANTITY, src.TOTAL_NOTIONAL, src.REALIZED_PNL, src.TRADE_COUNT
    );

-- =============================================================================
-- STEP 3: Check Task Status (before enabling)
-- =============================================================================

SHOW TASKS IN SCHEMA TRADING_LAB_DB.STAGE;
-- Note: STATE = 'suspended' - tasks are disabled by default

-- =============================================================================
-- STEP 4: Enable Tasks
-- =============================================================================
-- IMPORTANT: Enable child tasks BEFORE parent tasks!

-- Enable the child (downstream) task first
ALTER TASK TASK_AGGREGATE_METRICS RESUME;

-- Then enable the root (upstream) task
ALTER TASK TASK_TRANSFORM_TRADES RESUME;

-- Verify they're running
SHOW TASKS IN SCHEMA TRADING_LAB_DB.STAGE;
-- STATE should now be 'started'

-- =============================================================================
-- STEP 5: Test the Automated Pipeline!
-- =============================================================================
-- Insert a new trade and watch it flow through automatically.

-- First, check current row counts
SELECT 'TRADES_ENRICHED' AS tbl, COUNT(*) AS cnt FROM STAGE.TRADES_ENRICHED
UNION ALL
SELECT 'TRADE_METRICS', COUNT(*) FROM CURATED.TRADE_METRICS;

-- Insert a new trade
INSERT INTO RAW.TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('AUTO-001', 'AMZN', 'BUY', 75, 185.00, CURRENT_TIMESTAMP(), 'ACCT-AUTO', 'NASDAQ');

-- Verify stream has data
SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRADES_RAW_STREAM') AS stream_has_data;
-- Should return: TRUE

-- =============================================================================
-- STEP 6: Wait for Task or Execute Manually
-- =============================================================================
-- Option A: Wait ~1 minute for the scheduled task to run automatically
-- Option B: Execute the task immediately for testing:

EXECUTE TASK TASK_TRANSFORM_TRADES;

-- Wait a few seconds, then check if data flowed through
SELECT * FROM STAGE.TRADES_ENRICHED WHERE TRADE_ID = 'AUTO-001';
SELECT * FROM CURATED.TRADE_METRICS WHERE ACCOUNT_ID = 'ACCT-AUTO';

-- =============================================================================
-- STEP 7: Monitor Task Execution
-- =============================================================================

-- View recent task runs
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE DATABASE_NAME = 'TRADING_LAB_DB'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- =============================================================================
-- STEP 8: Add Position Updates Task (Optional)
-- =============================================================================
-- This task processes position updates on a separate schedule.

-- First create the stream on positions
CREATE OR REPLACE STREAM TRADING_LAB_DB.RAW.POSITIONS_RAW_STREAM 
    ON TABLE TRADING_LAB_DB.RAW.POSITIONS_RAW
    APPEND_ONLY = TRUE;

-- Create the task
CREATE OR REPLACE TASK TASK_UPDATE_POSITIONS
    WAREHOUSE = TRADING_TRANSFORM_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TRADING_LAB_DB.RAW.POSITIONS_RAW_STREAM')
AS
    MERGE INTO TRADING_LAB_DB.CURATED.POSITION_SUMMARY tgt
    USING (
        SELECT 
            ACCOUNT_ID, SYMBOL, QUANTITY, AVG_COST, MARKET_VALUE,
            MARKET_VALUE - (QUANTITY * AVG_COST) AS UNREALIZED_PNL,
            AS_OF_DATE, SECTOR, ASSET_CLASS
        FROM TRADING_LAB_DB.RAW.POSITIONS_RAW_STREAM
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ACCOUNT_ID, SYMBOL 
            ORDER BY AS_OF_DATE DESC
        ) = 1
    ) src
    ON tgt.ACCOUNT_ID = src.ACCOUNT_ID AND tgt.SYMBOL = src.SYMBOL
    WHEN MATCHED THEN UPDATE SET
        QUANTITY = src.QUANTITY,
        AVG_COST = src.AVG_COST,
        MARKET_VALUE = src.MARKET_VALUE,
        UNREALIZED_PNL = src.UNREALIZED_PNL,
        AS_OF_DATE = src.AS_OF_DATE,
        SECTOR = src.SECTOR,
        ASSET_CLASS = src.ASSET_CLASS,
        _UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT VALUES (
        src.ACCOUNT_ID, src.SYMBOL, src.QUANTITY, src.AVG_COST, src.MARKET_VALUE,
        src.UNREALIZED_PNL, src.AS_OF_DATE, src.SECTOR, src.ASSET_CLASS, CURRENT_TIMESTAMP()
    );

ALTER TASK TASK_UPDATE_POSITIONS RESUME;

-- =============================================================================
-- SUMMARY: Our Automated Pipeline
-- =============================================================================
--
--   ┌──────────────┐
--   │ TRADES_RAW   │
--   │ (new rows)   │
--   └──────┬───────┘
--          │ Stream detects changes
--          ▼
--   ┌──────────────────────────┐
--   │ TASK_TRANSFORM_TRADES    │ ← Runs every 1 min IF stream has data
--   │ (enriches trades)        │
--   └──────────┬───────────────┘
--              │ AFTER (task chain)
--              ▼
--   ┌──────────────────────────┐
--   │ TASK_AGGREGATE_METRICS   │ ← Runs automatically after transform
--   │ (updates TRADE_METRICS)  │
--   └──────────────────────────┘
--
-- Key Points:
-- 1. WHEN clause prevents tasks from running when there's no work
-- 2. AFTER creates a dependency chain between tasks
-- 3. This is serverless - no infrastructure to manage!
--
-- NEXT: Run 03_dbt_setup.sql to add the analytics layer
-- =============================================================================

-- =============================================================================
-- CLEANUP (if needed to restart)
-- =============================================================================
/*
-- Suspend tasks before dropping
ALTER TASK TRADING_LAB_DB.STAGE.TASK_AGGREGATE_METRICS SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES SUSPEND;
ALTER TASK TRADING_LAB_DB.STAGE.TASK_UPDATE_POSITIONS SUSPEND;

-- Drop tasks
DROP TASK IF EXISTS TRADING_LAB_DB.STAGE.TASK_AGGREGATE_METRICS;
DROP TASK IF EXISTS TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES;
DROP TASK IF EXISTS TRADING_LAB_DB.STAGE.TASK_UPDATE_POSITIONS;

-- Drop stream
DROP STREAM IF EXISTS TRADING_LAB_DB.RAW.TRADES_RAW_STREAM;
*/

