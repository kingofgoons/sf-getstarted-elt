-- =============================================================================
-- 02b_transform_demo.sql - Building the Transformation Pipeline
-- =============================================================================
-- 
-- Now we'll create tables to hold transformed data and build the logic
-- to enrich raw trades with calculated fields.
--
-- DURATION: ~5 minutes
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;
USE WAREHOUSE TRADING_TRANSFORM_WH;

-- =============================================================================
-- STEP 1: Create destination tables for transformed data
-- =============================================================================

-- STAGE schema: Enriched data (Silver layer)
USE SCHEMA STAGE;

CREATE OR REPLACE TABLE TRADES_ENRICHED (
    TRADE_ID        STRING      NOT NULL,
    SYMBOL          STRING      NOT NULL,
    SIDE            STRING      NOT NULL,
    QUANTITY        NUMBER(18,4) NOT NULL,
    PRICE           NUMBER(18,6) NOT NULL,
    NOTIONAL_VALUE  NUMBER(18,2) NOT NULL    COMMENT 'quantity * price',
    EXECUTION_TS    TIMESTAMP_NTZ NOT NULL,
    EXECUTION_DATE  DATE        NOT NULL,
    ACCOUNT_ID      STRING      NOT NULL,
    VENUE           STRING,
    TRADER_ID       STRING,
    ORDER_ID        STRING,
    POSITION_QTY    NUMBER(18,4)             COMMENT 'Position qty before trade',
    AVG_COST        NUMBER(18,6)             COMMENT 'Avg cost before trade',
    REALIZED_PNL    NUMBER(18,2)             COMMENT 'Realized P&L if closing',
    IS_CLOSING      BOOLEAN     DEFAULT FALSE,
    _PROCESSED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CURATED schema: Business metrics (Gold layer)
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE TRADE_METRICS (
    SYMBOL          STRING      NOT NULL,
    METRIC_DATE     DATE        NOT NULL,
    ACCOUNT_ID      STRING      NOT NULL,
    BUY_QUANTITY    NUMBER(18,4) NOT NULL,
    SELL_QUANTITY   NUMBER(18,4) NOT NULL,
    TOTAL_NOTIONAL  NUMBER(18,2) NOT NULL,
    REALIZED_PNL    NUMBER(18,2) DEFAULT 0,
    TRADE_COUNT     NUMBER      NOT NULL,
    _UPDATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (METRIC_DATE, ACCOUNT_ID, SYMBOL)
);

CREATE OR REPLACE TABLE POSITION_SUMMARY (
    ACCOUNT_ID      STRING      NOT NULL,
    SYMBOL          STRING      NOT NULL,
    QUANTITY        NUMBER(18,4) NOT NULL,
    AVG_COST        NUMBER(18,6) NOT NULL,
    MARKET_VALUE    NUMBER(18,2) NOT NULL,
    UNREALIZED_PNL  NUMBER(18,2) NOT NULL,
    AS_OF_DATE      DATE        NOT NULL,
    SECTOR          STRING,
    ASSET_CLASS     STRING,
    _UPDATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ACCOUNT_ID, SYMBOL)
);

-- =============================================================================
-- STEP 2: Manually run the transformation to understand it
-- =============================================================================
-- Let's process the stream data and see what the transformation looks like.
-- First, check if we have data in the stream:

SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRADES_RAW_STREAM') AS has_data;

-- View what's in the stream (don't worry if empty - we'll add data)
SELECT * FROM RAW.TRADES_RAW_STREAM;

-- =============================================================================
-- STEP 3: Insert test data if stream is empty
-- =============================================================================

INSERT INTO RAW.TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('TXN-001', 'MSFT', 'BUY', 200, 415.00, CURRENT_TIMESTAMP(), 'ACCT-001', 'NASDAQ'),
    ('TXN-002', 'MSFT', 'SELL', 100, 418.00, CURRENT_TIMESTAMP(), 'ACCT-001', 'NASDAQ'),
    ('TXN-003', 'GOOGL', 'BUY', 50, 175.00, CURRENT_TIMESTAMP(), 'ACCT-002', 'NASDAQ');

-- Verify stream captured them
SELECT TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, ACCOUNT_ID 
FROM RAW.TRADES_RAW_STREAM;

-- =============================================================================
-- STEP 4: Transform and load into TRADES_ENRICHED
-- =============================================================================
-- This INSERT consumes the stream (moves data from stream to target).

INSERT INTO STAGE.TRADES_ENRICHED (
    TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, NOTIONAL_VALUE,
    EXECUTION_TS, EXECUTION_DATE, ACCOUNT_ID, VENUE, TRADER_ID, ORDER_ID,
    POSITION_QTY, AVG_COST, REALIZED_PNL, IS_CLOSING
)
SELECT 
    t.TRADE_ID,
    t.SYMBOL,
    t.SIDE,
    t.QUANTITY,
    t.PRICE,
    t.QUANTITY * t.PRICE AS NOTIONAL_VALUE,  -- Calculate notional
    t.EXECUTION_TS,
    DATE(t.EXECUTION_TS) AS EXECUTION_DATE,
    t.ACCOUNT_ID,
    t.VENUE,
    t.TRADER_ID,
    t.ORDER_ID,
    p.QUANTITY AS POSITION_QTY,
    p.AVG_COST,
    -- Calculate realized P&L for closing trades
    CASE 
        WHEN t.SIDE = 'SELL' AND p.QUANTITY > 0 THEN 
            (t.PRICE - p.AVG_COST) * LEAST(t.QUANTITY, p.QUANTITY)
        WHEN t.SIDE = 'BUY' AND p.QUANTITY < 0 THEN 
            (p.AVG_COST - t.PRICE) * LEAST(t.QUANTITY, ABS(p.QUANTITY))
        ELSE 0
    END AS REALIZED_PNL,
    -- Flag closing trades
    CASE 
        WHEN (t.SIDE = 'SELL' AND p.QUANTITY > 0) OR 
             (t.SIDE = 'BUY' AND p.QUANTITY < 0) THEN TRUE
        ELSE FALSE
    END AS IS_CLOSING
FROM RAW.TRADES_RAW_STREAM t
LEFT JOIN RAW.POSITIONS_RAW p 
    ON t.ACCOUNT_ID = p.ACCOUNT_ID AND t.SYMBOL = p.SYMBOL;

-- Verify data was transformed
SELECT * FROM STAGE.TRADES_ENRICHED ORDER BY _PROCESSED_AT DESC LIMIT 10;

-- Stream should be empty now (data was consumed)
SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRADES_RAW_STREAM') AS has_data;
-- Should return: FALSE

-- =============================================================================
-- STEP 5: Aggregate into TRADE_METRICS (Gold layer)
-- =============================================================================

MERGE INTO CURATED.TRADE_METRICS tgt
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
    FROM STAGE.TRADES_ENRICHED
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

-- View the aggregated metrics
SELECT * FROM CURATED.TRADE_METRICS ORDER BY METRIC_DATE DESC;

-- =============================================================================
-- SUMMARY: What we built
-- =============================================================================
-- 
--   RAW.TRADES_RAW  →  Stream  →  STAGE.TRADES_ENRICHED  →  CURATED.TRADE_METRICS
--   (raw data)         (CDC)      (+ notional, + P&L)       (daily aggregates)
--
-- We manually ran the transformation. Next, we'll automate it with TASKS!
--
-- NEXT: Run 02c_tasks_demo.sql to automate this pipeline
-- =============================================================================

