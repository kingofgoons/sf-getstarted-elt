-- Dynamic Tables for declarative incremental transformations
-- Alternative to Streams + Tasks for simpler pipeline management
-- Financial Services theme: trades, market events, positions
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE WAREHOUSE LAB_TRANSFORM_WH;

-- ============================================================
-- Dynamic Tables vs Streams + Tasks
-- ============================================================
-- Streams + Tasks:
--   - Fine-grained control over execution timing and logic.
--   - Can call stored procedures, complex branching.
--   - Requires manual stream consumption and task DAG management.
--
-- Dynamic Tables:
--   - Declarative: define the transformation SQL once.
--   - Snowflake handles incremental refresh automatically.
--   - Simpler to manage; no explicit stream/task wiring.
--   - TARGET_LAG controls freshness (e.g., '1 minute', '1 hour').
--   - Automatic dependency tracking for chained dynamic tables.
--
-- Trade-offs:
--   - Less control over exact execution timing.
--   - Refresh cost depends on frequency and data volume.
--   - Best for straightforward transformations; complex logic may still need tasks.

-- ============================================================
-- Dynamic Table: TRADES_ENRICHED_DT (STAGE layer)
-- Joins trades with aggregated position data by SYMBOL
-- ============================================================
USE SCHEMA DEMO_LAB_DB.STAGE;

CREATE OR REPLACE DYNAMIC TABLE TRADES_ENRICHED_DT
  TARGET_LAG = '1 minute'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  WITH positions_agg AS (
    SELECT
      SYMBOL,
      AVG(CURRENT_PRICE) AS AVG_MARKET_PRICE,
      SUM(QUANTITY) AS TOTAL_POSITION_QTY,
      SUM(MARKET_VALUE) AS TOTAL_MARKET_VALUE
    FROM DEMO_LAB_DB.RAW.POSITIONS_RAW
    GROUP BY SYMBOL
  )
  SELECT
    t.TRADE_ID,
    t.ACCOUNT_ID,
    t.SYMBOL,
    t.TRADE_TS,
    t.SIDE,
    t.QUANTITY,
    t.PRICE,
    t.AMOUNT,
    t.EXCHANGE,
    t.STATUS,
    p.AVG_MARKET_PRICE,
    p.TOTAL_POSITION_QTY,
    p.TOTAL_MARKET_VALUE
  FROM DEMO_LAB_DB.RAW.TRADES_RAW t
  LEFT JOIN positions_agg p ON t.SYMBOL = p.SYMBOL;

-- ============================================================
-- Dynamic Table: TRADE_METRICS_DT (CURATED layer)
-- Aggregated trading metrics by hour and symbol
-- ============================================================
USE SCHEMA DEMO_LAB_DB.CURATED;

CREATE OR REPLACE DYNAMIC TABLE TRADE_METRICS_DT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    DATE_TRUNC('hour', TRADE_TS) AS TRADE_HOUR,
    SYMBOL,
    COUNT(*) AS TRADE_COUNT,
    SUM(QUANTITY) AS TOTAL_VOLUME,
    SUM(AMOUNT) AS TOTAL_NOTIONAL,
    AVG(PRICE) AS AVG_PRICE,
    SUM(CASE WHEN SIDE = 'BUY' THEN 1 ELSE 0 END) AS BUY_COUNT,
    SUM(CASE WHEN SIDE = 'SELL' THEN 1 ELSE 0 END) AS SELL_COUNT
  FROM DEMO_LAB_DB.STAGE.TRADES_ENRICHED_DT
  GROUP BY DATE_TRUNC('hour', TRADE_TS), SYMBOL;

-- ============================================================
-- Dynamic Table: MARKET_EVENT_SUMMARY_DT (CURATED layer)
-- Aggregated market events by hour, symbol, and type
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE MARKET_EVENT_SUMMARY_DT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    DATE_TRUNC('hour', EVENT_TS) AS EVENT_HOUR,
    SYMBOL,
    EVENT_TYPE,
    COUNT(*) AS EVENT_COUNT
  FROM DEMO_LAB_DB.RAW.MARKET_EVENTS_RAW
  GROUP BY DATE_TRUNC('hour', EVENT_TS), SYMBOL, EVENT_TYPE;

-- ============================================================
-- Dynamic Table: POSITION_SUMMARY_DT (CURATED layer)
-- Aggregated position metrics by symbol
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE POSITION_SUMMARY_DT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    SYMBOL,
    COUNT(DISTINCT ACCOUNT_ID) AS ACCOUNT_COUNT,
    SUM(QUANTITY) AS TOTAL_SHARES,
    SUM(COST_BASIS) AS TOTAL_COST_BASIS,
    SUM(MARKET_VALUE) AS TOTAL_MARKET_VALUE,
    SUM(UNREALIZED_PNL) AS TOTAL_UNREALIZED_PNL,
    AVG(CURRENT_PRICE) AS AVG_CURRENT_PRICE
  FROM DEMO_LAB_DB.RAW.POSITIONS_RAW
  GROUP BY SYMBOL;

-- ============================================================
-- Monitor Dynamic Tables
-- ============================================================
-- View refresh history:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME IN ('TRADES_ENRICHED_DT', 'TRADE_METRICS_DT', 'MARKET_EVENT_SUMMARY_DT', 'POSITION_SUMMARY_DT')
ORDER BY REFRESH_END_TIME DESC
LIMIT 50;

-- Check current lag and status:
SHOW DYNAMIC TABLES IN SCHEMA DEMO_LAB_DB.STAGE;
SHOW DYNAMIC TABLES IN SCHEMA DEMO_LAB_DB.CURATED;

-- ============================================================
-- Manual refresh (useful for testing)
-- ============================================================
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.TRADES_ENRICHED_DT REFRESH;

-- ============================================================
-- Suspend/Resume (cost control)
-- ============================================================
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.TRADES_ENRICHED_DT SUSPEND;
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.TRADES_ENRICHED_DT RESUME;
