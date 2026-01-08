-- Dynamic Tables for declarative incremental transformations
-- Alternative to Streams + Tasks for simpler pipeline management
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
-- Dynamic Table: ORDERS_ENRICHED_DT (STAGE layer)
-- ============================================================
USE SCHEMA DEMO_LAB_DB.STAGE;

CREATE OR REPLACE DYNAMIC TABLE ORDERS_ENRICHED_DT
  TARGET_LAG = '1 minute'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_TS,
    o.AMOUNT,
    o.STATUS,
    i.WAREHOUSE AS INVENTORY_WAREHOUSE,
    i.QTY AS INVENTORY_QTY,
    i.UPDATED_AT AS INVENTORY_UPDATED_AT
  FROM DEMO_LAB_DB.RAW.ORDERS_RAW o
  LEFT JOIN DEMO_LAB_DB.RAW.INVENTORY_RAW i
    ON o.ORDER_ID = i.SKU;

-- ============================================================
-- Dynamic Table: ORDER_METRICS_DT (CURATED layer)
-- Chains off ORDERS_ENRICHED_DT; Snowflake tracks dependency.
-- ============================================================
USE SCHEMA DEMO_LAB_DB.CURATED;

CREATE OR REPLACE DYNAMIC TABLE ORDER_METRICS_DT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    DATE_TRUNC('hour', ORDER_TS) AS ORDER_HOUR,
    COUNT(*) AS ORDER_COUNT,
    SUM(AMOUNT) AS TOTAL_AMOUNT,
    AVG(AMOUNT) AS AVG_AMOUNT,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS
  FROM DEMO_LAB_DB.STAGE.ORDERS_ENRICHED_DT
  GROUP BY DATE_TRUNC('hour', ORDER_TS);

-- ============================================================
-- Dynamic Table: EVENT_FACTS_DT (CURATED layer)
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE EVENT_FACTS_DT
  TARGET_LAG = '5 minutes'
  WAREHOUSE = LAB_TRANSFORM_WH
AS
  SELECT
    DATE_TRUNC('hour', EVENT_TS) AS EVENT_HOUR,
    EVENT_TYPE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
  FROM DEMO_LAB_DB.RAW.EVENTS_RAW
  GROUP BY DATE_TRUNC('hour', EVENT_TS), EVENT_TYPE;

-- ============================================================
-- Monitor Dynamic Tables
-- ============================================================
-- View refresh history:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME IN ('ORDERS_ENRICHED_DT', 'ORDER_METRICS_DT', 'EVENT_FACTS_DT')
ORDER BY REFRESH_END_TIME DESC
LIMIT 50;

-- Check current lag and status:
SHOW DYNAMIC TABLES IN SCHEMA DEMO_LAB_DB.STAGE;
SHOW DYNAMIC TABLES IN SCHEMA DEMO_LAB_DB.CURATED;

-- ============================================================
-- Manual refresh (useful for testing)
-- ============================================================
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.ORDERS_ENRICHED_DT REFRESH;

-- ============================================================
-- Suspend/Resume (cost control)
-- ============================================================
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.ORDERS_ENRICHED_DT SUSPEND;
-- ALTER DYNAMIC TABLE DEMO_LAB_DB.STAGE.ORDERS_ENRICHED_DT RESUME;


