-- =============================================================================
-- 05_advanced_optional.sql - Advanced Topics (Optional)
-- =============================================================================
-- This script contains COMMENTED examples of advanced Snowflake features.
-- Uncomment and customize sections as needed for your use case.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;

-- =============================================================================
-- TIME TRAVEL - Query and Restore Historical Data
-- =============================================================================
-- Snowflake automatically retains historical data for a configurable period.
-- See: https://docs.snowflake.com/en/user-guide/data-time-travel

/*
-- Query data as it was 5 minutes ago
SELECT * FROM RAW.TRADES_RAW AT(OFFSET => -60*5);

-- Query data as it was at a specific timestamp
SELECT * FROM RAW.TRADES_RAW AT(TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP);

-- Query data before a specific query was run (use QUERY_ID from query history)
SELECT * FROM RAW.TRADES_RAW BEFORE(STATEMENT => '<query-id>');

-- Clone a table as it was at a point in time
CREATE TABLE RAW.TRADES_RAW_BACKUP CLONE RAW.TRADES_RAW
    AT(TIMESTAMP => DATEADD('hour', -1, CURRENT_TIMESTAMP()));

-- Restore a dropped table (within retention period)
DROP TABLE RAW.TRADES_RAW;
UNDROP TABLE RAW.TRADES_RAW;

-- Set data retention period (default is 1 day, max 90 days for Enterprise+)
ALTER TABLE RAW.TRADES_RAW SET DATA_RETENTION_TIME_IN_DAYS = 7;
*/

-- =============================================================================
-- QUERY OPTIMIZATION - Performance Tuning
-- =============================================================================
-- See: https://docs.snowflake.com/en/user-guide/performance-query

/*
-- Check if result cache was used (QUERY_RESULT_REUSE_*  columns)
SELECT
    QUERY_ID,
    QUERY_TEXT,
    TOTAL_ELAPSED_TIME,
    BYTES_SCANNED,
    PERCENTAGE_SCANNED_FROM_CACHE,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT LIKE '%TRADES_RAW%'
ORDER BY START_TIME DESC
LIMIT 10;

-- Add clustering key to frequently filtered columns
-- (useful for large tables with range queries)
ALTER TABLE RAW.TRADES_RAW CLUSTER BY (EXECUTION_TS);

-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('RAW.TRADES_RAW');

-- Manually trigger reclustering (usually automatic)
-- ALTER TABLE RAW.TRADES_RAW RECLUSTER;

-- Search optimization (for point lookups on large tables)
ALTER TABLE RAW.TRADES_RAW ADD SEARCH OPTIMIZATION ON EQUALITY(TRADE_ID, SYMBOL);

-- Check search optimization status
SHOW TABLES LIKE 'TRADES_RAW';
-- Look at SEARCH_OPTIMIZATION column
*/

-- =============================================================================
-- WAREHOUSE SIZING - Right-size for Workloads
-- =============================================================================
-- See: https://docs.snowflake.com/en/user-guide/warehouses-considerations

/*
-- Check query queue time (indicates warehouse is undersized)
SELECT
    WAREHOUSE_NAME,
    AVG(QUEUED_OVERLOAD_TIME) / 1000 AS AVG_QUEUE_SEC,
    MAX(QUEUED_OVERLOAD_TIME) / 1000 AS MAX_QUEUE_SEC,
    COUNT(*) AS QUERY_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME LIKE 'TRADING%'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1;

-- Scale up warehouse (more compute per query)
ALTER WAREHOUSE TRADING_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- Scale out warehouse (multi-cluster for concurrency)
ALTER WAREHOUSE TRADING_ANALYTICS_WH SET
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD';

-- Auto-suspend settings
ALTER WAREHOUSE TRADING_ANALYTICS_WH SET AUTO_SUSPEND = 60;  -- 60 seconds
ALTER WAREHOUSE TRADING_ANALYTICS_WH SET AUTO_RESUME = TRUE;
*/

-- =============================================================================
-- DATA GOVERNANCE - Row Access Policies & Masking
-- =============================================================================
-- See: https://docs.snowflake.com/en/user-guide/security-row-intro
-- See: https://docs.snowflake.com/en/user-guide/security-column-ddm-intro

/*
-- Row Access Policy: Restrict rows based on user's role
CREATE OR REPLACE ROW ACCESS POLICY ACCOUNT_ACCESS_POLICY AS (account_id STRING)
RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ACCOUNTADMIN', 'TRADING_LAB_ROLE')
    OR account_id IN (
        SELECT allowed_account_id 
        FROM GOVERNANCE.USER_ACCOUNT_MAPPING 
        WHERE user_name = CURRENT_USER()
    );

-- Apply to table
ALTER TABLE RAW.TRADES_RAW ADD ROW ACCESS POLICY ACCOUNT_ACCESS_POLICY ON (ACCOUNT_ID);

-- Dynamic Data Masking: Hide sensitive data from non-privileged users
CREATE OR REPLACE MASKING POLICY TRADER_ID_MASK AS (val STRING)
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'COMPLIANCE_ROLE') THEN val
        ELSE '***MASKED***'
    END;

-- Apply to column
ALTER TABLE RAW.TRADES_RAW MODIFY COLUMN TRADER_ID SET MASKING POLICY TRADER_ID_MASK;

-- Remove policies
-- ALTER TABLE RAW.TRADES_RAW DROP ROW ACCESS POLICY ACCOUNT_ACCESS_POLICY;
-- ALTER TABLE RAW.TRADES_RAW MODIFY COLUMN TRADER_ID UNSET MASKING POLICY;
*/

-- =============================================================================
-- OBJECT TAGGING - Metadata Classification
-- =============================================================================
-- See: https://docs.snowflake.com/en/user-guide/object-tagging

/*
-- Create tags for classification
CREATE OR REPLACE TAG DATA_CLASSIFICATION ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'PII';
CREATE OR REPLACE TAG DATA_OWNER;
CREATE OR REPLACE TAG COST_CENTER;

-- Apply tags to objects
ALTER TABLE RAW.TRADES_RAW SET TAG
    DATA_CLASSIFICATION = 'CONFIDENTIAL',
    DATA_OWNER = 'trading-team',
    COST_CENTER = 'TRADING-001';

-- Tag specific columns
ALTER TABLE RAW.TRADES_RAW MODIFY COLUMN ACCOUNT_ID SET TAG DATA_CLASSIFICATION = 'PII';

-- Find all objects with a specific tag
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('DATA_CLASSIFICATION', 'TABLE'));

-- Query tag values
SELECT SYSTEM$GET_TAG('DATA_CLASSIFICATION', 'TRADING_LAB_DB.RAW.TRADES_RAW', 'TABLE');
*/

-- =============================================================================
-- DYNAMIC TABLES - Alternative to Streams + Tasks
-- =============================================================================
-- See: https://docs.snowflake.com/en/user-guide/dynamic-tables-intro

/*
-- Dynamic tables automatically refresh based on a query
-- They're simpler than streams + tasks for incremental materialization

CREATE OR REPLACE DYNAMIC TABLE STAGE.TRADES_ENRICHED_DT
    TARGET_LAG = '1 minute'  -- Refresh within 1 minute of source changes
    WAREHOUSE = TRADING_TRANSFORM_WH
AS
SELECT
    t.TRADE_ID,
    t.SYMBOL,
    t.SIDE,
    t.QUANTITY,
    t.PRICE,
    t.EXECUTION_TS,
    t.ACCOUNT_ID,
    t.VENUE,
    t.QUANTITY * t.PRICE AS NOTIONAL_VALUE,
    p.QUANTITY AS POSITION_QTY,
    p.AVG_COST,
    CASE
        WHEN t.SIDE = 'SELL' THEN (t.PRICE - p.AVG_COST) * t.QUANTITY
        ELSE 0
    END AS REALIZED_PNL
FROM RAW.TRADES_RAW t
LEFT JOIN RAW.POSITIONS_RAW p
    ON t.ACCOUNT_ID = p.ACCOUNT_ID
    AND t.SYMBOL = p.SYMBOL
    AND t.EXECUTION_TS::DATE = p.AS_OF_DATE;

-- Check dynamic table status
SHOW DYNAMIC TABLES LIKE 'TRADES%';

-- Manually refresh
ALTER DYNAMIC TABLE STAGE.TRADES_ENRICHED_DT REFRESH;
*/

