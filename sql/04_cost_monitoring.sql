-- =============================================================================
-- 04_cost_monitoring.sql - Cost Control and Usage Monitoring
-- =============================================================================
-- Resource monitors for budget control and queries for monitoring usage
-- Run as ACCOUNTADMIN
-- See: https://docs.snowflake.com/en/user-guide/resource-monitors
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- RESOURCE MONITORS - Budget Controls
-- =============================================================================
-- Resource monitors track credit usage and can notify or suspend warehouses
-- when thresholds are exceeded. Essential for cost control.

-- -----------------------------------------------------------------------------
-- 1. Create Resource Monitor for Trading Lab
-- -----------------------------------------------------------------------------
-- Adjust CREDIT_QUOTA based on your expected monthly usage
CREATE OR REPLACE RESOURCE MONITOR TRADING_LAB_MONITOR
    WITH
        CREDIT_QUOTA = 100                    -- Monthly credit limit
        FREQUENCY = MONTHLY                   -- Reset every month
        START_TIMESTAMP = IMMEDIATELY         -- Start tracking now
        END_TIMESTAMP = NULL                  -- No end date
    TRIGGERS
        ON 50 PERCENT DO NOTIFY               -- Email at 50%
        ON 75 PERCENT DO NOTIFY               -- Email at 75%
        ON 90 PERCENT DO NOTIFY               -- Email at 90%
        ON 100 PERCENT DO SUSPEND             -- Suspend warehouses at 100%
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;  -- Force suspend at 110%

-- -----------------------------------------------------------------------------
-- 2. Assign Resource Monitor to Warehouses
-- -----------------------------------------------------------------------------
-- Apply the monitor to all Trading Lab warehouses
ALTER WAREHOUSE TRADING_INGEST_WH SET RESOURCE_MONITOR = TRADING_LAB_MONITOR;
ALTER WAREHOUSE TRADING_TRANSFORM_WH SET RESOURCE_MONITOR = TRADING_LAB_MONITOR;
ALTER WAREHOUSE TRADING_ANALYTICS_WH SET RESOURCE_MONITOR = TRADING_LAB_MONITOR;

-- -----------------------------------------------------------------------------
-- 3. View Resource Monitor Status
-- -----------------------------------------------------------------------------
-- List all resource monitors and their current status
SHOW RESOURCE MONITORS;

-- To see details of a specific monitor:
-- SHOW RESOURCE MONITORS LIKE 'TRADING_LAB_MONITOR';

-- Note: Resource monitor usage data is also available in:
-- SNOWFLAKE.ACCOUNT_USAGE.RESOURCE_MONITORS (has 45-minute latency)
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.RESOURCE_MONITORS 
-- WHERE NAME = 'TRADING_LAB_MONITOR';

-- =============================================================================
-- USAGE MONITORING QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4. Warehouse Credit Consumption (Last 7 Days)
-- -----------------------------------------------------------------------------
SELECT
    WAREHOUSE_NAME,
    DATE_TRUNC('hour', START_TIME) AS HOUR,
    SUM(CREDITS_USED) AS CREDITS_USED,
    SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME IN ('TRADING_INGEST_WH', 'TRADING_TRANSFORM_WH', 'TRADING_ANALYTICS_WH')
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY WAREHOUSE_NAME, HOUR DESC;

-- -----------------------------------------------------------------------------
-- 5. Daily Credit Summary by Warehouse
-- -----------------------------------------------------------------------------
SELECT
    WAREHOUSE_NAME,
    DATE_TRUNC('day', START_TIME) AS DAY,
    ROUND(SUM(CREDITS_USED), 4) AS TOTAL_CREDITS,
    COUNT(DISTINCT DATE_TRUNC('hour', START_TIME)) AS ACTIVE_HOURS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME LIKE 'TRADING%'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY DAY DESC, WAREHOUSE_NAME;

-- -----------------------------------------------------------------------------
-- 6. Task Execution History
-- -----------------------------------------------------------------------------
SELECT
    NAME AS TASK_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    TIMESTAMPDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE DATABASE_NAME = 'TRADING_LAB_DB'
ORDER BY SCHEDULED_TIME DESC;

-- -----------------------------------------------------------------------------
-- 7. Query Performance (Last 24 Hours)
-- -----------------------------------------------------------------------------
SELECT
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME,
    USER_NAME,
    ROLE_NAME,
    START_TIME,
    TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SEC,
    EXECUTION_TIME / 1000 AS EXEC_SEC,
    QUEUED_OVERLOAD_TIME / 1000 AS QUEUE_SEC,
    BYTES_SCANNED / (1024*1024) AS MB_SCANNED,
    ROWS_PRODUCED,
    CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME IN ('TRADING_INGEST_WH', 'TRADING_TRANSFORM_WH', 'TRADING_ANALYTICS_WH')
  AND START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND QUERY_TYPE != 'UNKNOWN'
ORDER BY START_TIME DESC
LIMIT 100;

-- -----------------------------------------------------------------------------
-- 8. Expensive Queries (by execution time)
-- -----------------------------------------------------------------------------
SELECT
    QUERY_ID,
    SUBSTR(QUERY_TEXT, 1, 100) AS QUERY_PREVIEW,
    WAREHOUSE_NAME,
    TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SEC,
    BYTES_SCANNED / (1024*1024*1024) AS GB_SCANNED,
    ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME LIKE 'TRADING%'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND EXECUTION_STATUS = 'SUCCESS'
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 9. Storage Usage
-- -----------------------------------------------------------------------------
SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024*1024) AS SIZE_MB,
    ACTIVE_BYTES / (1024*1024) AS ACTIVE_MB,
    TIME_TRAVEL_BYTES / (1024*1024) AS TIME_TRAVEL_MB,
    FAILSAFE_BYTES / (1024*1024) AS FAILSAFE_MB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'TRADING_LAB_DB'
  AND DELETED = FALSE
ORDER BY BYTES DESC;

-- -----------------------------------------------------------------------------
-- 10. Estimated Monthly Cost (based on recent usage)
-- -----------------------------------------------------------------------------
WITH daily_credits AS (
    SELECT
        DATE_TRUNC('day', START_TIME) AS DAY,
        SUM(CREDITS_USED) AS CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE WAREHOUSE_NAME LIKE 'TRADING%'
      AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1
)
SELECT
    ROUND(AVG(CREDITS), 4) AS AVG_DAILY_CREDITS,
    ROUND(AVG(CREDITS) * 30, 2) AS EST_MONTHLY_CREDITS,
    ROUND(AVG(CREDITS) * 30 * 3, 2) AS EST_MONTHLY_COST_USD  -- Assuming $3/credit
FROM daily_credits;

-- =============================================================================
-- CLEANUP (if needed)
-- =============================================================================
-- To remove the resource monitor:
-- ALTER WAREHOUSE TRADING_INGEST_WH SET RESOURCE_MONITOR = NULL;
-- ALTER WAREHOUSE TRADING_TRANSFORM_WH SET RESOURCE_MONITOR = NULL;
-- ALTER WAREHOUSE TRADING_ANALYTICS_WH SET RESOURCE_MONITOR = NULL;
-- DROP RESOURCE MONITOR TRADING_LAB_MONITOR;

