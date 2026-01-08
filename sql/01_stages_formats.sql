-- =============================================================================
-- 01_stages_formats.sql - Stages, File Formats, and Raw Tables
-- =============================================================================
-- Run after 00_setup.sql
-- Creates: Stages, file formats, raw tables, and COPY commands
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;
USE SCHEMA TRADING_LAB_DB.RAW;
USE WAREHOUSE TRADING_INGEST_WH;

-- -----------------------------------------------------------------------------
-- 1. File Formats
-- -----------------------------------------------------------------------------
-- CSV format for trade executions
CREATE OR REPLACE FILE FORMAT FF_CSV_TRADES
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    COMMENT = 'CSV format for trade execution files';

-- JSON format for market events (semi-structured)
CREATE OR REPLACE FILE FORMAT FF_JSON_EVENTS
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for market event feeds';

-- Parquet format for position snapshots
CREATE OR REPLACE FILE FORMAT FF_PARQUET_POSITIONS
    TYPE = PARQUET
    COMMENT = 'Parquet format for EOD position files';

-- -----------------------------------------------------------------------------
-- 2. Internal Stage (for local file uploads)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE RAW_INTERNAL_STAGE
    FILE_FORMAT = FF_CSV_TRADES
    COMMENT = 'Internal stage for uploading sample data files';

-- -----------------------------------------------------------------------------
-- 3. External Stage (AWS S3)
-- -----------------------------------------------------------------------------
-- Uses existing S3_INT storage integration
-- See: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

-- =============================================================================
-- STORAGE INTEGRATION REFERENCE (already exists as S3_INT)
-- =============================================================================
-- A storage integration is a Snowflake object that stores a generated IAM user
-- for your S3 bucket. The integration delegates authentication to Snowflake
-- instead of requiring you to pass credentials.
--
-- Your existing S3_INT was created like this:
--
-- CREATE OR REPLACE STORAGE INTEGRATION S3_INT
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/<role-name>'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket-name>/');
--
-- To verify the integration and get the AWS IAM user ARN + External ID:
-- DESC INTEGRATION S3_INT;
--
-- The STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values from DESC
-- must be added to your IAM role's trust policy in AWS.
-- =============================================================================

-- Verify the integration exists and check its configuration
DESC INTEGRATION S3_INT;

-- Grant usage on the integration to the demo role (if not already granted)
GRANT USAGE ON INTEGRATION S3_INT TO ROLE TRADING_LAB_ROLE;

-- =============================================================================
-- EXTERNAL STAGE
-- =============================================================================
-- An external stage references an external location (S3) and uses the storage
-- integration for authentication. No credentials are stored in the stage.
--
-- URL format: s3://<bucket-name>/<path>/
-- The path should match a prefix allowed in STORAGE_ALLOWED_LOCATIONS

-- ⚠️ EDIT THIS: Replace YOUR-BUCKET with your S3 bucket name
CREATE OR REPLACE STAGE RAW_S3_STAGE
    STORAGE_INTEGRATION = S3_INT
    URL = 's3://YOUR-BUCKET/finserv-getting-started/'
    FILE_FORMAT = FF_CSV_TRADES
    COMMENT = 'External S3 stage for Financial Services demo data';

-- Verify the stage was created and list files
SHOW STAGES LIKE 'RAW_S3%';
LIST @RAW_S3_STAGE;

-- =============================================================================
-- HOW STORAGE INTEGRATION + EXTERNAL STAGE WORK TOGETHER
-- =============================================================================
--
--   ┌─────────────────────────────────────────────────────────────────────────┐
--   │                         AWS ACCOUNT                                     │
--   │  ┌─────────────────────────────────────────────────────────────────┐   │
--   │  │  S3 Bucket: YOUR-BUCKET                                          │   │
--   │  │  └── finserv-getting-started/                                   │   │
--   │  │      ├── trades.csv                                             │   │
--   │  │      ├── market_events.json                                     │   │
--   │  │      └── positions.parquet                                      │   │
--   │  └─────────────────────────────────────────────────────────────────┘   │
--   │                              ▲                                          │
--   │                              │ AssumeRole                               │
--   │  ┌─────────────────────────────────────────────────────────────────┐   │
--   │  │  IAM Role (trusts Snowflake's AWS account + external ID)        │   │
--   │  │  Policy: s3:GetObject, s3:ListBucket on bucket/prefix           │   │
--   │  └─────────────────────────────────────────────────────────────────┘   │
--   └─────────────────────────────────────────────────────────────────────────┘
--                                  ▲
--                                  │ STS AssumeRole
--   ┌─────────────────────────────────────────────────────────────────────────┐
--   │                       SNOWFLAKE ACCOUNT                                 │
--   │                                                                         │
--   │  ┌─────────────────────────────────────────────────────────────────┐   │
--   │  │  STORAGE INTEGRATION: S3_INT                                    │   │
--   │  │  - Stores IAM role ARN                                          │   │
--   │  │  - Snowflake-managed AWS IAM user                               │   │
--   │  │  - STORAGE_ALLOWED_LOCATIONS validates paths                    │   │
--   │  └──────────────────────────┬──────────────────────────────────────┘   │
--   │                             │                                           │
--   │                             ▼                                           │
--   │  ┌─────────────────────────────────────────────────────────────────┐   │
--   │  │  EXTERNAL STAGE: RAW_S3_STAGE                                   │   │
--   │  │  - URL = s3://YOUR-BUCKET/finserv-getting-started/               │   │
--   │  │  - STORAGE_INTEGRATION = S3_INT                                 │   │
--   │  │  - FILE_FORMAT = FF_CSV_TRADES (default)                        │   │
--   │  └──────────────────────────┬──────────────────────────────────────┘   │
--   │                             │                                           │
--   │                             ▼                                           │
--   │  ┌─────────────────────────────────────────────────────────────────┐   │
--   │  │  COPY INTO / SELECT FROM @RAW_S3_STAGE/...                      │   │
--   │  │  - No credentials needed in SQL                                 │   │
--   │  │  - Snowflake handles auth via integration                       │   │
--   │  └─────────────────────────────────────────────────────────────────┘   │
--   └─────────────────────────────────────────────────────────────────────────┘
--
-- Key Benefits:
-- 1. No credentials stored in stage definition (secure)
-- 2. Centralized credential management in integration
-- 3. Role-based access control via GRANT USAGE ON INTEGRATION
-- 4. Path validation via STORAGE_ALLOWED_LOCATIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4. Raw Tables
-- -----------------------------------------------------------------------------

-- Trade executions (from OMS/EMS)
CREATE OR REPLACE TABLE TRADES_RAW (
    TRADE_ID        STRING      NOT NULL    COMMENT 'Unique trade identifier',
    SYMBOL          STRING      NOT NULL    COMMENT 'Ticker symbol (e.g., AAPL)',
    SIDE            STRING      NOT NULL    COMMENT 'BUY or SELL',
    QUANTITY        NUMBER(18,4) NOT NULL   COMMENT 'Number of shares/units',
    PRICE           NUMBER(18,6) NOT NULL   COMMENT 'Execution price',
    EXECUTION_TS    TIMESTAMP_NTZ NOT NULL  COMMENT 'Trade execution timestamp',
    ACCOUNT_ID      STRING      NOT NULL    COMMENT 'Trading account identifier',
    VENUE           STRING                  COMMENT 'Execution venue (NYSE, NASDAQ, etc.)',
    TRADER_ID       STRING                  COMMENT 'Trader identifier',
    ORDER_ID        STRING                  COMMENT 'Parent order ID',
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL load timestamp'
)
COMMENT = 'Raw trade execution records from trading systems';

-- Market events (semi-structured)
CREATE OR REPLACE TABLE MARKET_EVENTS_RAW (
    EVENT_TS        TIMESTAMP_NTZ NOT NULL  COMMENT 'Event timestamp',
    SYMBOL          STRING      NOT NULL    COMMENT 'Affected symbol',
    EVENT_TYPE      STRING      NOT NULL    COMMENT 'Event type: PRICE_UPDATE, DIVIDEND, SPLIT, HALT',
    EVENT_DATA      VARIANT     NOT NULL    COMMENT 'Semi-structured event payload',
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL load timestamp'
)
COMMENT = 'Raw market events - prices, corporate actions, halts';

-- End-of-day positions (from portfolio systems)
CREATE OR REPLACE TABLE POSITIONS_RAW (
    ACCOUNT_ID      STRING      NOT NULL    COMMENT 'Trading account identifier',
    SYMBOL          STRING      NOT NULL    COMMENT 'Ticker symbol',
    QUANTITY        NUMBER(18,4) NOT NULL   COMMENT 'Position quantity (negative for short)',
    AVG_COST        NUMBER(18,6) NOT NULL   COMMENT 'Average cost basis per share',
    MARKET_VALUE    NUMBER(18,2) NOT NULL   COMMENT 'Current market value',
    AS_OF_DATE      DATE        NOT NULL    COMMENT 'Position snapshot date',
    SECTOR          STRING                  COMMENT 'Industry sector',
    ASSET_CLASS     STRING                  COMMENT 'Asset class (EQUITY, FIXED_INCOME, etc.)',
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'ETL load timestamp'
)
COMMENT = 'Raw EOD position snapshots from portfolio systems';

-- -----------------------------------------------------------------------------
-- 5. COPY INTO Commands (from S3 External Stage)
-- -----------------------------------------------------------------------------
-- Loading data from s3://YOUR-BUCKET/finserv-getting-started/
-- via the RAW_S3_STAGE which uses S3_INT storage integration

-- Copy trades from S3
COPY INTO TRADES_RAW (
    TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, 
    EXECUTION_TS, ACCOUNT_ID, VENUE, TRADER_ID, ORDER_ID
)
FROM @RAW_S3_STAGE/trades.csv
FILE_FORMAT = FF_CSV_TRADES
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Copy market events (JSON) from S3
COPY INTO MARKET_EVENTS_RAW (EVENT_TS, SYMBOL, EVENT_TYPE, EVENT_DATA)
FROM (
    SELECT 
        $1:event_ts::TIMESTAMP_NTZ,
        $1:symbol::STRING,
        $1:event_type::STRING,
        $1:event_data::VARIANT
    FROM @RAW_S3_STAGE/market_events.json
    (FILE_FORMAT => FF_JSON_EVENTS)
)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Copy positions (Parquet) from S3
COPY INTO POSITIONS_RAW (
    ACCOUNT_ID, SYMBOL, QUANTITY, AVG_COST, 
    MARKET_VALUE, AS_OF_DATE, SECTOR, ASSET_CLASS
)
FROM (
    SELECT 
        $1:ACCOUNT_ID::STRING,
        $1:SYMBOL::STRING,
        $1:QUANTITY::NUMBER(18,4),
        $1:AVG_COST::NUMBER(18,6),
        $1:MARKET_VALUE::NUMBER(18,2),
        $1:AS_OF_DATE::DATE,
        $1:SECTOR::STRING,
        $1:ASSET_CLASS::STRING
    FROM @RAW_S3_STAGE/positions.parquet
    (FILE_FORMAT => FF_PARQUET_POSITIONS)
)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- -----------------------------------------------------------------------------
-- 5b. COPY INTO Commands (from Internal Stage - Alternative)
-- -----------------------------------------------------------------------------
-- Use these if loading from local files via PUT command instead of S3
/*
-- First upload files:
-- PUT file:///path/to/trades.csv @RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;
-- PUT file:///path/to/market_events.json @RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;
-- PUT file:///path/to/positions.parquet @RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;

COPY INTO TRADES_RAW FROM @RAW_INTERNAL_STAGE/trades.csv FILE_FORMAT = FF_CSV_TRADES;
COPY INTO MARKET_EVENTS_RAW FROM (SELECT ... FROM @RAW_INTERNAL_STAGE/market_events.json);
COPY INTO POSITIONS_RAW FROM (SELECT ... FROM @RAW_INTERNAL_STAGE/positions.parquet);
*/

-- -----------------------------------------------------------------------------
-- 6. Verification Queries
-- -----------------------------------------------------------------------------

-- List files in S3 stage:
LIST @RAW_S3_STAGE;

-- Check loaded data:
SELECT 'TRADES_RAW' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM TRADES_RAW
UNION ALL
SELECT 'MARKET_EVENTS_RAW', COUNT(*) FROM MARKET_EVENTS_RAW
UNION ALL
SELECT 'POSITIONS_RAW', COUNT(*) FROM POSITIONS_RAW;

-- Sample data:
-- SELECT * FROM TRADES_RAW LIMIT 10;
-- SELECT * FROM MARKET_EVENTS_RAW LIMIT 10;
-- SELECT * FROM POSITIONS_RAW LIMIT 10;
