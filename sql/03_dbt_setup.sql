-- =============================================================================
-- 03_dbt_setup.sql - DBT Service Account and Permissions
-- =============================================================================
-- Creates dedicated role and grants for DBT to operate on ANALYTICS schema
-- Run as ACCOUNTADMIN after 00_setup.sql
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. DBT Service Role
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ROLE DBT_TRADING_ROLE
    COMMENT = 'Service role for DBT operations on trading analytics';

-- Grant to admin for management
GRANT ROLE DBT_TRADING_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE DBT_TRADING_ROLE TO ROLE TRADING_LAB_ROLE;

-- -----------------------------------------------------------------------------
-- 2. Database and Schema Access
-- -----------------------------------------------------------------------------
-- Database usage
GRANT USAGE ON DATABASE TRADING_LAB_DB TO ROLE DBT_TRADING_ROLE;

-- Read access to source schemas (RAW, STAGE, CURATED)
GRANT USAGE ON SCHEMA TRADING_LAB_DB.RAW TO ROLE DBT_TRADING_ROLE;
GRANT USAGE ON SCHEMA TRADING_LAB_DB.STAGE TO ROLE DBT_TRADING_ROLE;
GRANT USAGE ON SCHEMA TRADING_LAB_DB.CURATED TO ROLE DBT_TRADING_ROLE;

-- Read all tables in source schemas
GRANT SELECT ON ALL TABLES IN SCHEMA TRADING_LAB_DB.RAW TO ROLE DBT_TRADING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA TRADING_LAB_DB.STAGE TO ROLE DBT_TRADING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA TRADING_LAB_DB.CURATED TO ROLE DBT_TRADING_ROLE;

-- Future tables in source schemas
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRADING_LAB_DB.RAW TO ROLE DBT_TRADING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRADING_LAB_DB.STAGE TO ROLE DBT_TRADING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA TRADING_LAB_DB.CURATED TO ROLE DBT_TRADING_ROLE;

-- -----------------------------------------------------------------------------
-- 3. STAGING Schema - For DBT staging views
-- -----------------------------------------------------------------------------
-- Create STAGING schema for DBT staging models (separate from Snowpark STAGE)
CREATE SCHEMA IF NOT EXISTS TRADING_LAB_DB.STAGING;

GRANT USAGE ON SCHEMA TRADING_LAB_DB.STAGING TO ROLE DBT_TRADING_ROLE;
GRANT CREATE VIEW ON SCHEMA TRADING_LAB_DB.STAGING TO ROLE DBT_TRADING_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA TRADING_LAB_DB.STAGING TO ROLE DBT_TRADING_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA TRADING_LAB_DB.STAGING TO ROLE DBT_TRADING_ROLE;

-- -----------------------------------------------------------------------------
-- 4. ANALYTICS Schema - Full Control for DBT marts
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT CREATE TABLE ON SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT CREATE VIEW ON SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT CREATE DYNAMIC TABLE ON SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;

-- All privileges on existing and future objects in ANALYTICS
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA TRADING_LAB_DB.ANALYTICS TO ROLE DBT_TRADING_ROLE;

-- -----------------------------------------------------------------------------
-- 5. Warehouse Access
-- -----------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE TRADING_ANALYTICS_WH TO ROLE DBT_TRADING_ROLE;
GRANT OPERATE ON WAREHOUSE TRADING_ANALYTICS_WH TO ROLE DBT_TRADING_ROLE;

-- -----------------------------------------------------------------------------
-- 6. Optional: Create DBT Service User
-- -----------------------------------------------------------------------------
-- Uncomment and customize for production deployments

/*
CREATE OR REPLACE USER DBT_SERVICE_USER
    PASSWORD = '<strong-password>'
    LOGIN_NAME = 'dbt_service'
    DISPLAY_NAME = 'DBT Service Account'
    DEFAULT_ROLE = DBT_TRADING_ROLE
    DEFAULT_WAREHOUSE = TRADING_ANALYTICS_WH
    DEFAULT_NAMESPACE = TRADING_LAB_DB.ANALYTICS
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for DBT CI/CD pipelines';

GRANT ROLE DBT_TRADING_ROLE TO USER DBT_SERVICE_USER;
*/

-- -----------------------------------------------------------------------------
-- 7. Verification
-- -----------------------------------------------------------------------------
-- Check grants to DBT role
SHOW GRANTS TO ROLE DBT_TRADING_ROLE;

-- Test as DBT role
-- USE ROLE DBT_TRADING_ROLE;
-- USE WAREHOUSE TRADING_ANALYTICS_WH;
-- SELECT * FROM TRADING_LAB_DB.CURATED.TRADE_METRICS LIMIT 10;
-- CREATE OR REPLACE TABLE TRADING_LAB_DB.ANALYTICS.TEST_TABLE (ID INT);
-- DROP TABLE TRADING_LAB_DB.ANALYTICS.TEST_TABLE;

