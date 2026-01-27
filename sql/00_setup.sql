-- =============================================================================
-- 00_setup.sql - Financial Services ELT Demo Setup
-- =============================================================================
-- Run as ACCOUNTADMIN (trial account OK)
-- Creates: Role, Warehouses, Database, Schemas for trading data pipeline
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Demo Role
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ROLE TRADING_LAB_ROLE
    COMMENT = 'Role for Financial Services ELT demo - trade/position analytics';

GRANT ROLE TRADING_LAB_ROLE TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 2. Warehouses (sized for different workload types)
-- -----------------------------------------------------------------------------
-- Ingest WH: Small for COPY operations (bursty, short-running)
CREATE OR REPLACE WAREHOUSE TRADING_INGEST_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'XS warehouse for data ingestion (COPY INTO)';

-- Transform WH: Medium for Snowpark procedures and task-based transforms
CREATE OR REPLACE WAREHOUSE TRADING_TRANSFORM_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Small warehouse for Snowpark transforms and tasks';

-- Analytics WH: For DBT runs and ad-hoc queries
CREATE OR REPLACE WAREHOUSE TRADING_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Small warehouse for DBT and analytics queries';

-- -----------------------------------------------------------------------------
-- 3. Database and Schemas (Medallion Architecture)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DATABASE TRADING_LAB_DB
    COMMENT = 'Financial Services ELT Demo - Trades, Positions, Market Events';

-- RAW: Landing zone for ingested data (bronze layer)
CREATE OR REPLACE SCHEMA TRADING_LAB_DB.RAW
    COMMENT = 'Raw ingested data - trades, positions, market events';

-- STAGE: Cleaned and enriched data (silver layer)
CREATE OR REPLACE SCHEMA TRADING_LAB_DB.STAGE
    COMMENT = 'Transformed data - enriched trades, flattened events';

-- CURATED: Business-ready aggregates (gold layer)
CREATE OR REPLACE SCHEMA TRADING_LAB_DB.CURATED
    COMMENT = 'Curated metrics - trade summaries, position snapshots';

-- ANALYTICS: DBT-managed models (reporting layer)
CREATE OR REPLACE SCHEMA TRADING_LAB_DB.ANALYTICS
    COMMENT = 'DBT models - facts, dimensions, P&L calculations';

-- -----------------------------------------------------------------------------
-- 4. Grant Privileges to Demo Role
-- -----------------------------------------------------------------------------
-- Database access
GRANT USAGE ON DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;

-- Schema access (current and future)
GRANT USAGE ON ALL SCHEMAS IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;

-- Full privileges on schemas for demo flexibility
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;

-- Table privileges (current and future)
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE TRADING_LAB_DB TO ROLE TRADING_LAB_ROLE;

-- Warehouse access
GRANT USAGE, OPERATE ON WAREHOUSE TRADING_INGEST_WH TO ROLE TRADING_LAB_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE TRADING_TRANSFORM_WH TO ROLE TRADING_LAB_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE TRADING_ANALYTICS_WH TO ROLE TRADING_LAB_ROLE;

-- -----------------------------------------------------------------------------
-- 5. Verification
-- -----------------------------------------------------------------------------
-- Run these to confirm setup:
-- SHOW WAREHOUSES LIKE 'TRADING%';
-- SHOW SCHEMAS IN DATABASE TRADING_LAB_DB;
-- SHOW GRANTS TO ROLE TRADING_LAB_ROLE;
