-- Create Snowpark Python stored procedure for transformations
-- Run after 02_streams_tasks.sql creates the target tables
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE LAB_TRANSFORM_WH;

-- ============================================================
-- Option A: Inline Python procedure (simplest for demo)
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_TRANSFORM_ORDERS()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_timestamp

def main(session: Session) -> str:
    # USE statements not allowed in stored procs; use fully qualified names
    orders_raw = session.table("DEMO_LAB_DB.RAW.ORDERS_RAW")
    inventory = session.table("DEMO_LAB_DB.RAW.INVENTORY_RAW")

    enriched = (
        orders_raw.join(inventory, orders_raw["ORDER_ID"] == inventory["SKU"], how="left")
        .with_column("ORDER_TS", to_timestamp(col("ORDER_TS")))
        .with_column("UPDATED_AT", to_timestamp(col("UPDATED_AT")))
    )

    enriched.write.save_as_table(
        "DEMO_LAB_DB.STAGE.ORDERS_ENRICHED",
        mode="overwrite",
    )

    return "ORDERS_ENRICHED refreshed"
$$;

-- Grant execute to demo role
GRANT USAGE ON PROCEDURE SP_TRANSFORM_ORDERS() TO ROLE DEMO_LAB_ROLE;

-- ============================================================
-- Option B: Deploy from Git repo (if using Snowflake Git integration)
-- After running 04_git_integration.sql and fetching the repo:
-- ============================================================
-- CREATE OR REPLACE PROCEDURE SP_TRANSFORM_ORDERS()
--   RETURNS STRING
--   LANGUAGE PYTHON
--   RUNTIME_VERSION = '3.10'
--   PACKAGES = ('snowflake-snowpark-python')
--   IMPORTS = ('@LAB_GIT_REPO/branches/main/python/sp_transform_orders.py')
--   HANDLER = 'sp_transform_orders.main';

-- ============================================================
-- Test the procedure
-- ============================================================
-- CALL SP_TRANSFORM_ORDERS();
-- SELECT * FROM DEMO_LAB_DB.STAGE.ORDERS_ENRICHED LIMIT 10;

