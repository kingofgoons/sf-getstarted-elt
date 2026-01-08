-- Create Snowpark Python stored procedure for transformations
-- Run after 02_streams_tasks.sql creates the target tables
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_LAB_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE LAB_TRANSFORM_WH;

-- ============================================================
-- Option A: Inline Python procedure (simplest for demo)
-- Enriches trades with current position data by SYMBOL
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_ENRICH_TRADES()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_timestamp, sum as sf_sum, avg as sf_avg

def main(session: Session) -> str:
    # USE statements not allowed in stored procs; use fully qualified names
    trades = session.table("DEMO_LAB_DB.RAW.TRADES_RAW")
    positions = session.table("DEMO_LAB_DB.RAW.POSITIONS_RAW")

    # Aggregate positions by symbol (latest price, total holdings)
    positions_agg = (
        positions
        .group_by("SYMBOL")
        .agg(
            sf_avg("CURRENT_PRICE").alias("AVG_MARKET_PRICE"),
            sf_sum("QUANTITY").alias("TOTAL_POSITION_QTY"),
            sf_sum("MARKET_VALUE").alias("TOTAL_MARKET_VALUE")
        )
    )

    # Enrich trades with position context
    enriched = (
        trades
        .join(positions_agg, trades["SYMBOL"] == positions_agg["SYMBOL"], how="left")
        .select(
            trades["TRADE_ID"],
            trades["ACCOUNT_ID"],
            trades["SYMBOL"],
            trades["TRADE_TS"],
            trades["SIDE"],
            trades["QUANTITY"],
            trades["PRICE"],
            trades["AMOUNT"],
            trades["EXCHANGE"],
            trades["STATUS"],
            positions_agg["AVG_MARKET_PRICE"],
            positions_agg["TOTAL_POSITION_QTY"],
            positions_agg["TOTAL_MARKET_VALUE"]
        )
    )

    enriched.write.save_as_table(
        "DEMO_LAB_DB.STAGE.TRADES_ENRICHED",
        mode="overwrite",
    )

    return "TRADES_ENRICHED refreshed"
$$;

-- Grant execute to demo role
GRANT USAGE ON PROCEDURE SP_ENRICH_TRADES() TO ROLE DEMO_LAB_ROLE;

-- ============================================================
-- Option B: Deploy from Git repo (if using Snowflake Git integration)
-- After running 04_git_integration.sql and fetching the repo:
-- ============================================================
-- CREATE OR REPLACE PROCEDURE SP_ENRICH_TRADES()
--   RETURNS STRING
--   LANGUAGE PYTHON
--   RUNTIME_VERSION = '3.10'
--   PACKAGES = ('snowflake-snowpark-python')
--   IMPORTS = ('@LAB_GIT_REPO/branches/main/python/sp_enrich_trades.py')
--   HANDLER = 'sp_enrich_trades.main';

-- ============================================================
-- Test the procedure
-- ============================================================
-- CALL SP_ENRICH_TRADES();
-- SELECT * FROM DEMO_LAB_DB.STAGE.TRADES_ENRICHED LIMIT 10;

