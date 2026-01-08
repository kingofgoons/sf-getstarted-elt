-- =============================================================================
-- 02d_snowpark_procedure.sql - Python Transformation with Snowpark
-- =============================================================================
-- 
-- This script shows how to use Snowpark Python for transformations.
-- Same logic as 02b, but in Python instead of SQL.
--
-- WHY SNOWPARK?
-- - Complex business logic is easier in Python
-- - Leverage Python libraries (pandas, numpy, etc.)
-- - Data scientists can contribute without learning SQL
-- - Unit testable code
--
-- DURATION: ~5 minutes
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TRADING_LAB_DB;
USE WAREHOUSE TRADING_TRANSFORM_WH;
USE SCHEMA PUBLIC;

-- =============================================================================
-- STEP 1: Create the Snowpark Stored Procedure
-- =============================================================================
-- This procedure does the same enrichment as the SQL in 02b, but in Python.
-- 
-- KEY FIX: We do NOT use session.use_database() or session.use_schema()
-- because those are not allowed in stored procedures. Instead, we use
-- fully qualified table names (DATABASE.SCHEMA.TABLE).

CREATE OR REPLACE PROCEDURE SP_TRANSFORM_TRADES()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
AS
$$
"""
Snowpark procedure to transform RAW trades into STAGE.TRADES_ENRICHED.

What it does:
1. Reads new trades from TRADES_RAW_STREAM (only unprocessed records)
2. Joins with positions to get cost basis
3. Calculates notional value and realized P&L
4. Writes enriched records to TRADES_ENRICHED table

Note: Uses fully qualified table names - do NOT use session.use_database()
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    abs as sf_abs,
    col,
    current_timestamp,
    least,
    lit,
    to_date,
    when,
)


def main(session: Session) -> str:
    """
    Transform trades from RAW stream to STAGE with enrichment.
    
    Args:
        session: Snowpark Session (injected by Snowflake)
    
    Returns:
        Status message with row count
    """
    # Read from stream using FULLY QUALIFIED name
    # The stream only returns NEW records since last consumption
    trades_stream = session.table("TRADING_LAB_DB.RAW.TRADES_RAW_STREAM")
    
    # Get positions for cost basis lookup (fully qualified)
    positions = session.table("TRADING_LAB_DB.RAW.POSITIONS_RAW")
    
    # Get one row per account/symbol
    latest_positions = positions.select(
        col("ACCOUNT_ID").alias("POS_ACCOUNT_ID"),
        col("SYMBOL").alias("POS_SYMBOL"),
        col("QUANTITY").alias("POSITION_QTY"),
        col("AVG_COST"),
    ).distinct()
    
    # Join trades with positions to get cost basis
    enriched = trades_stream.join(
        latest_positions,
        (trades_stream["ACCOUNT_ID"] == latest_positions["POS_ACCOUNT_ID"]) &
        (trades_stream["SYMBOL"] == latest_positions["POS_SYMBOL"]),
        how="left",
    ).select(
        trades_stream["TRADE_ID"],
        trades_stream["SYMBOL"],
        trades_stream["SIDE"],
        trades_stream["QUANTITY"],
        trades_stream["PRICE"],
        # Calculate notional value: quantity * price
        (trades_stream["QUANTITY"] * trades_stream["PRICE"]).alias("NOTIONAL_VALUE"),
        trades_stream["EXECUTION_TS"],
        to_date(trades_stream["EXECUTION_TS"]).alias("EXECUTION_DATE"),
        trades_stream["ACCOUNT_ID"],
        trades_stream["VENUE"],
        trades_stream["TRADER_ID"],
        trades_stream["ORDER_ID"],
        col("POSITION_QTY"),
        col("AVG_COST"),
        # Calculate realized P&L for CLOSING trades
        # SELL when long: profit = (price - avg_cost) * quantity
        # BUY when short: profit = (avg_cost - price) * quantity
        when(
            (col("SIDE") == lit("SELL")) & (col("POSITION_QTY") > 0),
            (col("PRICE") - col("AVG_COST")) * 
            least(col("QUANTITY"), col("POSITION_QTY"))
        ).when(
            (col("SIDE") == lit("BUY")) & (col("POSITION_QTY") < 0),
            (col("AVG_COST") - col("PRICE")) * 
            least(col("QUANTITY"), sf_abs(col("POSITION_QTY")))
        ).otherwise(lit(0.0)).alias("REALIZED_PNL"),
        # Flag closing trades
        when(
            ((col("SIDE") == lit("SELL")) & (col("POSITION_QTY") > 0)) |
            ((col("SIDE") == lit("BUY")) & (col("POSITION_QTY") < 0)),
            lit(True)
        ).otherwise(lit(False)).alias("IS_CLOSING"),
        current_timestamp().alias("_PROCESSED_AT"),
    )
    
    # Count rows
    row_count = enriched.count()
    
    if row_count > 0:
        # Write to enriched table (fully qualified)
        enriched.write.mode("append").save_as_table(
            "TRADING_LAB_DB.STAGE.TRADES_ENRICHED"
        )
    
    return f"SP_TRANSFORM_TRADES: {row_count} rows processed"
$$;

-- Verify procedure was created
SHOW PROCEDURES LIKE 'SP_TRANSFORM%' IN SCHEMA TRADING_LAB_DB.PUBLIC;

-- =============================================================================
-- STEP 2: Test the Procedure Manually
-- =============================================================================

-- First, insert a test trade
INSERT INTO RAW.TRADES_RAW 
    (TRADE_ID, SYMBOL, SIDE, QUANTITY, PRICE, EXECUTION_TS, ACCOUNT_ID, VENUE)
VALUES
    ('PYTH-001', 'META', 'BUY', 30, 505.00, CURRENT_TIMESTAMP(), 'ACCT-PYTHON', 'NASDAQ');

-- Verify stream has data
SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRADES_RAW_STREAM') AS has_data;
-- Should return: TRUE

-- Call the procedure
CALL SP_TRANSFORM_TRADES();
-- Should return: "SP_TRANSFORM_TRADES: 1 rows processed"

-- Verify the enriched record
SELECT 
    TRADE_ID,
    SYMBOL,
    SIDE,
    QUANTITY,
    PRICE,
    NOTIONAL_VALUE,  -- Should be 30 * 505 = 15,150
    REALIZED_PNL,
    IS_CLOSING
FROM STAGE.TRADES_ENRICHED 
WHERE TRADE_ID = 'PYTH-001';

-- =============================================================================
-- STEP 3: Update the Task to Use the Snowpark Procedure (Optional)
-- =============================================================================
-- If you prefer Python for transformations, update the task:

/*
-- First suspend the existing task
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES SUSPEND;

-- Recreate it to use the Snowpark procedure
CREATE OR REPLACE TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES
    WAREHOUSE = TRADING_TRANSFORM_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TRADING_LAB_DB.RAW.TRADES_RAW_STREAM')
AS
    CALL TRADING_LAB_DB.PUBLIC.SP_TRANSFORM_TRADES();

-- Re-enable
ALTER TASK TRADING_LAB_DB.STAGE.TASK_TRANSFORM_TRADES RESUME;
*/

-- =============================================================================
-- SUMMARY: SQL vs Snowpark
-- =============================================================================
--
-- | Aspect           | SQL (02b)              | Snowpark (02d)         |
-- |------------------|------------------------|------------------------|
-- | Language         | SQL                    | Python                 |
-- | Best for         | Simple transforms      | Complex logic          |
-- | Libraries        | SQL functions only     | pandas, numpy, etc.    |
-- | Testing          | Integration tests      | Unit tests possible    |
-- | Maintenance      | SQL skills             | Python skills          |
--
-- Use SQL when:
-- - Transformations are straightforward
-- - Team is SQL-heavy
-- - Performance is critical (SQL can be faster)
--
-- Use Snowpark when:
-- - Complex business logic (loops, conditionals)
-- - Need Python libraries
-- - Data science team contributions
-- - Want unit-testable transformation code
--
-- =============================================================================

