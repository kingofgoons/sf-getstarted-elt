"""
Snowpark stored procedure to transform RAW trades into STAGE.TRADES_ENRICHED.

This procedure:
1. Reads new trades from TRADES_RAW (via stream)
2. Joins with latest position data to get cost basis
3. Calculates realized P&L for closing trades
4. Writes enriched records to TRADES_ENRICHED

Intended to be called by task when TRADES_RAW_STREAM has data.
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    abs as sf_abs,
    col,
    current_timestamp,
    lit,
    to_date,
    when,
)
from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def calculate_realized_pnl(
    side: str,
    quantity: float,
    price: float,
    position_qty: float,
    avg_cost: float,
) -> tuple[float, bool]:
    """
    Calculate realized P&L for a trade.

    A trade is "closing" if it reduces the absolute position size:
    - SELL when long (position_qty > 0)
    - BUY when short (position_qty < 0)

    Args:
        side: Trade side (BUY or SELL)
        quantity: Trade quantity (always positive)
        price: Execution price
        position_qty: Current position before trade
        avg_cost: Average cost basis

    Returns:
        Tuple of (realized_pnl, is_closing)
    """
    if position_qty is None or avg_cost is None:
        return (0.0, False)

    is_closing = False
    realized_pnl = 0.0

    if side == "SELL" and position_qty > 0:
        # Closing a long position
        is_closing = True
        closing_qty = min(quantity, position_qty)
        realized_pnl = closing_qty * (price - avg_cost)
    elif side == "BUY" and position_qty < 0:
        # Closing a short position
        is_closing = True
        closing_qty = min(quantity, abs(position_qty))
        realized_pnl = closing_qty * (avg_cost - price)

    return (realized_pnl, is_closing)


def main(session: Session) -> str:
    """
    Main entry point for the stored procedure.

    Transforms trades from RAW to STAGE with enrichment and P&L calculation.

    Args:
        session: Snowpark Session (injected by Snowflake)

    Returns:
        Status message with row count
    """
    # Set context
    session.use_database("TRADING_LAB_DB")
    session.use_schema("STAGE")

    # Read from stream (only new/changed records)
    # If called when stream is empty, this returns 0 rows
    trades_stream = session.table("TRADING_LAB_DB.RAW.TRADES_RAW_STREAM")

    # Get latest positions for cost basis lookup
    positions = session.table("TRADING_LAB_DB.RAW.POSITIONS_RAW")

    # Get the latest position per account/symbol
    latest_positions = positions.select(
        col("ACCOUNT_ID"),
        col("SYMBOL"),
        col("QUANTITY").alias("POSITION_QTY"),
        col("AVG_COST"),
    ).distinct()

    # Join trades with positions
    enriched = trades_stream.join(
        latest_positions,
        (trades_stream["ACCOUNT_ID"] == latest_positions["ACCOUNT_ID"])
        & (trades_stream["SYMBOL"] == latest_positions["SYMBOL"]),
        how="left",
    ).select(
        trades_stream["TRADE_ID"],
        trades_stream["SYMBOL"],
        trades_stream["SIDE"],
        trades_stream["QUANTITY"],
        trades_stream["PRICE"],
        # Calculate notional value
        (trades_stream["QUANTITY"] * trades_stream["PRICE"]).alias("NOTIONAL_VALUE"),
        trades_stream["EXECUTION_TS"],
        to_date(trades_stream["EXECUTION_TS"]).alias("EXECUTION_DATE"),
        trades_stream["ACCOUNT_ID"],
        trades_stream["VENUE"],
        trades_stream["TRADER_ID"],
        trades_stream["ORDER_ID"],
        latest_positions["POSITION_QTY"],
        latest_positions["AVG_COST"],
        # Calculate realized P&L for closing trades
        when(
            (col("SIDE") == lit("SELL")) & (col("POSITION_QTY") > 0),
            # Closing long: (price - avg_cost) * min(qty, position)
            (col("PRICE") - col("AVG_COST"))
            * when(col("QUANTITY") < col("POSITION_QTY"), col("QUANTITY")).otherwise(
                col("POSITION_QTY")
            ),
        )
        .when(
            (col("SIDE") == lit("BUY")) & (col("POSITION_QTY") < 0),
            # Closing short: (avg_cost - price) * min(qty, abs(position))
            (col("AVG_COST") - col("PRICE"))
            * when(col("QUANTITY") < sf_abs(col("POSITION_QTY")), col("QUANTITY")).otherwise(
                sf_abs(col("POSITION_QTY"))
            ),
        )
        .otherwise(lit(0.0))
        .alias("REALIZED_PNL"),
        # Flag closing trades
        when(
            ((col("SIDE") == lit("SELL")) & (col("POSITION_QTY") > 0))
            | ((col("SIDE") == lit("BUY")) & (col("POSITION_QTY") < 0)),
            lit(True),
        )
        .otherwise(lit(False))
        .alias("IS_CLOSING"),
        current_timestamp().alias("_PROCESSED_AT"),
    )

    # Get row count before write
    row_count = enriched.count()

    if row_count > 0:
        # Write to enriched table
        enriched.write.mode("append").save_as_table(
            "TRADING_LAB_DB.STAGE.TRADES_ENRICHED"
        )

    return f"TRADES_ENRICHED: {row_count} rows processed"


# For local testing (not executed in Snowflake)
if __name__ == "__main__":
    from snowflake.snowpark import Session

    # Create session from config
    session = Session.builder.config("connection_name", "default").create()

    try:
        result = main(session)
        print(result)
    finally:
        session.close()

