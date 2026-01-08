"""
Snowpark stored procedure to enrich trades with position data.
Joins TRADES_RAW with aggregated POSITIONS_RAW on SYMBOL.
Intended to be called by tasks (after-stream).
"""
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sf_sum, avg as sf_avg


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
        mode="overwrite",  # in a real pipeline consider merge/upsert
    )

    return "TRADES_ENRICHED refreshed"

