"""
Snowpark stored procedure to transform RAW tables into STAGE.ORDERS_ENRICHED.
Intended to be called by tasks (after-stream).
"""
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_timestamp


def main(session: Session) -> str:
    session.use_database("DEMO_LAB_DB")
    session.use_schema("STAGE")

    orders_raw = session.table("DEMO_LAB_DB.RAW.ORDERS_RAW")
    inventory = session.table("DEMO_LAB_DB.RAW.INVENTORY_RAW")

    enriched = (
        orders_raw.join(inventory, orders_raw["ORDER_ID"] == inventory["SKU"], how="left")
        .with_column("ORDER_TS", to_timestamp(col("ORDER_TS")))
        .with_column("UPDATED_AT", to_timestamp(col("UPDATED_AT")))
    )

    enriched.write.save_as_table(
        "ORDERS_ENRICHED",
        mode="overwrite",  # in a real pipeline consider merge/upsert
    )

    return "ORDERS_ENRICHED refreshed"

