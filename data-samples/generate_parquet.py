#!/usr/bin/env python3
"""Generate sample inventory.parquet for Snowflake demo lab."""

import pandas as pd
from datetime import datetime, timedelta
import random

def generate_inventory_data(num_rows: int = 50) -> pd.DataFrame:
    """Generate sample inventory records matching INVENTORY_RAW schema."""
    warehouses = ["EAST-01", "WEST-01", "CENTRAL-01", "SOUTH-01"]
    sku_prefixes = ["SKU", "PROD", "ITEM"]
    
    data = []
    base_time = datetime.now() - timedelta(days=7)
    
    for i in range(num_rows):
        sku = f"{random.choice(sku_prefixes)}-{1000 + i}"
        warehouse = random.choice(warehouses)
        qty = random.randint(0, 500)
        updated_at = base_time + timedelta(hours=random.randint(0, 168))
        data.append({
            "SKU": sku,
            "WAREHOUSE": warehouse,
            "QTY": qty,
            "UPDATED_AT": updated_at
        })
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_inventory_data(50)
    output_path = "inventory.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"Generated {output_path} with {len(df)} rows")
    print(df.head())

