#!/usr/bin/env python3
"""Generate sample positions.parquet for Snowflake demo lab (FinServ theme)."""

import pandas as pd
from datetime import datetime, timedelta
import random

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "SPY", "QQQ"]
ACCOUNTS = [f"ACCT-{i:04d}" for i in [8, 12, 23, 45]]

def generate_positions_data(num_rows: int = 20) -> pd.DataFrame:
    """Generate sample position records matching POSITIONS_RAW schema."""
    base_time = datetime.now() - timedelta(hours=2)
    
    data = []
    used_combos = set()
    
    for _ in range(num_rows):
        while True:
            symbol = random.choice(SYMBOLS)
            account_id = random.choice(ACCOUNTS)
            combo = (symbol, account_id)
            if combo not in used_combos:
                used_combos.add(combo)
                break
        
        quantity = random.randint(100, 5000)
        avg_cost = round(random.uniform(100, 500), 2)
        current_price = round(avg_cost * random.uniform(0.95, 1.10), 2)
        
        data.append({
            "SYMBOL": symbol,
            "ACCOUNT_ID": account_id,
            "QUANTITY": quantity,
            "AVG_COST": avg_cost,
            "COST_BASIS": round(quantity * avg_cost, 2),
            "CURRENT_PRICE": current_price,
            "MARKET_VALUE": round(quantity * current_price, 2),
            "UNREALIZED_PNL": round(quantity * (current_price - avg_cost), 2),
            "UPDATED_AT": (base_time + timedelta(minutes=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_positions_data(20)
    output_path = "positions.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"Generated {output_path} with {len(df)} rows")
    print(df.head())
