#!/usr/bin/env python3
"""
Generate connected sample data for Snowpipe ingestion demo.
Financial Services / Hedge Fund theme: trades, market events, positions.

Usage:
    pip install pandas pyarrow
    python generate_all.py [--batch N]

Files are created with timestamps to simulate continuous data landing.
"""

import pandas as pd
import json
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Shared reference data for connected records (Financial Services theme)
ACCOUNTS = [f"ACCT-{i:04d}" for i in range(1, 51)]  # Fund/client accounts
SYMBOLS = [
    # Equities
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "JNJ",
    "WMT", "PG", "UNH", "HD", "MA", "BAC", "XOM", "PFE", "ABBV", "KO",
    # ETFs
    "SPY", "QQQ", "IWM", "VTI", "AGG", "BND", "GLD", "TLT", "XLF", "XLE",
    # Bonds (simplified tickers)
    "T-BILL-3M", "T-NOTE-2Y", "T-NOTE-10Y", "T-BOND-30Y",
]
EXCHANGES = ["NYSE", "NASDAQ", "ARCA", "BATS", "IEX"]
TRADE_SIDES = ["BUY", "SELL"]
TRADE_STATUSES = ["pending", "filled", "partial_fill", "cancelled", "rejected"]
EVENT_TYPES = [
    "price_alert", "order_placed", "order_filled", "order_cancelled",
    "dividend_announced", "earnings_release", "rebalance_triggered",
    "margin_call", "settlement_complete", "corporate_action"
]


def generate_trades(num_rows: int, batch_id: str) -> pd.DataFrame:
    """Generate trade order records (CSV format)."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 24))
    
    data = []
    for i in range(num_rows):
        trade_id = f"TRD-{batch_id}-{i:04d}"
        symbol = random.choice(SYMBOLS)
        side = random.choice(TRADE_SIDES)
        quantity = random.randint(10, 10000)
        # Price varies by asset type
        if symbol.startswith("T-"):  # Bonds
            price = round(random.uniform(95.0, 105.0), 4)
        elif symbol in ["SPY", "QQQ", "IWM", "VTI"]:  # ETFs
            price = round(random.uniform(200.0, 500.0), 2)
        else:  # Equities
            price = round(random.uniform(50.0, 800.0), 2)
        
        data.append({
            "TRADE_ID": trade_id,
            "ACCOUNT_ID": random.choice(ACCOUNTS),
            "SYMBOL": symbol,
            "TRADE_TS": (base_time + timedelta(seconds=random.randint(0, 3600))).strftime("%Y-%m-%d %H:%M:%S"),
            "SIDE": side,
            "QUANTITY": quantity,
            "PRICE": price,
            "AMOUNT": round(quantity * price, 2),
            "EXCHANGE": random.choice(EXCHANGES),
            "STATUS": random.choice(TRADE_STATUSES)
        })
    
    return pd.DataFrame(data)


def generate_market_events(num_rows: int, batch_id: str, trade_ids: list, symbols_traded: list) -> list:
    """Generate market/trading event records (JSON format) linked to trades and symbols."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 24))
    
    events = []
    for i in range(num_rows):
        event_type = random.choice(EVENT_TYPES)
        symbol = random.choice(symbols_traded) if symbols_traded else random.choice(SYMBOLS)
        
        event = {
            "EVENT_TS": (base_time + timedelta(seconds=random.randint(0, 3600))).strftime("%Y-%m-%d %H:%M:%S"),
            "SYMBOL": symbol,
            "EVENT_TYPE": event_type,
            "EVENT_ATTR": {
                "source": random.choice(["market_data", "trading_system", "risk_engine", "compliance"]),
                "priority": random.choice(["low", "medium", "high", "critical"])
            }
        }
        
        # Add event-specific attributes
        if event_type == "price_alert":
            event["EVENT_ATTR"]["alert_type"] = random.choice(["above_threshold", "below_threshold", "volatility_spike"])
            event["EVENT_ATTR"]["threshold_price"] = round(random.uniform(100.0, 500.0), 2)
            event["EVENT_ATTR"]["current_price"] = round(random.uniform(100.0, 500.0), 2)
        elif event_type in ["order_placed", "order_filled", "order_cancelled"]:
            event["EVENT_ATTR"]["trade_id"] = random.choice(trade_ids) if trade_ids else f"TRD-{batch_id}-0000"
            event["EVENT_ATTR"]["account_id"] = random.choice(ACCOUNTS)
        elif event_type == "dividend_announced":
            event["EVENT_ATTR"]["dividend_amount"] = round(random.uniform(0.10, 2.50), 2)
            event["EVENT_ATTR"]["ex_date"] = (datetime.now() + timedelta(days=random.randint(7, 30))).strftime("%Y-%m-%d")
        elif event_type == "earnings_release":
            event["EVENT_ATTR"]["eps_actual"] = round(random.uniform(0.50, 5.00), 2)
            event["EVENT_ATTR"]["eps_estimate"] = round(random.uniform(0.50, 5.00), 2)
            event["EVENT_ATTR"]["surprise_pct"] = round(random.uniform(-20.0, 20.0), 2)
        elif event_type == "margin_call":
            event["EVENT_ATTR"]["account_id"] = random.choice(ACCOUNTS)
            event["EVENT_ATTR"]["required_amount"] = round(random.uniform(10000, 500000), 2)
        
        events.append(event)
    
    return events


def generate_positions(num_rows: int, batch_id: str, symbols_traded: list) -> pd.DataFrame:
    """Generate position/holdings records (Parquet format) for symbols."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 12))
    
    data = []
    used_combinations = set()
    
    # Prioritize symbols that were traded
    symbol_pool = symbols_traded + SYMBOLS if symbols_traded else SYMBOLS
    
    for _ in range(num_rows):
        while True:
            symbol = random.choice(symbol_pool)
            account_id = random.choice(ACCOUNTS)
            combo = (symbol, account_id)
            if combo not in used_combinations:
                used_combinations.add(combo)
                break
        
        quantity = random.randint(100, 50000)
        # Price varies by asset type
        if symbol.startswith("T-"):
            avg_cost = round(random.uniform(95.0, 105.0), 4)
            current_price = round(avg_cost * random.uniform(0.98, 1.02), 4)
        elif symbol in ["SPY", "QQQ", "IWM", "VTI"]:
            avg_cost = round(random.uniform(200.0, 500.0), 2)
            current_price = round(avg_cost * random.uniform(0.95, 1.10), 2)
        else:
            avg_cost = round(random.uniform(50.0, 800.0), 2)
            current_price = round(avg_cost * random.uniform(0.90, 1.15), 2)
        
        data.append({
            "SYMBOL": symbol,
            "ACCOUNT_ID": account_id,
            "QUANTITY": quantity,
            "AVG_COST": avg_cost,
            "COST_BASIS": round(quantity * avg_cost, 2),
            "CURRENT_PRICE": current_price,
            "MARKET_VALUE": round(quantity * current_price, 2),
            "UNREALIZED_PNL": round(quantity * (current_price - avg_cost), 2),
            "UPDATED_AT": (base_time + timedelta(minutes=random.randint(0, 120))).strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="Generate sample data for Snowpipe demo (FinServ theme)")
    parser.add_argument("--batch", type=int, default=1, help="Batch number (affects filenames and IDs)")
    parser.add_argument("--trades", type=int, default=20, help="Number of trade records")
    parser.add_argument("--events", type=int, default=50, help="Number of event records")
    parser.add_argument("--positions", type=int, default=30, help="Number of position records")
    parser.add_argument("--output-dir", type=str, default="generated", help="Output directory (default: generated/)")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_id = f"{args.batch:03d}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate trades (CSV)
    trades_df = generate_trades(args.trades, batch_id)
    trades_file = output_dir / f"trades_{timestamp}_batch{batch_id}.csv"
    trades_df.to_csv(trades_file, index=False)
    print(f"Generated {trades_file} with {len(trades_df)} records")
    
    # Collect trade IDs and symbols for linked data
    trade_ids = trades_df["TRADE_ID"].tolist()
    symbols_traded = trades_df["SYMBOL"].unique().tolist()
    
    # Generate market events (JSON) - linked to trades and symbols
    events = generate_market_events(args.events, batch_id, trade_ids, symbols_traded)
    events_file = output_dir / f"events_{timestamp}_batch{batch_id}.json"
    with open(events_file, "w") as f:
        json.dump(events, f, indent=2)
    print(f"Generated {events_file} with {len(events)} records")
    
    # Generate positions (Parquet) - includes traded symbols
    positions_df = generate_positions(args.positions, batch_id, symbols_traded)
    positions_file = output_dir / f"positions_{timestamp}_batch{batch_id}.parquet"
    positions_df.to_parquet(positions_file, index=False, engine="pyarrow")
    print(f"Generated {positions_file} with {len(positions_df)} records")
    
    print(f"\n--- Upload to S3 ---")
    print(f"aws s3 cp {trades_file} s3://demo-lab-landing/raw/")
    print(f"aws s3 cp {events_file} s3://demo-lab-landing/raw/")
    print(f"aws s3 cp {positions_file} s3://demo-lab-landing/raw/")


if __name__ == "__main__":
    main()
