"""
Generate sample financial data for the Trading Lab demo.

This module generates realistic trade, market event, and position data
for demonstrating Snowflake ELT patterns in a financial services context.

Usage:
    python generate_data.py [--output-dir ./data-samples]
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Seed for reproducibility
random.seed(42)

# Configuration
SYMBOLS = [
    {"symbol": "AAPL", "sector": "Technology", "asset_class": "EQUITY", "base_price": 185.00},
    {"symbol": "MSFT", "sector": "Technology", "asset_class": "EQUITY", "base_price": 390.00},
    {"symbol": "GOOGL", "sector": "Technology", "asset_class": "EQUITY", "base_price": 142.00},
    {"symbol": "NVDA", "sector": "Technology", "asset_class": "EQUITY", "base_price": 545.00},
    {"symbol": "AMZN", "sector": "Consumer Discretionary", "asset_class": "EQUITY", "base_price": 155.00},
    {"symbol": "META", "sector": "Technology", "asset_class": "EQUITY", "base_price": 380.00},
    {"symbol": "TSLA", "sector": "Consumer Discretionary", "asset_class": "EQUITY", "base_price": 215.00},
    {"symbol": "JPM", "sector": "Financials", "asset_class": "EQUITY", "base_price": 175.00},
    {"symbol": "V", "sector": "Financials", "asset_class": "EQUITY", "base_price": 275.00},
    {"symbol": "UNH", "sector": "Healthcare", "asset_class": "EQUITY", "base_price": 525.00},
    {"symbol": "HD", "sector": "Consumer Discretionary", "asset_class": "EQUITY", "base_price": 355.00},
    {"symbol": "BAC", "sector": "Financials", "asset_class": "EQUITY", "base_price": 33.50},
    {"symbol": "WMT", "sector": "Consumer Staples", "asset_class": "EQUITY", "base_price": 162.00},
    {"symbol": "PG", "sector": "Consumer Staples", "asset_class": "EQUITY", "base_price": 155.00},
    {"symbol": "JNJ", "sector": "Healthcare", "asset_class": "EQUITY", "base_price": 160.00},
]

ACCOUNTS = ["ACCT-001", "ACCT-002", "ACCT-003"]
TRADERS = {"ACCT-001": ["TRD-A1", "TRD-A2"], "ACCT-002": ["TRD-B1", "TRD-B2"], "ACCT-003": ["TRD-C1", "TRD-C2"]}
VENUES = ["NYSE", "NASDAQ", "ARCA", "BATS"]


def generate_trades(
    num_trades: int = 100,
    start_date: datetime = datetime(2024, 1, 15),
    num_days: int = 5,
) -> pd.DataFrame:
    """
    Generate realistic trade execution data.

    Args:
        num_trades: Number of trades to generate
        start_date: Starting date for trades
        num_days: Number of trading days to span

    Returns:
        DataFrame with trade records
    """
    trades = []
    trade_id = 1

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        # Skip weekends
        if current_date.weekday() >= 5:
            continue

        trades_per_day = num_trades // num_days

        for _ in range(trades_per_day):
            symbol_info = random.choice(SYMBOLS)
            account = random.choice(ACCOUNTS)
            trader = random.choice(TRADERS[account])

            # Random time during trading hours (9:30 AM - 4:00 PM)
            hour = random.randint(9, 15)
            minute = random.randint(0, 59) if hour > 9 else random.randint(30, 59)
            second = random.randint(0, 59)

            execution_ts = current_date.replace(
                hour=hour, minute=minute, second=second, microsecond=0
            )

            # Price with small variation from base
            price_variation = random.uniform(-0.02, 0.02)
            price = round(symbol_info["base_price"] * (1 + price_variation), 2)

            # Quantity (round lots mostly)
            quantity = random.choice([10, 25, 50, 75, 100, 150, 200]) + random.randint(0, 5) * 5

            # Venue based on symbol (tech stocks more likely NASDAQ)
            if symbol_info["sector"] == "Technology":
                venue = random.choices(VENUES, weights=[0.2, 0.5, 0.15, 0.15])[0]
            else:
                venue = random.choices(VENUES, weights=[0.5, 0.2, 0.15, 0.15])[0]

            trades.append({
                "trade_id": f"TRD-{trade_id:04d}",
                "symbol": symbol_info["symbol"],
                "side": random.choice(["BUY", "SELL"]),
                "quantity": quantity,
                "price": price,
                "execution_ts": execution_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "account_id": account,
                "venue": venue,
                "trader_id": trader,
                "order_id": f"ORD-{trade_id:04d}",
            })
            trade_id += 1

    return pd.DataFrame(trades)


def generate_market_events(
    start_date: datetime = datetime(2024, 1, 15),
    num_days: int = 5,
) -> list[dict]:
    """
    Generate market events (price updates, dividends, halts).

    Args:
        start_date: Starting date for events
        num_days: Number of trading days

    Returns:
        List of market event dictionaries
    """
    events = []

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        if current_date.weekday() >= 5:
            continue

        # Price updates every 30 minutes for each symbol
        for hour in range(9, 16):
            for minute in [0, 30]:
                if hour == 9 and minute == 0:
                    continue  # Market not open yet

                event_ts = current_date.replace(hour=hour, minute=minute, second=0)

                for symbol_info in SYMBOLS:
                    price_variation = random.uniform(-0.01, 0.01)
                    price = round(symbol_info["base_price"] * (1 + price_variation), 2)
                    spread = round(price * 0.0002, 2)  # ~2 basis point spread

                    events.append({
                        "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "symbol": symbol_info["symbol"],
                        "event_type": "PRICE_UPDATE",
                        "event_data": {
                            "price": price,
                            "volume": random.randint(10000, 500000),
                            "bid": round(price - spread, 2),
                            "ask": round(price + spread, 2),
                        },
                    })

        # Random dividend announcements (1-2 per day)
        for _ in range(random.randint(1, 2)):
            symbol_info = random.choice(SYMBOLS)
            event_ts = current_date.replace(hour=random.randint(10, 14), minute=0, second=0)
            ex_date = current_date + timedelta(days=random.randint(20, 40))

            events.append({
                "event_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "symbol": symbol_info["symbol"],
                "event_type": "DIVIDEND",
                "event_data": {
                    "dividend_amount": round(random.uniform(0.20, 1.50), 2),
                    "ex_date": ex_date.strftime("%Y-%m-%d"),
                    "record_date": (ex_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                    "pay_date": (ex_date + timedelta(days=30)).strftime("%Y-%m-%d"),
                },
            })

    return events


def generate_positions(
    as_of_date: datetime = datetime(2024, 1, 15),
) -> pd.DataFrame:
    """
    Generate end-of-day position snapshots.

    Args:
        as_of_date: Position snapshot date

    Returns:
        DataFrame with position records
    """
    positions = []

    for account in ACCOUNTS:
        # Each account holds 5-10 symbols
        num_holdings = random.randint(5, 10)
        held_symbols = random.sample(SYMBOLS, num_holdings)

        for symbol_info in held_symbols:
            quantity = random.randint(50, 500) * 10  # Round lots
            # Random long/short (90% long)
            if random.random() < 0.1:
                quantity = -quantity

            # Average cost with some historical variation
            cost_variation = random.uniform(-0.10, 0.05)
            avg_cost = round(symbol_info["base_price"] * (1 + cost_variation), 4)

            # Current market value
            current_price = symbol_info["base_price"] * (1 + random.uniform(-0.02, 0.02))
            market_value = round(abs(quantity) * current_price, 2)
            if quantity < 0:
                market_value = -market_value

            positions.append({
                "ACCOUNT_ID": account,
                "SYMBOL": symbol_info["symbol"],
                "QUANTITY": quantity,
                "AVG_COST": avg_cost,
                "MARKET_VALUE": market_value,
                "AS_OF_DATE": as_of_date.strftime("%Y-%m-%d"),
                "SECTOR": symbol_info["sector"],
                "ASSET_CLASS": symbol_info["asset_class"],
            })

    return pd.DataFrame(positions)


def main(output_dir: str = "./data-samples") -> None:
    """
    Generate all sample data files.

    Args:
        output_dir: Directory to write output files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("Generating sample financial data...")

    # Generate trades
    print("  - Generating trades.csv...")
    trades_df = generate_trades(num_trades=100, num_days=5)
    trades_df.to_csv(output_path / "trades.csv", index=False)
    print(f"    Generated {len(trades_df)} trades")

    # Generate market events
    print("  - Generating market_events.json...")
    events = generate_market_events(num_days=5)
    with open(output_path / "market_events.json", "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"    Generated {len(events)} market events")

    # Generate positions
    print("  - Generating positions.parquet...")
    positions_df = generate_positions()
    positions_df.to_parquet(output_path / "positions.parquet", index=False)
    print(f"    Generated {len(positions_df)} position records")

    print(f"\nAll files written to {output_path.absolute()}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate sample financial data")
    parser.add_argument(
        "--output-dir",
        default="./data-samples",
        help="Output directory for generated files",
    )
    args = parser.parse_args()

    main(args.output_dir)

