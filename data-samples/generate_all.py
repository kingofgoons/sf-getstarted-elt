#!/usr/bin/env python3
"""
Generate connected sample data for Snowpipe ingestion demo.
Creates orders (.csv), events (.json), and inventory (.parquet) with related data.

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

# Shared reference data for connected records
CUSTOMERS = [f"CUST-{i:04d}" for i in range(1, 51)]
SKUS = [f"SKU-{i:04d}" for i in range(1, 101)]
WAREHOUSES = ["EAST-01", "WEST-01", "CENTRAL-01", "SOUTH-01"]
EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "purchase", "search", "wishlist_add"]
STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]


def generate_orders(num_rows: int, batch_id: str) -> pd.DataFrame:
    """Generate order records (CSV format)."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 24))
    
    data = []
    for i in range(num_rows):
        order_id = f"ORD-{batch_id}-{i:04d}"
        data.append({
            "ORDER_ID": order_id,
            "CUSTOMER_ID": random.choice(CUSTOMERS),
            "ORDER_TS": (base_time + timedelta(minutes=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S"),
            "AMOUNT": round(random.uniform(10.0, 500.0), 2),
            "STATUS": random.choice(STATUSES)
        })
    
    return pd.DataFrame(data)


def generate_events(num_rows: int, batch_id: str, order_ids: list) -> list:
    """Generate event records (JSON format) linked to customers and orders."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 24))
    
    events = []
    for i in range(num_rows):
        event_type = random.choice(EVENT_TYPES)
        event = {
            "EVENT_TS": (base_time + timedelta(seconds=random.randint(0, 3600))).strftime("%Y-%m-%d %H:%M:%S"),
            "USER_ID": random.choice(CUSTOMERS),
            "EVENT_TYPE": event_type,
            "EVENT_ATTR": {
                "session_id": f"sess-{batch_id}-{random.randint(1000, 9999)}",
                "device": random.choice(["mobile", "desktop", "tablet"]),
                "page": random.choice(["/home", "/products", "/cart", "/checkout", "/search"])
            }
        }
        # Link purchase events to actual orders
        if event_type == "purchase" and order_ids:
            event["EVENT_ATTR"]["order_id"] = random.choice(order_ids)
            event["EVENT_ATTR"]["sku"] = random.choice(SKUS)
        elif event_type in ["add_to_cart", "wishlist_add"]:
            event["EVENT_ATTR"]["sku"] = random.choice(SKUS)
            event["EVENT_ATTR"]["quantity"] = random.randint(1, 5)
        
        events.append(event)
    
    return events


def generate_inventory(num_rows: int, batch_id: str) -> pd.DataFrame:
    """Generate inventory records (Parquet format) for SKUs."""
    base_time = datetime.now() - timedelta(hours=random.randint(1, 12))
    
    data = []
    used_combinations = set()
    
    for _ in range(num_rows):
        # Ensure unique SKU+WAREHOUSE combinations per batch
        while True:
            sku = random.choice(SKUS)
            warehouse = random.choice(WAREHOUSES)
            combo = (sku, warehouse)
            if combo not in used_combinations:
                used_combinations.add(combo)
                break
        
        data.append({
            "SKU": sku,
            "WAREHOUSE": warehouse,
            "QTY": random.randint(0, 500),
            "UPDATED_AT": (base_time + timedelta(minutes=random.randint(0, 120))).strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="Generate sample data for Snowpipe demo")
    parser.add_argument("--batch", type=int, default=1, help="Batch number (affects filenames and IDs)")
    parser.add_argument("--orders", type=int, default=20, help="Number of order records")
    parser.add_argument("--events", type=int, default=50, help="Number of event records")
    parser.add_argument("--inventory", type=int, default=30, help="Number of inventory records")
    parser.add_argument("--output-dir", type=str, default=".", help="Output directory")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_id = f"{args.batch:03d}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate orders (CSV)
    orders_df = generate_orders(args.orders, batch_id)
    orders_file = output_dir / f"orders_{timestamp}_batch{batch_id}.csv"
    orders_df.to_csv(orders_file, index=False)
    print(f"Generated {orders_file} with {len(orders_df)} records")
    
    # Generate events (JSON) - linked to orders
    order_ids = orders_df["ORDER_ID"].tolist()
    events = generate_events(args.events, batch_id, order_ids)
    events_file = output_dir / f"events_{timestamp}_batch{batch_id}.json"
    with open(events_file, "w") as f:
        json.dump(events, f, indent=2)
    print(f"Generated {events_file} with {len(events)} records")
    
    # Generate inventory (Parquet)
    inventory_df = generate_inventory(args.inventory, batch_id)
    inventory_file = output_dir / f"inventory_{timestamp}_batch{batch_id}.parquet"
    inventory_df.to_parquet(inventory_file, index=False, engine="pyarrow")
    print(f"Generated {inventory_file} with {len(inventory_df)} records")
    
    print(f"\n--- Upload to S3 ---")
    print(f"aws s3 cp {orders_file} s3://demo-lab-landing/raw/")
    print(f"aws s3 cp {events_file} s3://demo-lab-landing/raw/")
    print(f"aws s3 cp {inventory_file} s3://demo-lab-landing/raw/")


if __name__ == "__main__":
    main()

