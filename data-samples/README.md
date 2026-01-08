Sample files for ingestion demos.

## Static Sample Files
- `orders.csv` (structured) - basic order data
- `events.json` (semi-structured) - clickstream events
- `inventory.parquet` (to be generated via `generate_parquet.py`)

Upload to internal stage:
```bash
PUT file://./data-samples/orders.csv @raw_stage;
```

Upload to S3:
```bash
aws s3 cp data-samples/orders.csv s3://demo-lab-landing/raw/
```

## Data Generators

### Generate All (Connected Data for Snowpipe Demo)
Creates orders, events, and inventory with connected/related data:

```bash
pip install pandas pyarrow
cd data-samples
python generate_all.py --batch 1 --orders 20 --events 50 --inventory 30
```

Options:
- `--batch N` — Batch number (affects filenames and record IDs)
- `--orders N` — Number of order records (default: 20)
- `--events N` — Number of event records (default: 50)
- `--inventory N` — Number of inventory records (default: 30)
- `--output-dir DIR` — Output directory (default: `generated/`)

Output files are written to `generated/` by default:
- `generated/orders_YYYYMMDD_HHMMSS_batch001.csv`
- `generated/events_YYYYMMDD_HHMMSS_batch001.json`
- `generated/inventory_YYYYMMDD_HHMMSS_batch001.parquet`

Upload all to S3 to trigger Snowpipe:
```bash
aws s3 cp generated/orders_*.csv s3://demo-lab-landing/raw/
aws s3 cp generated/events_*.json s3://demo-lab-landing/raw/
aws s3 cp generated/inventory_*.parquet s3://demo-lab-landing/raw/
```

Generate multiple batches to simulate continuous data flow:
```bash
python generate_all.py --batch 2
python generate_all.py --batch 3
```

Custom output directory:
```bash
python generate_all.py --batch 1 --output-dir /tmp/snowpipe-test
```

### Generate Parquet Only
```bash
python generate_parquet.py
```

