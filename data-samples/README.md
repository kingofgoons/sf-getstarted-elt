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

Output files are timestamped for continuous ingestion simulation:
- `orders_YYYYMMDD_HHMMSS_batch001.csv`
- `events_YYYYMMDD_HHMMSS_batch001.json`
- `inventory_YYYYMMDD_HHMMSS_batch001.parquet`

Upload all to S3 to trigger Snowpipe:
```bash
aws s3 cp orders_*.csv s3://demo-lab-landing/raw/
aws s3 cp events_*.json s3://demo-lab-landing/raw/
aws s3 cp inventory_*.parquet s3://demo-lab-landing/raw/
```

Generate multiple batches to simulate continuous data flow:
```bash
python generate_all.py --batch 2
python generate_all.py --batch 3
```

### Generate Parquet Only
```bash
python generate_parquet.py
```

