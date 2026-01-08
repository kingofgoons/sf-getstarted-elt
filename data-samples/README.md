# Sample Data for Snowflake Demo Lab

Financial Services / Hedge Fund theme: trades, market events, positions.

## Static Sample Files
- `trades.csv` (structured) - sample trade orders
- `events.json` (semi-structured) - market events
- `positions.parquet` (to be generated via `generate_parquet.py`)

Upload to internal stage:
```bash
PUT file://./data-samples/trades.csv @raw_stage;
```

Upload to S3:
```bash
aws s3 cp data-samples/trades.csv s3://demo-lab-landing/raw/
```

## Data Generators

### Generate All (Connected Data for Snowpipe Demo)
Creates trades, market events, and positions with connected/related data:

```bash
pip install pandas pyarrow
cd data-samples
python generate_all.py --batch 1 --trades 20 --events 50 --positions 30
```

Options:
- `--batch N` — Batch number (affects filenames and record IDs)
- `--trades N` — Number of trade records (default: 20)
- `--events N` — Number of event records (default: 50)
- `--positions N` — Number of position records (default: 30)
- `--output-dir DIR` — Output directory (default: `generated/`)

Output files are written to `generated/` by default:
- `generated/trades_YYYYMMDD_HHMMSS_batch001.csv`
- `generated/events_YYYYMMDD_HHMMSS_batch001.json`
- `generated/positions_YYYYMMDD_HHMMSS_batch001.parquet`

Upload all to S3 to trigger Snowpipe:
```bash
aws s3 cp generated/trades_*.csv s3://demo-lab-landing/raw/
aws s3 cp generated/events_*.json s3://demo-lab-landing/raw/
aws s3 cp generated/positions_*.parquet s3://demo-lab-landing/raw/
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
