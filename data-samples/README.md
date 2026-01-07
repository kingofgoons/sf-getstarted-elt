Sample files for ingestion demos.

- Upload to internal stage: `PUT file://./data-samples/orders.csv @raw_stage;`
- Upload to S3: `aws s3 cp data-samples/orders.csv s3://demo-lab-landing/raw/orders.csv`

Files:
- `orders.csv` (structured)
- `events.json` (semi-structured)
- `inventory.parquet` (to be generated; see note)

To generate Parquet (example):
- Use Python/pandas locally to write `inventory.parquet` or export from a dataframe; then upload to S3 or internal stage.

