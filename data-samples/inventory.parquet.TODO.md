Parquet sample placeholder.

Generate with Python (example):
```python
import pandas as pd
df = pd.DataFrame([
    {"SKU": "1001", "WAREHOUSE": "W1", "QTY": 50, "UPDATED_AT": "2024-01-01T09:59:00Z"},
    {"SKU": "1002", "WAREHOUSE": "W1", "QTY": 20, "UPDATED_AT": "2024-01-01T10:01:00Z"},
    {"SKU": "1003", "WAREHOUSE": "W2", "QTY": 0, "UPDATED_AT": "2024-01-01T10:02:00Z"},
])
df.to_parquet("data-samples/inventory.parquet", index=False)
```
Upload:
- Internal: `PUT file://./data-samples/inventory.parquet @raw_stage;`
- S3: `aws s3 cp data-samples/inventory.parquet s3://mlp-demo-landing/raw/inventory.parquet`

