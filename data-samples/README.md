# Sample Data Files

Financial services sample data for the Trading Lab ELT demo.

## Files

| File | Format | Description | Records |
|------|--------|-------------|---------|
| `trades.csv` | CSV | Trade execution records | 55 |
| `market_events.json` | JSON (NDJSON) | Market events (prices, dividends, halts) | 54 |
| `positions.parquet` | Parquet | End-of-day position snapshots | 16 |

## Data Schema

### trades.csv
```
trade_id        - Unique trade identifier (TRD-XXXX)
symbol          - Ticker symbol (AAPL, MSFT, etc.)
side            - BUY or SELL
quantity        - Number of shares
price           - Execution price
execution_ts    - ISO 8601 timestamp
account_id      - Trading account (ACCT-001, ACCT-002, ACCT-003)
venue           - Execution venue (NYSE, NASDAQ, ARCA, BATS)
trader_id       - Trader identifier
order_id        - Parent order ID
```

### market_events.json
```json
{
  "event_ts": "2024-01-15T09:30:00Z",
  "symbol": "AAPL",
  "event_type": "PRICE_UPDATE | DIVIDEND | HALT | RESUME",
  "event_data": { ... }  // Varies by event_type
}
```

**Event Types:**
- `PRICE_UPDATE`: price, volume, bid, ask
- `DIVIDEND`: dividend_amount, ex_date, record_date, pay_date
- `HALT`: halt_reason, halt_time, expected_resume
- `RESUME`: resume_time, resume_price

### positions.parquet
```
ACCOUNT_ID    - Trading account identifier
SYMBOL        - Ticker symbol
QUANTITY      - Position quantity (negative = short)
AVG_COST      - Average cost basis per share
MARKET_VALUE  - Current market value
AS_OF_DATE    - Position snapshot date
SECTOR        - Industry sector
ASSET_CLASS   - Asset class (EQUITY)
```

## Upload to S3 (Required)

The demo uses an external S3 stage with your existing `S3_INT` storage integration.

### Step 1: Upload Files to S3

```bash
# Navigate to repo
cd /path/to/sf-getstarted-elt

# Upload to your S3 bucket under finserv-getting-started/ prefix
aws s3 cp data-samples/trades.csv s3://your-bucket/finserv-getting-started/trades.csv
aws s3 cp data-samples/market_events.json s3://your-bucket/finserv-getting-started/market_events.json
aws s3 cp data-samples/positions.parquet s3://your-bucket/finserv-getting-started/positions.parquet

# Verify uploads
aws s3 ls s3://your-bucket/finserv-getting-started/
```

**Expected Output:**
```
2024-01-15 10:00:00       3456 trades.csv
2024-01-15 10:00:01       8901 market_events.json
2024-01-15 10:00:02      12345 positions.parquet
```

### Step 2: Verify in Snowflake

After running `sql/01_stages_formats.sql`, verify the external stage can access your files:

```sql
-- List files in the S3 stage
LIST @TRADING_LAB_DB.RAW.RAW_S3_STAGE;
```

### Step 3: Load Data

The COPY commands in `sql/01_stages_formats.sql` will load data from S3:

```sql
-- These commands are in sql/01_stages_formats.sql
COPY INTO TRADES_RAW FROM @RAW_S3_STAGE/trades.csv ...;
COPY INTO MARKET_EVENTS_RAW FROM @RAW_S3_STAGE/market_events.json ...;
COPY INTO POSITIONS_RAW FROM @RAW_S3_STAGE/positions.parquet ...;
```

## Alternative: Internal Stage (Local Upload)

If you prefer to upload directly without S3:

```sql
-- Upload files via PUT command (requires SnowSQL or Snowsight)
PUT file:///path/to/data-samples/trades.csv @TRADING_LAB_DB.RAW.RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;
PUT file:///path/to/data-samples/market_events.json @TRADING_LAB_DB.RAW.RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;
PUT file:///path/to/data-samples/positions.parquet @TRADING_LAB_DB.RAW.RAW_INTERNAL_STAGE AUTO_COMPRESS=FALSE;

-- Verify uploads
LIST @TRADING_LAB_DB.RAW.RAW_INTERNAL_STAGE;

-- Then modify COPY commands to use @RAW_INTERNAL_STAGE instead of @RAW_S3_STAGE
```

## Regenerating Data

Use the data generation script for larger or custom datasets:

```bash
cd /path/to/sf-getstarted-elt
python python/utils/generate_data.py --output-dir ./data-samples
```

This generates:
- 100 trades across 5 trading days
- Price updates every 30 minutes for all symbols
- Random dividend announcements
- EOD positions for 3 accounts

After regenerating, upload the new files to S3:

```bash
aws s3 sync data-samples/ s3://your-bucket/finserv-getting-started/ --exclude "README.md"
```
