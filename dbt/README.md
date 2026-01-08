# Trading Lab Analytics - DBT Project

DBT models for financial services analytics, transforming curated trade and position data into reporting-ready facts and dimensions.

## Model Lineage

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCES                                  │
│  ┌─────────────────┐    ┌─────────────────────┐                 │
│  │ TRADE_METRICS   │    │  POSITION_SUMMARY   │                 │
│  │ (CURATED)       │    │  (CURATED)          │                 │
│  └────────┬────────┘    └──────────┬──────────┘                 │
└───────────┼─────────────────────────┼───────────────────────────┘
            │                         │
            ▼                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                        STAGING (views)                           │
│  ┌─────────────────┐    ┌─────────────────────┐                 │
│  │  stg_trades     │    │   stg_positions     │                 │
│  └────────┬────────┘    └──────────┬──────────┘                 │
└───────────┼─────────────────────────┼───────────────────────────┘
            │                         │
            └───────────┬─────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INTERMEDIATE (ephemeral)                      │
│              ┌─────────────────────────┐                        │
│              │  int_trade_positions    │                        │
│              └────────────┬────────────┘                        │
└───────────────────────────┼─────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                        MARTS (tables)                            │
│  ┌──────────────┐  ┌───────────────┐  ┌────────────────────┐   │
│  │ fct_daily_pnl│  │dim_instruments│  │ fct_account_summary│   │
│  └──────────────┘  └───────────────┘  └────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Models

### Staging Layer (`models/staging/`)
| Model | Description |
|-------|-------------|
| `stg_trades` | Standardized trade metrics with computed fields |
| `stg_positions` | Position data with classification and return calculations |

### Intermediate Layer (`models/intermediate/`)
| Model | Description |
|-------|-------------|
| `int_trade_positions` | Joins trades with current positions for P&L context |

### Marts Layer (`models/marts/`)
| Model | Description |
|-------|-------------|
| `fct_daily_pnl` | Daily P&L by account/symbol (realized + unrealized) |
| `dim_instruments` | Instrument reference with trading statistics |
| `fct_account_summary` | Account-level portfolio metrics |

## Setup

### 1. Install Dependencies
```bash
cd dbt
pip install -r requirements.txt
```

### 2. Configure Profile
```bash
# Rename the example profile (stays in this directory, won't overwrite ~/.dbt/profiles.yml)
mv profiles.yml.example profiles.yml

# Point DBT to use this directory for profiles
export DBT_PROFILES_DIR=$(pwd)

# Set your Snowflake credentials as environment variables
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
```

### 3. Verify Connection
```bash
dbt debug
```

## Usage

### Run All Models
```bash
dbt run
```

### Run Specific Models
```bash
# Run staging models only
dbt run --select staging

# Run marts only
dbt run --select marts

# Run a specific model and its dependencies
dbt run --select +fct_daily_pnl
```

### Run Tests
```bash
dbt test
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

## Testing

The project includes:
- **Schema tests**: Not null, unique, accepted values
- **Source freshness**: Validates data is recent

Run tests:
```bash
# All tests
dbt test

# Source tests only
dbt test --select source:*

# Model tests only  
dbt test --select fct_daily_pnl
```

## Development

### Adding New Models

1. Create SQL file in appropriate layer directory
2. Add model config at top of file:
```sql
{{
    config(
        materialized='table',
        schema='analytics'
    )
}}
```

3. Add schema tests in `_schema.yml`
4. Run `dbt run --select new_model`
5. Run `dbt test --select new_model`

### Code Style

This project follows SQLFluff for SQL linting:
```bash
sqlfluff lint models/
sqlfluff fix models/
```

