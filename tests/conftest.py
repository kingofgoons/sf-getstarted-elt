"""
Pytest fixtures for Trading Lab tests.

Provides reusable fixtures for:
- Sample data paths
- Mock Snowpark sessions
- Test data generation
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def data_samples_dir(project_root: Path) -> Path:
    """Return the data-samples directory."""
    return project_root / "data-samples"


@pytest.fixture
def sql_dir(project_root: Path) -> Path:
    """Return the sql directory."""
    return project_root / "sql"


@pytest.fixture
def trades_csv_path(data_samples_dir: Path) -> Path:
    """Return path to trades.csv."""
    return data_samples_dir / "trades.csv"


@pytest.fixture
def market_events_json_path(data_samples_dir: Path) -> Path:
    """Return path to market_events.json."""
    return data_samples_dir / "market_events.json"


@pytest.fixture
def positions_parquet_path(data_samples_dir: Path) -> Path:
    """Return path to positions.parquet."""
    return data_samples_dir / "positions.parquet"


@pytest.fixture
def trades_df(trades_csv_path: Path) -> pd.DataFrame:
    """Load trades CSV as DataFrame."""
    return pd.read_csv(trades_csv_path)


@pytest.fixture
def market_events_list(market_events_json_path: Path) -> list[dict]:
    """Load market events JSON as list of dicts."""
    events = []
    with open(market_events_json_path) as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))
    return events


@pytest.fixture
def positions_df(positions_parquet_path: Path) -> pd.DataFrame:
    """Load positions Parquet as DataFrame."""
    return pd.read_parquet(positions_parquet_path)


@pytest.fixture
def mock_snowpark_session() -> MagicMock:
    """
    Create a mock Snowpark session for unit testing.
    
    This mock provides basic DataFrame-like behavior without
    requiring an actual Snowflake connection.
    """
    session = MagicMock()
    
    # Mock database/schema context
    session.use_database = MagicMock()
    session.use_schema = MagicMock()
    
    # Mock table method to return a mock DataFrame
    mock_df = MagicMock()
    mock_df.count.return_value = 0
    mock_df.join.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.with_column.return_value = mock_df
    mock_df.write.mode.return_value.save_as_table = MagicMock()
    
    session.table.return_value = mock_df
    
    return session


@pytest.fixture
def sample_trade_data() -> list[dict]:
    """Generate sample trade data for testing."""
    return [
        {
            "trade_id": "TRD-001",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 100,
            "price": 185.25,
            "execution_ts": "2024-01-15T09:30:15Z",
            "account_id": "ACCT-001",
            "venue": "NYSE",
        },
        {
            "trade_id": "TRD-002",
            "symbol": "AAPL",
            "side": "SELL",
            "quantity": 50,
            "price": 186.50,
            "execution_ts": "2024-01-15T10:30:15Z",
            "account_id": "ACCT-001",
            "venue": "NYSE",
        },
    ]


@pytest.fixture
def sample_position_data() -> list[dict]:
    """Generate sample position data for testing."""
    return [
        {
            "account_id": "ACCT-001",
            "symbol": "AAPL",
            "quantity": 500,
            "avg_cost": 178.50,
            "market_value": 93125.00,
        },
        {
            "account_id": "ACCT-001",
            "symbol": "MSFT",
            "quantity": -100,  # Short position
            "avg_cost": 390.00,
            "market_value": -39500.00,
        },
    ]

