"""
Tests for sample data validation.

Verifies that generated sample data:
- Exists and is readable
- Has correct schema/columns
- Contains valid values
- Meets business rules
"""

import json
from pathlib import Path

import pandas as pd
import pytest


class TestTradesData:
    """Tests for trades.csv sample data."""

    def test_file_exists(self, trades_csv_path: Path) -> None:
        """Verify trades.csv exists."""
        assert trades_csv_path.exists(), f"Missing {trades_csv_path}"

    def test_required_columns(self, trades_df: pd.DataFrame) -> None:
        """Verify all required columns are present."""
        required_columns = {
            "trade_id",
            "symbol",
            "side",
            "quantity",
            "price",
            "execution_ts",
            "account_id",
            "venue",
        }
        actual_columns = set(trades_df.columns)
        missing = required_columns - actual_columns
        assert not missing, f"Missing columns: {missing}"

    def test_no_null_required_fields(self, trades_df: pd.DataFrame) -> None:
        """Verify no nulls in required fields."""
        required_fields = ["trade_id", "symbol", "side", "quantity", "price", "account_id"]
        for field in required_fields:
            null_count = trades_df[field].isnull().sum()
            assert null_count == 0, f"{field} has {null_count} null values"

    def test_trade_id_unique(self, trades_df: pd.DataFrame) -> None:
        """Verify trade_id is unique."""
        assert trades_df["trade_id"].is_unique, "Duplicate trade_id values found"

    def test_side_values(self, trades_df: pd.DataFrame) -> None:
        """Verify side is BUY or SELL."""
        valid_sides = {"BUY", "SELL"}
        actual_sides = set(trades_df["side"].unique())
        invalid = actual_sides - valid_sides
        assert not invalid, f"Invalid side values: {invalid}"

    def test_quantity_positive(self, trades_df: pd.DataFrame) -> None:
        """Verify quantity is positive."""
        assert (trades_df["quantity"] > 0).all(), "Quantity must be positive"

    def test_price_positive(self, trades_df: pd.DataFrame) -> None:
        """Verify price is positive."""
        assert (trades_df["price"] > 0).all(), "Price must be positive"

    def test_valid_symbols(self, trades_df: pd.DataFrame) -> None:
        """Verify symbols are valid tickers."""
        valid_symbols = {
            "AAPL", "MSFT", "GOOGL", "NVDA", "AMZN", "META", "TSLA",
            "JPM", "V", "UNH", "HD", "BAC", "WMT", "PG", "JNJ",
        }
        actual_symbols = set(trades_df["symbol"].unique())
        invalid = actual_symbols - valid_symbols
        assert not invalid, f"Invalid symbols: {invalid}"

    def test_valid_accounts(self, trades_df: pd.DataFrame) -> None:
        """Verify account_id values."""
        valid_accounts = {"ACCT-001", "ACCT-002", "ACCT-003"}
        actual_accounts = set(trades_df["account_id"].unique())
        invalid = actual_accounts - valid_accounts
        assert not invalid, f"Invalid accounts: {invalid}"

    def test_minimum_row_count(self, trades_df: pd.DataFrame) -> None:
        """Verify minimum number of trades."""
        assert len(trades_df) >= 50, f"Expected at least 50 trades, got {len(trades_df)}"


class TestMarketEventsData:
    """Tests for market_events.json sample data."""

    def test_file_exists(self, market_events_json_path: Path) -> None:
        """Verify market_events.json exists."""
        assert market_events_json_path.exists(), f"Missing {market_events_json_path}"

    def test_valid_json_lines(self, market_events_json_path: Path) -> None:
        """Verify file is valid NDJSON."""
        with open(market_events_json_path) as f:
            for i, line in enumerate(f, 1):
                if line.strip():
                    try:
                        json.loads(line)
                    except json.JSONDecodeError as e:
                        pytest.fail(f"Invalid JSON on line {i}: {e}")

    def test_required_fields(self, market_events_list: list[dict]) -> None:
        """Verify all events have required fields."""
        required_fields = {"event_ts", "symbol", "event_type", "event_data"}
        for i, event in enumerate(market_events_list):
            missing = required_fields - set(event.keys())
            assert not missing, f"Event {i} missing fields: {missing}"

    def test_valid_event_types(self, market_events_list: list[dict]) -> None:
        """Verify event_type is valid."""
        valid_types = {"PRICE_UPDATE", "DIVIDEND", "HALT", "RESUME", "SPLIT"}
        for event in market_events_list:
            assert event["event_type"] in valid_types, \
                f"Invalid event_type: {event['event_type']}"

    def test_price_update_has_price(self, market_events_list: list[dict]) -> None:
        """Verify PRICE_UPDATE events have price in event_data."""
        for event in market_events_list:
            if event["event_type"] == "PRICE_UPDATE":
                assert "price" in event["event_data"], \
                    "PRICE_UPDATE missing price field"
                assert event["event_data"]["price"] > 0, \
                    "Price must be positive"

    def test_dividend_has_amount(self, market_events_list: list[dict]) -> None:
        """Verify DIVIDEND events have dividend_amount."""
        for event in market_events_list:
            if event["event_type"] == "DIVIDEND":
                assert "dividend_amount" in event["event_data"], \
                    "DIVIDEND missing dividend_amount"

    def test_minimum_event_count(self, market_events_list: list[dict]) -> None:
        """Verify minimum number of events."""
        assert len(market_events_list) >= 30, \
            f"Expected at least 30 events, got {len(market_events_list)}"


class TestPositionsData:
    """Tests for positions.parquet sample data."""

    def test_file_exists(self, positions_parquet_path: Path) -> None:
        """Verify positions.parquet exists."""
        assert positions_parquet_path.exists(), f"Missing {positions_parquet_path}"

    def test_readable_parquet(self, positions_parquet_path: Path) -> None:
        """Verify file is valid Parquet."""
        try:
            pd.read_parquet(positions_parquet_path)
        except Exception as e:
            pytest.fail(f"Cannot read Parquet: {e}")

    def test_required_columns(self, positions_df: pd.DataFrame) -> None:
        """Verify all required columns are present."""
        required_columns = {
            "ACCOUNT_ID",
            "SYMBOL",
            "QUANTITY",
            "AVG_COST",
            "MARKET_VALUE",
            "AS_OF_DATE",
        }
        actual_columns = set(positions_df.columns)
        missing = required_columns - actual_columns
        assert not missing, f"Missing columns: {missing}"

    def test_no_null_required_fields(self, positions_df: pd.DataFrame) -> None:
        """Verify no nulls in required fields."""
        required_fields = ["ACCOUNT_ID", "SYMBOL", "QUANTITY", "AVG_COST"]
        for field in required_fields:
            null_count = positions_df[field].isnull().sum()
            assert null_count == 0, f"{field} has {null_count} null values"

    def test_avg_cost_positive(self, positions_df: pd.DataFrame) -> None:
        """Verify avg_cost is positive."""
        assert (positions_df["AVG_COST"] > 0).all(), "AVG_COST must be positive"

    def test_unique_account_symbol(self, positions_df: pd.DataFrame) -> None:
        """Verify account_id + symbol is unique."""
        duplicates = positions_df.duplicated(subset=["ACCOUNT_ID", "SYMBOL"])
        assert not duplicates.any(), "Duplicate account/symbol pairs found"

    def test_short_positions_allowed(self, positions_df: pd.DataFrame) -> None:
        """Verify short positions (negative quantity) exist and are valid."""
        short_positions = positions_df[positions_df["QUANTITY"] < 0]
        # Short positions should have negative market value
        if len(short_positions) > 0:
            assert (short_positions["MARKET_VALUE"] < 0).all(), \
                "Short positions should have negative market value"

    def test_minimum_position_count(self, positions_df: pd.DataFrame) -> None:
        """Verify minimum number of positions."""
        assert len(positions_df) >= 10, \
            f"Expected at least 10 positions, got {len(positions_df)}"

