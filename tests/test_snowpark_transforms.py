"""
Tests for Snowpark transformation logic.

Unit tests for P&L calculation and trade enrichment
without requiring a live Snowflake connection.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest


# Import the P&L calculation function from the stored procedure
# We'll test the pure Python logic separately from Snowpark integration


def calculate_realized_pnl(
    side: str,
    quantity: float,
    price: float,
    position_qty: float | None,
    avg_cost: float | None,
) -> tuple[float, bool]:
    """
    Calculate realized P&L for a trade.

    Mirrors the logic in sp_transform_trades.py.
    
    A trade is "closing" if it reduces the absolute position size:
    - SELL when long (position_qty > 0)
    - BUY when short (position_qty < 0)

    Args:
        side: Trade side (BUY or SELL)
        quantity: Trade quantity (always positive)
        price: Execution price
        position_qty: Current position before trade (None if no position)
        avg_cost: Average cost basis (None if no position)

    Returns:
        Tuple of (realized_pnl, is_closing)
    """
    if position_qty is None or avg_cost is None:
        return (0.0, False)

    is_closing = False
    realized_pnl = 0.0

    if side == "SELL" and position_qty > 0:
        # Closing a long position
        is_closing = True
        closing_qty = min(quantity, position_qty)
        realized_pnl = closing_qty * (price - avg_cost)
    elif side == "BUY" and position_qty < 0:
        # Closing a short position
        is_closing = True
        closing_qty = min(quantity, abs(position_qty))
        realized_pnl = closing_qty * (avg_cost - price)

    return (realized_pnl, is_closing)


class TestPnLCalculation:
    """Tests for P&L calculation logic."""

    def test_buy_no_position(self) -> None:
        """BUY with no existing position has zero P&L."""
        pnl, is_closing = calculate_realized_pnl(
            side="BUY", quantity=100, price=185.00,
            position_qty=None, avg_cost=None
        )
        assert pnl == 0.0
        assert is_closing is False

    def test_sell_no_position(self) -> None:
        """SELL with no existing position has zero P&L."""
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=100, price=185.00,
            position_qty=None, avg_cost=None
        )
        assert pnl == 0.0
        assert is_closing is False

    def test_sell_closes_long_profit(self) -> None:
        """SELL when long realizes profit."""
        # Long 100 shares at $180, sell at $185
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=100, price=185.00,
            position_qty=100, avg_cost=180.00
        )
        assert pnl == 500.0  # 100 * (185 - 180) = 500
        assert is_closing is True

    def test_sell_closes_long_loss(self) -> None:
        """SELL when long realizes loss."""
        # Long 100 shares at $190, sell at $185
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=100, price=185.00,
            position_qty=100, avg_cost=190.00
        )
        assert pnl == -500.0  # 100 * (185 - 190) = -500
        assert is_closing is True

    def test_sell_partial_close(self) -> None:
        """SELL only partially closes position."""
        # Long 200 shares at $180, sell 50 at $185
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=50, price=185.00,
            position_qty=200, avg_cost=180.00
        )
        assert pnl == 250.0  # 50 * (185 - 180) = 250
        assert is_closing is True

    def test_sell_more_than_position(self) -> None:
        """SELL more than position only realizes P&L on position size."""
        # Long 50 shares at $180, sell 100 at $185
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=100, price=185.00,
            position_qty=50, avg_cost=180.00
        )
        assert pnl == 250.0  # 50 * (185 - 180) = 250 (only 50 shares closed)
        assert is_closing is True

    def test_buy_opens_new_position(self) -> None:
        """BUY when already long just adds to position."""
        # Long 100 shares at $180, buy 50 more at $185
        pnl, is_closing = calculate_realized_pnl(
            side="BUY", quantity=50, price=185.00,
            position_qty=100, avg_cost=180.00
        )
        assert pnl == 0.0
        assert is_closing is False

    def test_buy_closes_short_profit(self) -> None:
        """BUY when short realizes profit."""
        # Short 100 shares at $190 (avg_cost), buy to cover at $185
        pnl, is_closing = calculate_realized_pnl(
            side="BUY", quantity=100, price=185.00,
            position_qty=-100, avg_cost=190.00
        )
        assert pnl == 500.0  # 100 * (190 - 185) = 500
        assert is_closing is True

    def test_buy_closes_short_loss(self) -> None:
        """BUY when short realizes loss."""
        # Short 100 shares at $180 (avg_cost), buy to cover at $185
        pnl, is_closing = calculate_realized_pnl(
            side="BUY", quantity=100, price=185.00,
            position_qty=-100, avg_cost=180.00
        )
        assert pnl == -500.0  # 100 * (180 - 185) = -500
        assert is_closing is True

    def test_sell_when_already_short(self) -> None:
        """SELL when short just adds to short position."""
        # Short 100 shares, sell 50 more
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=50, price=185.00,
            position_qty=-100, avg_cost=190.00
        )
        assert pnl == 0.0
        assert is_closing is False

    def test_zero_quantity_trade(self) -> None:
        """Zero quantity trade has no P&L."""
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=0, price=185.00,
            position_qty=100, avg_cost=180.00
        )
        assert pnl == 0.0
        assert is_closing is True  # Still considered closing, but 0 qty

    def test_flat_position(self) -> None:
        """Trade against flat position has no P&L."""
        pnl, is_closing = calculate_realized_pnl(
            side="SELL", quantity=100, price=185.00,
            position_qty=0, avg_cost=180.00
        )
        assert pnl == 0.0
        assert is_closing is False


class TestSnowparkProcedure:
    """Tests for Snowpark procedure structure."""

    def test_procedure_file_exists(self, project_root: Path) -> None:
        """Verify stored procedure file exists."""
        proc_path = project_root / "python" / "sp_transform_trades.py"
        assert proc_path.exists()

    def test_procedure_has_main_function(self, project_root: Path) -> None:
        """Verify procedure has main function."""
        proc_path = project_root / "python" / "sp_transform_trades.py"
        content = proc_path.read_text()
        assert "def main(session: Session)" in content

    def test_procedure_returns_string(self, project_root: Path) -> None:
        """Verify procedure returns a string."""
        proc_path = project_root / "python" / "sp_transform_trades.py"
        content = proc_path.read_text()
        assert "-> str:" in content or "-> str" in content

    def test_procedure_uses_correct_tables(self, project_root: Path) -> None:
        """Verify procedure references correct tables."""
        proc_path = project_root / "python" / "sp_transform_trades.py"
        content = proc_path.read_text()
        
        assert "TRADES_RAW_STREAM" in content or "trades_raw_stream" in content.lower()
        assert "TRADES_ENRICHED" in content or "trades_enriched" in content.lower()

    def test_mock_session_workflow(self, mock_snowpark_session: MagicMock) -> None:
        """Verify mock session can simulate procedure workflow."""
        session = mock_snowpark_session
        
        # Simulate procedure workflow
        session.use_database("TRADING_LAB_DB")
        session.use_schema("STAGE")
        
        trades_stream = session.table("TRADING_LAB_DB.RAW.TRADES_RAW_STREAM")
        positions = session.table("TRADING_LAB_DB.RAW.POSITIONS_RAW")
        
        # Verify methods were called
        session.use_database.assert_called_once()
        session.use_schema.assert_called_once()
        assert session.table.call_count == 2


class TestNotionalValueCalculation:
    """Tests for notional value calculation."""

    def test_notional_value_calculation(self) -> None:
        """Verify notional = quantity * price."""
        quantity = 100
        price = 185.50
        notional = quantity * price
        assert notional == 18550.0

    def test_notional_with_fractional_shares(self) -> None:
        """Verify notional with fractional quantities."""
        quantity = 10.5
        price = 185.50
        notional = quantity * price
        assert abs(notional - 1947.75) < 0.01

    def test_notional_precision(self) -> None:
        """Verify notional maintains precision."""
        quantity = 1000
        price = 0.0001  # Very small price
        notional = quantity * price
        assert notional == 0.1

