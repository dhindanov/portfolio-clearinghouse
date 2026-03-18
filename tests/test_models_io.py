import pytest
from datetime import datetime
from pydantic import ValidationError
from portfolio.models_io import (
    BasePosition, BaseTradeCptyA, BaseTradeCptyB, InputCheck,
    ReportPosition, ReportConcentration, ReportConcentrationTicker,
    ReportReconciliation, ReportReconciliationDetail
)


class TestBasePosition:
    """Tests for BasePosition model."""

    def test_valid_position(self):
        """Test creating a valid position."""
        pos = BasePosition(
            trade_date=datetime(2026, 1, 15),
            account_id='ACC001',
            ticker='AAPL',
            shares=100.0,
            market_value=15000.0,
            custodian_ref='BNY'
        )
        assert pos.account == 'ACC001'
        assert pos.quantity == 100.0

    def test_valid_quantity_zero(self):
        """Test that zero quantity is accepted."""
        pos = BasePosition(
            trade_date=datetime(2026, 1, 15),
            account_id='ACC001',
            ticker='AAPL',
            shares=0,
            market_value=15000.0
            )
        assert pos.account == 'ACC001'
        assert pos.quantity == 0.0

    def test_custodian_optional(self):
        """Test that custodian is optional."""
        pos = BasePosition(
            trade_date=datetime(2026, 1, 15),
            account_id='ACC001',
            ticker='AAPL',
            shares=100.0,
            market_value=15000.0
        )
        assert pos.custodian is None


class TestBaseTradeCptyA:
    """Tests for BaseTradeCptyA model."""

    def test_valid_trade_cpty_a(self):
        """Test creating a valid CptyA trade."""
        trade = BaseTradeCptyA(
            TradeDate=datetime(2026, 1, 15),
            AccountID='ACC001',
            Ticker='AAPL',
            Quantity=100.0,
            Price=150.0,
            TradeType='BUY',
            SettlementDate=datetime(2026, 1, 17)
        )
        assert trade.account == 'ACC001'
        assert trade.trade_type == 'BUY'

    def test_price_optional(self):
        """Test that price can be None."""
        trade = BaseTradeCptyA(
            TradeDate=datetime(2026, 1, 15),
            AccountID='ACC001',
            Ticker='AAPL',
            Quantity=100.0,
            Price=None,
            TradeType='BUY',
            SettlementDate=datetime(2026, 1, 17)
        )
        assert trade.price is None


class TestBaseTradeCptyB:
    """Tests for BaseTradeCptyB model."""

    def test_valid_trade_cpty_b(self):
        """Test creating a valid CptyB trade."""
        trade = BaseTradeCptyB(
            REPORT_DATE='20260115',
            ACCOUNT_ID='ACC001',
            SECURITY_TICKER='MSFT',
            SHARES=50.0,
            MARKET_VALUE=15000.0,
            SOURCE_SYSTEM='SYSTEM_B'
        )
        assert trade.account == 'ACC001'
        assert trade.trade_date == datetime(2026, 1, 15)

    def test_date_string_parsing(self):
        """Test that date strings are properly parsed."""
        trade = BaseTradeCptyB(
            REPORT_DATE='20260115',
            ACCOUNT_ID='ACC001',
            SECURITY_TICKER='MSFT',
            SHARES=50.0,
            MARKET_VALUE=15000.0,
            SOURCE_SYSTEM='SYSTEM_B'
        )
        assert isinstance(trade.trade_date, datetime)
        assert trade.trade_date.year == 2026

    def test_invalid_date_format(self):
        """Test that invalid date format is rejected."""
        with pytest.raises(ValidationError):
            BaseTradeCptyB(
                REPORT_DATE='01-15-2026',
                ACCOUNT_ID='ACC001',
                SECURITY_TICKER='MSFT',
                SHARES=50.0,
                MARKET_VALUE=15000.0,
                SOURCE_SYSTEM='SYSTEM_B'
            )

    def test_trade_type_optional(self):
        """Test that trade_type is optional."""
        trade = BaseTradeCptyB(
            REPORT_DATE='20260115',
            ACCOUNT_ID='ACC001',
            SECURITY_TICKER='MSFT',
            SHARES=50.0,
            MARKET_VALUE=15000.0,
            SOURCE_SYSTEM='SYSTEM_B'
        )
        assert trade.trade_type is None


class TestInputCheck:
    """Tests for InputCheck model."""

    def test_success_status(self):
        """Test successful input check."""
        check = InputCheck(status='success', count=10, errors=[])
        assert check.status == 'success'
        assert check.count == 10

    def test_error_status(self):
        """Test error status with messages."""
        errors = ['invalid format', 'missing field']
        check = InputCheck(status='data validation errors', count=0, errors=errors)
        assert len(check.errors) == 2


class TestReportModels:
    """Tests for report output models."""

    def test_report_position(self):
        """Test ReportPosition model."""
        rep = ReportPosition(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=100.0,
            market_value=15000.0
        )
        assert rep.account == 'ACC001'
        assert rep.quantity == 100.0

    def test_report_concentration_ticker(self):
        """Test ReportConcentrationTicker model."""
        ticker = ReportConcentrationTicker(
            ticker='AAPL',
            market_value=15000.0,
            mv_pct=25.5
        )
        assert ticker.mv_pct == 25.5

    def test_report_concentration(self):
        """Test ReportConcentration model."""
        breach = ReportConcentrationTicker(
            ticker='AAPL',
            market_value=15000.0,
            mv_pct=25.5
        )
        rep = ReportConcentration(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            acct_mv=58000.0,
            breach_details=[breach]
        )
        assert len(rep.breach_details) == 1

    def test_report_reconciliation_detail(self):
        """Test ReportReconciliationDetail model."""
        detail = ReportReconciliationDetail(
            quantity=100.0,
            market_value=15000.0
        )
        assert detail.quantity == 100.0

    def test_report_reconciliation(self):
        """Test ReportReconciliation model."""
        pos_detail = ReportReconciliationDetail(quantity=100.0, market_value=15000.0)
        trd_detail = ReportReconciliationDetail(quantity=95.0, market_value=14250.0)

        rep = ReportReconciliation(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            position=pos_detail,
            trade=trd_detail
        )
        assert rep.position.quantity != rep.trade.quantity
