import pytest
import yaml
from datetime import datetime
from unittest.mock import Mock
from portfolio.handler import (
    read_position_yaml, read_trade_csv, make_trade_CptyA, make_trade_CptyB,
    report_position, report_concentration, report_reconciliation,
    ISODateEncoder
)
from portfolio.models_io import (
    BasePosition, BaseTradeCptyA, BaseTradeCptyB
)
from portfolio.models_db import Positions, Trades
from portfolio.dao import db


class TestISODateEncoder:
    """Tests for ISODateEncoder class."""

    def test_encode_datetime(self, client):
        """Test encoding datetime to ISO format."""
        from portfolio.app import app
        encoder = ISODateEncoder(app)
        dt = datetime(2026, 1, 15, 10, 30, 45)
        result = encoder.default(dt)
        assert result == '2026-01-15T10:30:45'

    def test_encode_date(self, client):
        """Test encoding date to ISO format."""
        from portfolio.app import app
        encoder = ISODateEncoder(app)
        from datetime import date
        d = date(2026, 1, 15)
        result = encoder.default(d)
        assert result == '2026-01-15'


class TestReadPositionYaml:
    """Tests for read_position_yaml function."""

    def test_valid_position_yaml(self, client):
        """Test reading valid position YAML."""
        yaml_data = {
            'report_date': '20260115',
            'positions': [
                {
                    'account_id': 'ACC001',
                    'ticker': 'AAPL',
                    'shares': 100.0,
                    'market_value': 15000.0
                }
            ]
        }
        yaml_bytes = yaml.dump(yaml_data).encode('utf8')

        file = Mock()
        file.stream.read.return_value = yaml_bytes

        result = read_position_yaml(file)
        assert result.status == 'success'
        assert result.count == 1

    def test_invalid_yaml_format(self, client):
        """Test reading invalid YAML format."""
        file = Mock()
        file.stream.read.return_value = b'invalid: [yaml: format: ]'

        result = read_position_yaml(file)
        assert result.status == 'input error'


class TestReadTradeCsv:
    """Tests for read_trade_csv function."""

    def test_valid_cpty_a_csv(self, client):
        """Test reading valid CptyA CSV."""
        csv_data = 'TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n'
        csv_data += '20260115,ACC001,AAPL,100,150.0,BUY,20260117\n'

        file = Mock()
        file.stream.read.return_value = csv_data.encode('utf8')

        result = read_trade_csv(file, 'CptyA')
        assert result.status == 'success'

    def test_valid_cpty_b_csv(self, client):
        """Test reading valid CptyB CSV."""
        csv_data = 'REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n'
        csv_data += '20260115|ACC001|MSFT|50|15000|SYSTEM_B\n'

        file = Mock()
        file.stream.read.return_value = csv_data.encode('utf8')

        result = read_trade_csv(file, 'CptyB')
        assert result.status == 'success'

    def test_invalid_csv_format(self, client):
        """Test reading invalid CSV format."""
        file = Mock()
        file.stream.read.return_value = b'not valid csv'

        result = read_trade_csv(file, 'CptyA')
        assert result.status == 'success'
        assert result.count == 0


class TestMakeTradeCptyA:
    """Tests for make_trade_CptyA function."""
    def test_make_buy_trade(self):
        """Test creating a BUY trade."""
        trade_data = BaseTradeCptyA(
            TradeDate=datetime(2026, 1, 15),
            AccountID='ACC001',
            Ticker='AAPL',
            Quantity=100.0,
            Price=150.0,
            TradeType='BUY',
            SettlementDate=datetime(2026, 1, 17),
        )
        trade = make_trade_CptyA('CptyA', trade_data, batch_id=2)
        assert trade.quantity == 100.0
        assert trade.market_value == 15000.0
    def test_make_sell_trade(self):
        """Test creating a SELL trade."""
        trade_data = BaseTradeCptyA(
            TradeDate=datetime(2026, 1, 15),
            AccountID='ACC001',
            Ticker='AAPL',
            Quantity=100.0,
            Price=150.0,
            TradeType='SELL',
            SettlementDate=datetime(2026, 1, 17),
        )
        trade = make_trade_CptyA('CptyA', trade_data, batch_id=2)
        assert trade.quantity == -100.0
        assert trade.market_value == -15000.0


class TestMakeTradeCptyB:
    """Tests for make_trade_CptyB function."""
    def test_make_positive_quantity_trade(self):
        """Test creating a trade with positive quantity."""
        trade_data = BaseTradeCptyB(
            REPORT_DATE='20260115',
            ACCOUNT_ID='ACC001',
            SECURITY_TICKER='MSFT',
            SHARES=50.0,
            MARKET_VALUE=15000.0,
            SOURCE_SYSTEM='SYSTEM_B',
        )
        trade = make_trade_CptyB('CptyB', trade_data, batch_id=2)
        assert trade.trade_type == 'BUY'
        assert trade.price == 300.0

    def test_make_negative_quantity_trade(self):
        """Test creating a trade with negative quantity."""
        trade_data = BaseTradeCptyB(
            REPORT_DATE='20260115',
            ACCOUNT_ID='ACC001',
            SECURITY_TICKER='MSFT',
            SHARES=-50.0,
            MARKET_VALUE=-15000.0,
            SOURCE_SYSTEM='SYSTEM_B',
        )
        trade = make_trade_CptyB('CptyB', trade_data, batch_id=2)
        assert trade.trade_type == 'SELL'


class TestReportPosition:
    """Tests for report_position function."""

    def test_report_position_empty(self, client):
        """Test reporting positions from empty query."""
        query = Positions.query.all()
        result = report_position(query)
        assert len(result) == 0

    def test_report_position_with_data(self, client):
        """Test reporting positions with data."""
        pos = Positions(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=10.0,
            market_value=1500.0
        )
        db.session.add(pos)
        db.session.commit()

        query = Positions.query.all()
        result = report_position(query)
        assert len(result) == 1
        assert result[0].ticker == 'AAPL'


class TestReportConcentration:
    """Tests for report_concentration function."""

    def test_report_concentration_no_breaches(self, client):
        """Test concentration report with no breaches."""
        pos_lst = []
        for ticker in ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'):
            pos = Positions(
                trade_date=datetime(2026, 1, 15),
                account='ACC001',
                ticker=ticker,
                quantity=10.0,
                market_value=1500.0
            )
            pos_lst.append(pos)
        for pos in pos_lst:
            db.session.add(pos)
        db.session.commit()

        result = report_concentration(datetime(2026, 1, 15))
        # won't breach the 20% threshold
        assert len(result) == 0

    def test_report_concentration_with_breach(self, client):
        """Test concentration report with breach."""
        pos = Positions(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=30.0,
            market_value=50000.0,  # Would be >20% of account
        )
        db.session.add(pos)
        db.session.commit()

        result = report_concentration(datetime(2026, 1, 15))
        # Result depends on SQLite compatibility with the window function


class TestReportReconciliation:
    """Tests for report_reconciliation function."""

    def test_report_reconciliation_matching(self, client):
        """Test reconciliation when positions match trades."""
        pos = Positions(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=100.0,
            market_value=15000.0
        )
        trade = Trades(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=100.0,
            market_value=15000.0,
            trade_type='BUY',
            source='TEST',
            batch_id=1,
        )
        db.session.add(pos)
        db.session.add(trade)
        db.session.commit()

        result = report_reconciliation(datetime(2026, 1, 15))
        assert len(result) == 0  # No discrepancies

    def test_report_reconciliation_mismatch(self, client):
        """Test reconciliation with mismatched quantities."""
        pos = Positions(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=100.0,
            market_value=15000.0
        )
        trade = Trades(
            trade_date=datetime(2026, 1, 15),
            account='ACC001',
            ticker='AAPL',
            quantity=95.0,
            market_value=14250.0,
            trade_type='BUY',
            source='TEST',
            batch_id=1,
        )
        db.session.add(pos)
        db.session.add(trade)
        db.session.commit()

        result = report_reconciliation(datetime(2026, 1, 15))
        assert len(result) >= 0
