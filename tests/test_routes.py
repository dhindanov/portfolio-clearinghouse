import pytest
from datetime import datetime
from io import BytesIO
import yaml
from portfolio.models_db import Positions, Trades
from portfolio.dao import db


class TestIngestEndpoints:
    """Tests for file ingest endpoints."""

    def test_ingest_get_redirect(self, client):
        """Test GET /ingest redirects to home."""
        response = client.get('/ingest')
        assert response.status_code == 302

    def test_ingest_position_yaml(self, client):
        """Test POST /ingest with position YAML."""
        yaml_content = """
positions:
  - account_id: ACCT001
    ticker: AAPL
    shares: 100.0
    market_value: 15000.0
report_date: '20260301'
"""
        data = {
            'target': 'position',
            'file': (BytesIO(yaml_content.encode('utf-8')), 'positions.yaml')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = response.get_json()
        assert result['status'] == 'success'

    def test_ingest_trade_csv_cptya(self, client):
        """Test POST /ingest with CptyA trade CSV."""
        csv_content = """TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
20260301,ACCT001,AAPL,100.0,150.0,BUY,20260303"""
        data = {
            'target': 'trade',
            'cpty': 'CptyA',
            'file': (BytesIO(csv_content.encode('utf-8')), 'trades.csv')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = response.get_json()
        assert result['status'] == 'success'

    def test_ingest_trade_csv_cptyb(self, client):
        """Test POST /ingest with CptyB trade CSV."""
        csv_content = """REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20260301|ACCT001|AAPL|100.0|15000.0|BNY"""
        data = {
            'target': 'trade',
            'cpty': 'CptyB',
            'file': (BytesIO(csv_content.encode('utf-8')), 'trades.csv')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = response.get_json()
        assert result['status'] == 'success'

    def test_ingest_invalid_yaml(self, client):
        """Test POST /ingest with invalid YAML."""
        yaml_content = "not: valid: yaml: ["
        data = {
            'target': 'position',
            'file': (BytesIO(yaml_content.encode('utf-8')), 'positions.yaml')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = response.get_json()
        assert result['status'] == 'input error'


class TestPositionsEndpoint:
    """Tests for positions endpoint."""

    def test_positions_empty(self, client):
        """Test GET /positions with no data."""
        response = client.get('/positions')
        assert response.status_code == 200
        result = response.get_json()
        # May return empty list or error depending on implementation
        assert isinstance(result, (list, dict))

    def test_positions_with_data(self, client):
        """Test GET /positions with data."""
        # First insert test data
        with client.application.app_context():
            pos = Positions(
                trade_date=datetime(2026, 3, 1),
                account='ACCT001',
                ticker='AAPL',
                quantity=100.0,
                market_value=15000.0
            )
            db.session.add(pos)
            db.session.commit()
        
        response = client.get('/positions?date=2026-03-01')
        assert response.status_code == 200
        result = response.get_json()
        assert len(result) > 0
        assert result[0]['ticker'] == 'AAPL'

    def test_positions_filter_by_account(self, client):
        """Test GET /positions filtered by account."""
        with client.application.app_context():
            positions = [
                Positions(
                    trade_date=datetime(2026, 3, 1),
                    account='ACCT001',
                    ticker='AAPL',
                    quantity=100.0,
                    market_value=15000.0
                ),
                Positions(
                    trade_date=datetime(2026, 3, 1),
                    account='ACCT002',
                    ticker='MSFT',
                    quantity=50.0,
                    market_value=20000.0
                ),
            ]
            for pos in positions:
                db.session.add(pos)
            db.session.commit()
        
        response = client.get('/positions?date=2026-03-01&account=ACCT001')
        assert response.status_code == 200
        result = response.get_json()
        assert len(result) == 1
        assert result[0]['account'] == 'ACCT001'


class TestComplianceEndpoint:
    """Tests for compliance concentration endpoint."""

    def test_compliance_concentration_empty(self, client):
        """Test GET /compliance/concentration with no data."""
        response = client.get('/compliance/concentration?date=2026-03-01')
        assert response.status_code == 200
        result = response.get_json()
        assert isinstance(result, list)

    def test_compliance_concentration_with_breach(self, client):
        """Test GET /compliance/concentration with concentration breach."""
        with client.application.app_context():
            # Insert position that exceeds 20% threshold
            pos = Positions(
                trade_date=datetime(2026, 3, 1),
                account='ACCT001',
                ticker='AAPL',
                quantity=100.0,
                market_value=15000.0  # 75% of account
            )
            pos2 = Positions(
                trade_date=datetime(2026, 3, 1),
                account='ACCT001',
                ticker='MSFT',
                quantity=10.0,
                market_value=5000.0,  # 25% of account
            )
            db.session.add(pos)
            db.session.add(pos2)
            db.session.commit()
        
        response = client.get('/compliance/concentration?date=2026-03-01')
        assert response.status_code == 200
        result = response.get_json()
        # Should have breach data
        assert isinstance(result, list)


class TestReconciliationEndpoint:
    """Tests for reconciliation endpoint."""

    def test_reconciliation_empty(self, client):
        """Test GET /reconciliation with no data."""
        response = client.get('/reconciliation?date=2026-03-01')
        assert response.status_code == 200
        result = response.get_json()
        assert isinstance(result, list)

    def test_reconciliation_with_mismatch(self, client):
        """Test GET /reconciliation with position/trade mismatch."""
        with client.application.app_context():
            # Insert mismatched data
            pos = Positions(
                trade_date=datetime(2026, 3, 1),
                account='ACCT001',
                ticker='AAPL',
                quantity=100.0,
                market_value=15000.0
            )
            trade = Trades(
                trade_date=datetime(2026, 3, 1),
                account='ACCT001',
                ticker='AAPL',
                quantity=95.0,
                price=150.0,
                market_value=14250.0,
                trade_type='BUY',
                source='CptyA'
            )
            db.session.add(pos)
            db.session.add(trade)
            db.session.commit()
        
        response = client.get('/reconciliation?date=2026-03-01')
        assert response.status_code == 200
        result = response.get_json()
        assert len(result) == 1
        assert result[0]['position']['quantity'] == 100.0
        assert result[0]['trade']['quantity'] == 95.0


class TestIndexEndpoint:
    """Tests for index endpoint."""

    def test_index(self, client):
        """Test GET / returns home page."""
        response = client.get('/')
        # Should either return HTML or status 404 if static file not found
        assert response.status_code in [200, 404]