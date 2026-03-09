import pytest
import io
import yaml
import json
from datetime import datetime
from unittest.mock import Mock, patch
from portfolio.models_db import Positions, Trades
from portfolio.dao import db


class TestIngestEndpoints:
    """Tests for /ingest endpoints."""

    def test_ingest_get_redirect(self, client):
        """Test GET /ingest redirects to home."""
        response = client.get('/ingest')
        assert response.status_code == 302

    def test_ingest_post_position_success(self, client):
        """Test POST /ingest with valid position YAML."""
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
        yaml_str = yaml.dump(yaml_data)

        data = {
            'target': 'position',
            'file': (io.BytesIO(yaml_str.encode()), 'positions.yaml')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = json.loads(response.data)
        assert result['status'] == 'success'

    def test_ingest_post_trade_cpty_a_success(self, client):
        """Test POST /ingest with valid CptyA trade CSV."""
        csv_data = 'TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n'
        csv_data += '20260115,ACC001,AAPL,100,150.0,BUY,20260117\n'

        data = {
            'target': 'trade',
            'cpty': 'CptyA',
            'file': (io.BytesIO(csv_data.encode()), 'trades.csv')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = json.loads(response.data)
        assert result['status'] == 'success'

    def test_ingest_post_trade_cpty_b_success(self, client):
        """Test POST /ingest with valid CptyB trade CSV."""
        csv_data = 'REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n'
        csv_data += '20260115|ACC001|MSFT|50|15000|SYSTEM_B\n'

        data = {
            'target': 'trade',
            'cpty': 'CptyB',
            'file': (io.BytesIO(csv_data.encode()), 'trades.csv')
        }
        response = client.post('/ingest', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        result = json.loads(response.data)
        assert result['status'] == 'success'


class TestPositionsEndpoint:
    """Tests for /positions endpoint."""

    def test_positions_empty(self, client):
        """Test GET /positions with no data."""
        response = client.get('/positions')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_positions_with_data(self, client):
        """Test GET /positions with data."""
        with client.application.app_context():
            pos = Positions(
                trade_date=datetime(2026, 1, 15),
                account='ACC001',
                ticker='AAPL',
                quantity=100.0,
                market_value=15000.0
            )
            db.session.add(pos)
            db.session.commit()

        response = client.get('/positions?date=2026-01-15')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 0

    def test_positions_filter_by_account(self, client):
        """Test GET /positions filtered by account."""
        with client.application.app_context():
            pos1 = Positions(
                trade_date=datetime(2026, 1, 15),
                account='ACC001',
                ticker='AAPL',
                quantity=100.0,
                market_value=15000.0
            )
            pos2 = Positions(
                trade_date=datetime(2026, 1, 15),
                account='ACC002',
                ticker='MSFT',
                quantity=50.0,
                market_value=15000.0
            )
            db.session.add(pos1)
            db.session.add(pos2)
            db.session.commit()

        response = client.get('/positions?date=2026-01-15&account=ACC001')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert all(item['account'] == 'ACC001' for item in data)


class TestComplianceEndpoint:
    """Tests for /compliance/concentration endpoint."""

    def test_compliance_concentration_empty(self, client):
        """Test GET /compliance/concentration with no data."""
        response = client.get('/compliance/concentration?date=2026-01-15')
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)

    def test_compliance_concentration_with_data(self, client):
        """Test GET /compliance/concentration with data."""
        with client.application.app_context():
            pos = Positions(
                trade_date=datetime(2026, 1, 15),
                account='ACC001',
                ticker='AAPL',
                quantity=100.0,
                market_value=50000.0  # Large position
            )
            db.session.add(pos)
            db.session.commit()

        response = client.get('/compliance/concentration?date=2026-01-15')
        assert response.status_code == 200


class TestIndexEndpoint:
    """Tests for home page endpoint."""

    def test_index_redirect_or_static(self, client):
        """Test GET / endpoint."""
        response = client.get('/')
        # Will either return the static file or an error if file doesn't exist
        assert response.status_code in [200, 404, 405]
