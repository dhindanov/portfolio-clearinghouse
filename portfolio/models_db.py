from .dao import db


class Trades(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.DateTime, nullable=False)
    account = db.Column(db.Text, nullable=False)
    ticker = db.Column(db.Text, nullable=False)
    quantity = db.Column(db.Numeric, nullable=False)
    price = db.Column(db.Numeric, nullable=True)
    trade_type = db.Column(db.Text, nullable=False)
    market_value = db.Column(db.Numeric, nullable=True)
    custodian = db.Column(db.Text, nullable=True)
    source = db.Column(db.Text, nullable=False)
    batch_id = db.Column(db.Numeric, nullable=False)


class Positions(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.DateTime, nullable=False)
    account = db.Column(db.Text, nullable=False)
    ticker = db.Column(db.Text, nullable=False)
    quantity = db.Column(db.Numeric, nullable=False)
    market_value = db.Column(db.Numeric, nullable=True)
    custodian = db.Column(db.Text, nullable=True)
