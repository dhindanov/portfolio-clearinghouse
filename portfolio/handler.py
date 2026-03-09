import csv, yaml
import io
from datetime import datetime
from typing import Any
from pydantic import ValidationError

from .dao import db
from .models_db import Positions, Trades
from .models_io import InputCheck, BasePosition, BaseTradeCptyA, BaseTradeCptyB

# CPTY_MODEL = {
#     'CptyA': BaseTradeCptyA,
#     'CptyB': BaseTradeCptyB,
# }


def read_position_yaml(file: Any) -> InputCheck:
    """Positions are submitted in yaml format and go into 'positions' table."""
    # Parse first attachment or body is yaml
    errors: list[str] = []
    data: list[Any] = []
    try:
        stream = io.StringIO(file.stream.read().decode("utf8"), newline=None)
        incoming = yaml.load(stream, Loader=yaml.CLoader)
        data = incoming.get('positions', [])
        trade_date: datetime = datetime.strptime(incoming.get('report_date'), '%Y%m%d')
    except Exception as err:
        errors.append('invalid yaml input')
        return InputCheck(status='input error', errors=errors)

    # Validate incoming data
    checked: list[BasePosition] = []
    for row in data:
        try:
            checked.append(BasePosition(trade_date=trade_date, **row))
        except Exception as err:
            errors.append(str(err))
    if errors:
        status = 'data validation errors'
        return InputCheck(status=status, count=len(checked), errors=errors)

    # Delete and replace all Positions rows for trade_date
    try:
        Positions.query.filter(Positions.trade_date == trade_date).delete()
        for row in checked:
            out = Positions(**row.model_dump())
            db.session.add(out)
        db.session.commit()
    except Exception as err:
        errors.append(f'database error: {err}')
        status = 'database errors'
        return InputCheck(status=status, count=len(checked), errors=errors)

    return InputCheck(status='success', count=len(checked), errors=errors)


def read_trade_csv(file: Any, cpty: str) -> InputCheck:
    CPTY_MAKER = {
        'CptyA': (BaseTradeCptyA, make_trade_CptyA, ','),
        'CptyB': (BaseTradeCptyB, make_trade_CptyB, '|'),
    }
    read_model, maker, delim = CPTY_MAKER[cpty]

    """Trades come in as csv. File format depends on the counterparty."""
    # Parse first attachment or body is csv
    errors: list[str] = []
    data: list[Any] = []
    try:
        stream = io.StringIO(file.stream.read().decode("utf8"), newline=None)
        data = list(csv.DictReader(stream, delimiter=delim))
    except Exception as err:
        errors.append('invalid csv input')
        return InputCheck(status='input error', errors=errors)

    # Validate and process incoming data
    checked: list[BasePosition] = []
    for row in data:
        try:
            checked.append(read_model(**row))
        except Exception as err:
            errors.append(str(err))
    if errors:
        status = 'data validation errors'
        return InputCheck(status=status, count=len(checked), errors=errors)

    # Delete and replace all Positions rows for trade_date
    to_remove = set(row.trade_date for row in checked)
    try:
        for trade_date in to_remove:
            Trades.query.filter(Trades.source == cpty).filter(Trades.trade_date == trade_date).delete()
        for row in checked:
            out = maker(cpty, row)
            db.session.add(out)
        db.session.commit()
    except Exception as err:
        errors.append(f'database error: {err}')
        status = 'database errors'
        return InputCheck(status=status, count=len(checked), errors=errors)

    return InputCheck(status='success', count=len(checked), errors=errors)


def make_trade_CptyA(cpty: str, read_model: BaseTradeCptyA) -> Trades:
    w = {k: v for k, v in read_model.model_dump().items() if k in Trades.__table__.columns}
    print(w)
    t = Trades(source=cpty, **w)
    t.quantity *= 1 if read_model.trade_type[0] == 'B' else -1
    t.market_value = t.quantity * t.price
    return t


def make_trade_CptyB(cpty: str, read_model: BaseTradeCptyB) -> Trades:
    w = {k: v for k, v in read_model.model_dump().items() if k in Trades.__table__.columns}
    t = Trades(source=cpty, **w)
    t.trade_type = 'BUY' if t.quantity >=0 else 'SELL'
    t.price = abs(t.market_value / t.quantity) if t.quantity else 0
    return t


def csv_from_stream(file: Any):
    stream = io.StringIO(file.stream.read().decode("utf8"), newline=None)
    data = list(csv.DictReader(stream))
    return data
