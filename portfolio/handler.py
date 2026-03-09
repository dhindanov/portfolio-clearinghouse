import csv, yaml
import io
from collections import defaultdict
from datetime import datetime, date
from typing import Any, Iterator
from sqlalchemy import text
from flask.json.provider import DefaultJSONProvider

from .dao import db
from .models_db import Positions, Trades
from .models_io import (
    InputCheck, BasePosition, BaseTradeCptyA, BaseTradeCptyB, ReportPosition, ReportConcentrationTicker, ReportConcentration,
    ReportReconciliationDetail, ReportReconciliation,
)


class ISODateEncoder(DefaultJSONProvider):
    """Encode dates to isoformat."""
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)


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
    """Load a trade file to the database. The file has two possible formats, with different subsets of columns.
    Loading is handled by replacing all entries for a trade_date.
    """
    # Configuration and parsing methodology depends on file source
    CPTY_MAKER = {
        'CptyA': (BaseTradeCptyA, make_trade_CptyA, ','),
        'CptyB': (BaseTradeCptyB, make_trade_CptyB, '|'),
    }
    read_model, maker, delim = CPTY_MAKER[cpty]

    # Parse first attachment or body is csv
    errors: list[str] = []
    data: list[Any] = []
    try:
        stream = io.StringIO(file.stream.read().decode("utf8"), newline=None)
        data = list(csv.DictReader(stream, delimiter=delim))
    except Exception as err:
        errors.append(f'invalid csv input {err}')
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
            Trades.query.filter(Trades.trade_date == trade_date).delete()
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
    t = Trades(source=cpty, **w)
    t.quantity *= 1 if read_model.trade_type[0] == 'B' else -1
    t.market_value = t.quantity * t.price
    return t


def make_trade_CptyB(cpty: str, read_model: BaseTradeCptyB) -> Trades:
    w = {k: v for k, v in read_model.model_dump().items() if k in Trades.__table__.columns}
    t = Trades(source=cpty, **w)
    t.trade_type = 'BUY' if t.quantity >= 0 else 'SELL'
    t.price = abs(t.market_value / t.quantity) if t.quantity else 0
    return t


# Report outputs

def report_position(query: Iterator[Positions]) -> list[ReportPosition]:
    """Positions with cost basis and market value.
    """
    out: list[ReportPosition] = []
    for row in query:
        rep = ReportPosition(
            trade_date=row.trade_date,
            account=row.account,
            ticker=row.ticker,
            quantity=row.quantity,
            market_value=row.market_value,
        )
        out.append(rep)
    return out


def report_concentration(trade_date: datetime) -> list[ReportConcentration]:
    """Accounts exceeding 20% threshold with breach details.
    """
    sql: str = text("""with
    agg as (
        select trade_date, account, ticker, market_value, sum(abs(market_value)) over (partition by trade_date, account) as acct_mv
        from positions where trade_date = :dt
    ),
    ext as (select agg.*, round(100 * abs(market_value) / acct_mv, 2) as mv_pct from agg)
    select * from ext where mv_pct > 20
    """)
    cursor = db.session.execute(sql, {'dt': trade_date})

    # Pull the data and group by trade_date, account
    agg = defaultdict(list)
    for row in cursor:
        breach = ReportConcentrationTicker(
            ticker=row.ticker,
            market_value=row.market_value,
            mv_pct=row.mv_pct,
        )
        key = (row.trade_date, row.account, row.acct_mv)
        agg[key].append(breach)

    # Build nested output objects
    out: list[ReportConcentration] = []
    for key, breach_lst in sorted(agg.items()):
        rep = ReportConcentration(
            trade_date=key[0],
            account=key[1],
            acct_mv=key[2],
            breach_details=breach_lst
        )
        out.append(rep)
    return out


def report_reconciliation(trade_date: datetime) -> list[ReportReconciliation]:
    """Trade vs position file discrepancies on provided day.
    For lack of data on several days, we stub out a comparison between trades and positions.
    """
    # A data structure to aggregate and compare quantities from positions and trades
    agg = defaultdict(dict)
    compare_on = ['quantity', 'market_value']

    # Pull the data and group by trade_date, account, ticker
    for tbl in ('positions', 'trades'):
        sql: str = text(f"select * from {tbl} where trade_date = :dt")
        cursor = db.session.execute(sql, {'dt': trade_date})
        for row in cursor:
            key = (row.trade_date, row.account, row.ticker)
            side = agg[key].setdefault(tbl, {})
            for q in compare_on:
                side[q] = side.get(q, 0) + getattr(row, q)

    # Select keys that differ on any comparison items
    out: list[ReportReconciliation] = []
    for key, data in sorted(agg.items()):
        same = True
        pos, trd = data.get('positions', {}), data.get('trades', {})
        for q in compare_on:
            if pos.get(q, 0) != trd.get(q, 0):
                same = False
        if not same:
            failed = ReportReconciliation(
                trade_date=key[0],
                account=key[1],
                ticker=key[2],
                position=ReportReconciliationDetail(
                    quantity=pos.get('quantity'),
                    market_value=pos.get('market_value'),
                ),
                trade=ReportReconciliationDetail(
                    quantity=trd.get('quantity'),
                    market_value=trd.get('market_value'),
                ),
            )
            out.append(failed)
    return out
