from flask import request, jsonify, redirect
from datetime import datetime
from flask_openapi3 import APIBlueprint, Tag
from sqlalchemy import func

from . import handler
from .models_db import Trades, Positions
from .models_io import InputCheck, ReportPosition, ReportConcentration, ReportReconciliation
from .dao import db

tag_file = Tag(name="File Upload", description="Trade and position file upload")
router = APIBlueprint('root', __name__, abp_tags=[tag_file], url_prefix='/')


@router.get('/ingest')
async def ingest_get(target: str | None = None, cpty: str | None = None):
    return redirect('/')


@router.post('/ingest')
async def ingest_post() -> InputCheck:
    target: str = request.form.get('target')
    cpty: str | None = request.form.get('cpty')

    # Read mime attachments or the body as csv
    file = request.files.get('file') or request.body
    out: InputCheck

    # Pass to the correct handler
    if target == 'position':
        out = handler.read_position_yaml(file)
    if target == 'trade':
        out = handler.read_trade_csv(file, cpty)
    return jsonify(out.model_dump())


@router.get('/positions')
async def positions() -> list[ReportPosition]:
    """Positions with cost basis and market value.
    """
    acct = request.args.get('account')
    trade_date: datetime = request.args.get('date')
    if not trade_date:
        trade_date = db.session.query(func.max(Positions.trade_date)).scalar()
    out = Positions.query.filter(Positions.trade_date == trade_date)
    if acct:
        out = out.filter(Positions.account == acct)
    out = handler.report_position(out)
    return jsonify([rep.model_dump() for rep in out])


@router.get('/compliance/concentration')
def compliance() -> list[ReportConcentration]:
    """Accounts exceeding 20% threshold with breach details.
    """
    trade_date: datetime = request.args.get('date')
    out = handler.report_concentration(trade_date)
    return jsonify([rep.model_dump() for rep in out])


@router.get('/reconciliation')
def reconciliation() -> list[ReportReconciliation]:
    """Trade vs position file discrepancies on provided day.
    """
    trade_date: datetime = request.args.get('date')
    out = handler.report_reconciliation(trade_date)
    return jsonify([rep.model_dump() for rep in out])
