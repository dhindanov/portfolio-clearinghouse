import csv, io
# from flask import Blueprint
from flask import request, jsonify, redirect
from datetime import datetime
from flask_openapi3 import APIBlueprint, OpenAPI, Info, Tag
from pydantic import ValidationError

from . import handler
from .forms import upload_form
from .models_db import Trades, Positions
from .models_io import InputCheck
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
async def positions():
    users = Trades.query.all()
    return jsonify([{"id": u.id, "username": u.username} for u in users])


@router.get('/compliance')
def compliance(id):
    user = Trades.query.get_or_404(id)
    return jsonify({"id": user.id, "username": user.username})


@router.get('/reconciliation')
def reconciliation(id):
    user = Trades.query.get_or_404(id)
    return jsonify([{"title": p.title} for p in user.posts])
