import os
import asyncio
from datetime import datetime
from flask import request, jsonify, render_template_string
from flask_openapi3 import OpenAPI, Info, Tag
from a2wsgi import ASGIMiddleware, WSGIMiddleware
# from asgiref.wsgi import WsgiToAsgi
from .routes import router
from .dao import db

app = OpenAPI(__name__, info=Info(title='Portfolio API', version='1.0.0'))
app.url_map.strict_slashes = False
app.static_folder = '../static'
app.register_api(router, url='/', url_prefix='/')
asgi_app = WSGIMiddleware(app)

creds = [os.getenv(k) for k in ('MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_DATABASE')]
app.config['SQLALCHEMY_DATABASE_STRICT_TYPES'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{0}:{1}@{2}:{3}/{4}'.format(*creds)
db.init_app(app)


@app.get('/')
def index():
    return app.send_static_file('index.html')
