import os
from datetime import datetime, date
from flask_openapi3 import OpenAPI, Info
from a2wsgi import WSGIMiddleware

from .routes import router
from .dao import db
from .handler import ISODateEncoder


app = OpenAPI(__name__, info=Info(title='Portfolio API', version='1.0.0'))
app.json = ISODateEncoder(app)  # output dates in isoformat
app.url_map.strict_slashes = False
app.static_folder = '../static'  # for demo
app.register_api(router, url='/', url_prefix='/')
asgi_app = WSGIMiddleware(app)  # for uvicorn

# Connect Mysql
creds = [os.getenv(k) for k in ('MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_HOST', 'MYSQL_PORT', 'MYSQL_DATABASE')]
app.config['SQLALCHEMY_DATABASE_STRICT_TYPES'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://{0}:{1}@{2}:{3}/{4}'.format(*creds)
db.init_app(app)


# Demo page
@app.get('/')
def index():
    return app.send_static_file('index.html')
