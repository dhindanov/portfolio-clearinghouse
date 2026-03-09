import pytest


@pytest.fixture
def client(scope='session'):
    """Create a test client with an in-memory SQLite database."""
    from flask_openapi3 import OpenAPI, Info
    from portfolio.routes import router
    from portfolio.dao import db
    from portfolio.handler import ISODateEncoder

    app = OpenAPI(__name__, info=Info(title='Portfolio API', version='1.0.0'))
    app.json = ISODateEncoder(app)  # output dates in isoformat
    app.url_map.strict_slashes = False
    app.static_folder = '../static'  # for demo
    app.register_api(router, url='/', url_prefix='/')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)

    with app.app_context():
        db.create_all()
        yield app.test_client()
        db.session.remove()
        db.drop_all()
