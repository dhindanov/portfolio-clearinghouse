from portfolio.app import app
from portfolio.dao import db


with app.app_context(): 
  db.create_all()
