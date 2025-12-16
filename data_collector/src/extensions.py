from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
import logging

db = SQLAlchemy()
scheduler = APScheduler()

logging.basicConfig(level=logging.INFO)