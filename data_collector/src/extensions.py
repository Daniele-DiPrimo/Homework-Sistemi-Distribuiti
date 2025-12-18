from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
import logging

logging.basicConfig(level=logging.INFO)

db = SQLAlchemy()
scheduler = APScheduler()
kafka_producer = None
