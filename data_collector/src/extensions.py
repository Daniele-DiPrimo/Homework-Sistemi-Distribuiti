"""Container per estensioni condivise: DB, Scheduler, Kafka producer.

Questo modulo espone oggetti singleton usati dall'applicazione Flask
per evitare chiamate ripetute a `SQLAlchemy()` o `APScheduler()`.
"""

from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
import logging

logging.basicConfig(level=logging.INFO)

# SQLAlchemy DB instance
db = SQLAlchemy()

# Shared Period Task Scheduler 
scheduler = APScheduler()

# Placeholder that will be set at runtime with a Kafka wrapper
kafka_producer = None
