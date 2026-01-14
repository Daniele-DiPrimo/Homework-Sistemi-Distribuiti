"""Container per estensioni condivise: DB, Scheduler, Kafka producer.

Questo modulo espone oggetti singleton usati dall'applicazione Flask
per evitare chiamate ripetute a `SQLAlchemy()` o `APScheduler()`.
"""

from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
import logging

logging.basicConfig(level=logging.INFO)

# Istanza SQLAlchemy condivisa
db = SQLAlchemy()

# Scheduler per task periodici
scheduler = APScheduler()

# Placeholder che verrà impostato a runtime con un wrapper Kafka
kafka_producer = None
