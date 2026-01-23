"""Extensions condivise per il microservizio user_manager.

Espone l'istanza `db` di SQLAlchemy usata dall'applicazione.
"""

from flask_sqlalchemy import SQLAlchemy
import logging

logging.basicConfig(level=logging.INFO)

# Istanza condivisa di SQLAlchemy
db = SQLAlchemy()