"""Extensions condivise per il microservizio user_manager.

Espone l'istanza `db` di SQLAlchemy usata dall'applicazione.
"""

from flask_sqlalchemy import SQLAlchemy

# Istanza condivisa di SQLAlchemy
db = SQLAlchemy()