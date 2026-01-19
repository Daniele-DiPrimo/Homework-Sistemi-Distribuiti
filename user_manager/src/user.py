"""Model `User` con helper per CRUD elementari."""

from extensions import db
from sqlalchemy.exc import IntegrityError
import logging

logger = logging.getLogger(__name__)


class User(db.Model):
    __tablename__ = 'users'
    email = db.Column(db.String(255), primary_key=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    nome = db.Column(db.String(100), nullable=False)
    cognome = db.Column(db.String(150), nullable=False)
    
    @classmethod
    def login(cls, email, password):
        """Restituisce True se l'email e la password corrispondono."""
        user = cls.query.filter_by(email=email).first()
        if user and user.password == password:
            return True
        return False

    @classmethod
    def add_user(cls, email, password, nome, cognome):
        """Aggiunge un nuovo utente. Ritorna True se inserito correttamente."""
        try:
            new_user = cls(email=email, password=password, nome=nome, cognome=cognome)
            db.session.add(new_user)
            db.session.commit()
            logger.info("User successfully inserted")
            return True
        except IntegrityError:
            db.session.rollback()
            logger.warning("Duplicate user (IntegrityError)")
            return False
        except Exception as e:
            db.session.rollback()
            logger.error(f"Unknown SQL error: {e}")
            return False

    @classmethod
    def delete_user(cls, email):
        """Elimina l'utente corrispondente all'email, se presente."""
        try:
            logger.info("Checking and deleting user...")
            user_to_delete = cls.query.filter_by(email=email).first()
            if user_to_delete:
                db.session.delete(user_to_delete)
                db.session.commit()
                logger.info("User deleted from table")
                return True
            else:
                logger.info(f"No user found for email: {email}")
                return False
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error during deletion: {e}")
            return False