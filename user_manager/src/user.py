from extensions import db
from sqlalchemy.exc import IntegrityError

class User(db.Model):
    __tablename__ = 'users'

    email = db.Column(db.String(255), primary_key=True, nullable=False)
    nome = db.Column(db.String(100), nullable=False)
    cognome = db.Column(db.String(150), nullable=False)

    @classmethod
    def user_exist(cls, email):
        return cls.query.filter_by(email=email).first() is not None

    @classmethod
    def add_user(cls, email, nome, cognome):
        try:
            # Creiamo l'oggetto (istanza della classe corrente)
            new_user = cls(email=email, nome=nome, cognome=cognome)
            
            db.session.add(new_user)
            db.session.commit()
            
            print("UTENTE CORRETTAMENTE INSERITO NELLA TABELLA")
            return True

        except IntegrityError:
            db.session.rollback()
            print("Utente duplicato (IntegrityError)")
            return False

        except Exception as e:
            db.session.rollback()
            print(f"Errore sconosciuto SQL: {e}")
            return False

    @classmethod
    def delete_user(cls, email):
        try:
            print("CONTROLLO ED ELIMINAZIONE UTENTE...")
            
            user_to_delete = cls.query.filter_by(email=email).first()
            
            if user_to_delete:
                db.session.delete(user_to_delete)
                db.session.commit()
                print("UTENTE CORRETTAMENTE ELIMINATO DALLA TABELLA")
                return True
            else:
                print(f"Nessun utente trovato per email: {email}")
                return False

        except Exception as e:
            db.session.rollback()
            print(f"Errore durante l'eliminazione: {e}")
            return False