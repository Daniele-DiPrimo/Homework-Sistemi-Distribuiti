from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError
import os
import time

# ==========================================
# 1. DEFINIZIONE DEL MODELLO (ORM)
# ==========================================
# Invece di scrivere "CREATE TABLE", definiamo una classe Python.
# SQLAlchemy tradurrà questa classe in SQL automaticamente.
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    # Mappiamo le colonne della tabella
    # id = Column(Integer, autoincrement=True)
    email = Column(String(255), primary_key=True, nullable=False)
    nome = Column(String(100), nullable=False)
    cognome = Column(String(150), nullable=False)

    # password = Column(String(255), nullable=False)

# ==========================================
# 2. CLASSE GESTIONE DATABASE
# ==========================================
class UserDB:
    def __init__(self):
        # Leggiamo la configurazione dalle variabili d'ambiente
        self.host = os.getenv('HOST_DB')
        self.user = os.getenv('USER_DB')
        self.password = os.getenv('PASSWORD_DB')
        self.database = os.getenv('NAME_DB')
        self.port = 3306 

        # Creiamo l'URL di connessione per SQLAlchemy (usando il driver pymysql)
        self.db_url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        # Avviamo l'engine con il retry logic iniziale
        self.engine = self._wait_for_db_connection()
        
        # Creiamo la tabella se non esiste (Equivale al tuo __init__db con CREATE TABLE IF NOT EXISTS)
        Base.metadata.create_all(self.engine)
        print("✅ Tabella 'users' pronta (o già esistente).")

        # Creiamo la fabbrica di sessioni
        self.Session = sessionmaker(bind=self.engine)
        print("✅ DB avviato e pronto all'uso.")

    def _wait_for_db_connection(self):
        """Gestisce i retry di connessione all'avvio"""
        retries = 10
        while retries > 0:
            try:
                # pool_pre_ping=True controlla se la connessione è viva prima di usarla
                engine = create_engine(self.db_url, pool_recycle=3600, pool_pre_ping=True)
                # Testiamo la connessione
                with engine.connect() as connection:
                    pass
                print("✅ CONNESSIONE AL DB EFFETTUATA")
                return engine
            except OperationalError as e:
                print(f"⚠️ CONNESSIONE FALLITA: {e}. VADO CON IL RETRY ({retries} rimasti)")
                time.sleep(2)
                retries -= 1
        
        raise Exception("❌ RETRY TERMINATE, IMPOSSIBILE CONNETTERSI AL DATABASE")

    def user_exist(self, email):
        session = self.Session()
        try:
            # SELECT ? FROM users WHERE email = ...
            user = session.query(User).filter_by(email=email).first()
            return user is not None
        finally:
            session.close()

    def add_user(self, email, nome, cognome):
        session = self.Session()
        try:
            # Creiamo l'oggetto Utente
            new_user = User(email=email, nome=nome, cognome=cognome)
            
            # INSERT INTO users ...
            session.add(new_user)            
            session.commit()
            print("✅ UTENTE CORRETTAMENTE INSERITO NELLA TABELLA")
            return True
            
        except IntegrityError:
            session.rollback() # Annulla la transazione
            # QUI IMPLEMENTIAMO LA POLITICA AT-MOST-ONCE:
            # Se l'email esiste già (IntegrityError), ritorniamo False.
            # Il sistema è idempotente: due richieste uguali non creano danni.
            print("⚠️ Utente duplicato (IntegrityError)")
            return False
            
        except Exception as e:
            session.rollback()
            # Gestione eccezione generica
            print(f"❌ Errore sconosciuto SQL: {e}")
            # Come discusso, qui sarebbe meglio fare 'raise e', 
            # ma mantengo 'return False' come nel tuo codice originale.
            return False
            
        finally:
            session.close()

    def delete_user(self, email):
        session = self.Session()
        try:
            # In SQLAlchemy non serve fare prima la SELECT e poi la DELETE.
            # Possiamo provare a cancellare direttamente e vedere quante righe sono state toccate.
            
            print("CONTROLLO ED ELIMINAZIONE UTENTE...")
            
            # DELETE FROM users WHERE email = ...
            rows_deleted = session.query(User).filter_by(email=email).delete()
            session.commit()
            
            if rows_deleted > 0:
                print("✅ UTENTE CORRETTAMENTE ELIMINATO DALLA TABELLA")
                return True
            else:
                # Se rows_deleted è 0, significa che l'utente non esisteva (result is None)
                print(f"⚠️ Nessun utente trovato per email: {email}")
                return False

        except Exception as e:
            session.rollback()
            print(f"❌ Errore sconosciuto SQL: {e}")
            return False
            
        finally:
            session.close()