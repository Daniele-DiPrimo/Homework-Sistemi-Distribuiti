import mysql.connector
import time
import os

class UserDB: 

    def __init__(self): #attraverso la funzione __init__ creiamo la tabella all'avvio se non esiste. 
        #leggiamo tutta la configurazione delle variabili d'ambiente e salviamo i campi in variabili. In questo modo avremo un'istanza del nostro DB
        self.host = os.getenv('DB_HOST', 'localhost')
        self.user = os.getenv('DB_USER', 'user_service')
        self.password = os.getenv('DB_PASSWORD', '12345')
        self.database = os.getenv('DB_NAME', 'user_db')
        
        self.__init__db()

    
    def _get_connection(self): #in questa funzione faremo la connessione al database,
        
        retries = 5 # numero di retries nel caso in cui non riusciamo a collegarci al db

        while retries > 0: 
            try: 
                conn = mysql.connector.connect(
                    host = self.host,
                    user = self.user ,
                    password = self.password,
                    database = self.database 
                )
                print("CONNESSIONE AL DB EFFETTUATA")
                return conn
            except mysql.connector.Error as error: 
                print(f"CONNESSIONE FALLITA. ERRORE: {error}. VADO CON IL RETRY")
                time.sleep(2)        
                retries -= 1

        raise Exception("RETRY TERMINATE, IMPOSSIBILE CONNETTERSI AL DATABASE")   #se dopo le 5 retry non ci si riesce a connettere solleviamo un eccezione con raise

    def __init__db(self): #in questa funzione creo la tabella se non esiste
        conn = self._get_connection()
        cursor = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (email)
        )
        """
        
        try:
            cursor.execute(query)
            conn.commit()
            print("✅ Tabella 'users' pronta.")
        except mysql.connector.Error as err:
            print(f"❌ Errore nella creazione tabella: {err}")
        finally:
            cursor.close()
            conn.close()
            print("TABELLA USERS CORRETTAMENTE CREATA E PRONTA ALL'USO")

    def user_exist(self, email): 
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = "SELECT id FROM users WHERE email = %s"
        cursor.execute(query, (email,))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return result is not None
    
    def add_user(self,email):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            query = "INSERT INTO users (email) VALUES (%s)"
            
            cursor.execute(query, (email,))
            print("ARRIVO QUI POI FINISCE")
            
            conn.commit()
            print("UTENTE CORRETTAMENTE INSERITO NELLA TABELLA")
            return True
        except mysql.connector.IntegrityError: #Eccezione che viene sollevata se l'email inserita è già presente
            return False
        
        except Exception as e:
            print(f"Errore sconosciuto SQL: {e}")
            return False

        finally: 
            cursor.close()
            conn.close()
        
    def delete_user(self, email): 
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            print("CONTROLLO SE L'UTENTE ESISTE")
            query2 = "SELECT id FROM users WHERE email = %s"
            cursor.execute(query2, (email,))
            print("SONO QUI")
            result = cursor.fetchone()
            print(f"RESULT = {result}")
            print(f"{result is not None}")

            if result is None:
                return False

            query = "DELETE FROM users WHERE email = (%s)"
            cursor.execute(query, (email,))
            conn.commit()
            print("UTENTE CORRETTAMENTE ELIMINATO DALLA TABELLA")
            return True
        
        # NEL CASO IN CUI NEL DB C'E' SOLO UNA TABELLA, LA DELETE NON
        # TORNERA' MAI UNA ECCEZIONE DI TIPO INTEGRITY ERROR : 
        # LA DELETE DI UN RECORD CHE NON ESISTE NON DA ERRORE.
        #INTEGRITY ERROR VIENE SOLLEVATA DALLA DELETE NEL CASO IN CUI ESISTONO
        #DIPENDENZE DATI IN PIU' TABELLE.
        #except mysql.connector.IntegrityError: 
        #   return False
        
        except Exception as e:
            print(f"Errore sconosciuto SQL: {e}")
            return False

        finally:
            cursor.close()
            conn.close()
    