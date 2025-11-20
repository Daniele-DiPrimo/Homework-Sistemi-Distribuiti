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

    
    def _get_connectio(self): #in questa funzione faremo la connessione al database,
        
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

    
