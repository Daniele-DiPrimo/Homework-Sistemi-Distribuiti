from flask import Flask, request, jsonify 
import grpc 
from concurrent import futures 
import sys #serve per importare i file pb2 dalla cartella grpc_generated
import os
import threading 
import json 
import redis
from extensions import db
from user import User

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
import user_service_pb2
import user_service_pb2_grpc

app = Flask(__name__)

#setup database
db_user = os.getenv('USER_DB')
db_password = os.getenv('PASSWORD_DB')
db_host = os.getenv('HOST_DB')
db_port = os.getenv('USER_DB_PORT')
db_name = os.getenv('NAME_DB')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()

redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'user-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db = 0,
    decode_responses = True    #converte tutti i dati all'interno della cache redis in stringhe 
)

class CheckUserHandler(user_service_pb2_grpc.CheckUserServiceServicer): 
    def CheckUserExists(self, request, context): 
        email = request.email
        print(f"Controllo se esiste {email} nel DB")

        with app.app_context():    
            esiste = User.user_exist(email) 

        print("eseguito!!")

        if esiste:
            return user_service_pb2.UserCheckResponse(status = 0 , message = "UTENTE TROVATO")
        else: 
            return user_service_pb2.UserCheckResponse(status = 1, message = "UTENTE NON TROVATO")
        

def run_grpc_server(): 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service_pb2_grpc.add_CheckUserServiceServicer_to_server(CheckUserHandler(), server)

    server.add_insecure_port('[::]:50051')  
    print("gRPC Server in ascolto sulla porta 50051...")
    server.start()
    server.wait_for_termination()

@app.route('/register', methods = ['POST'])
def register_user(): 
    # prima della logica di business controllo sulla cache di redis se la richiesta è già stata gestita

    request_id = request.headers.get('X-Request-ID')
    client_id = request.headers.get('X-Client-ID')

    if not request_id or not client_id:
        return jsonify({"error": "X-REQUEST-ID/X-ClientID mancante nell'header della richiesta HTTP."}), 400   

    
    #la chiave cache sarà formata dalla concatenazione di client_id - nome del servizio - request_id
    cache_key = f"{client_id}:register:{request_id}"
    
    cached_data = redis_client.get(cache_key)

    #esiste la corrispondenza, richiesta già gestita
    if cached_data:
        print(f"Risposta presente nella cache. Cache Data: {cached_data}, cachekey{cache_key}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']
    
    # non esiste una corrispondenza, la richiesta è inviata per la prima volta, quindi passo alla logica di business
    data = request.get_json()

    if not data or 'email' not in data: 
        return jsonify({"errore" : "EMAIL NON INSERITA"}), 400
    
    email = data['email']
    nome = data['nome']
    cognome = data['cognome']
    print(f"TENTATIVO DI REGISTRAZIONE PER {email} --> {nome} {cognome}")

    success = User.add_user(email, nome, cognome)

    if success:        
        response_body = {              
            "message": "Utente registrato con successo",
            "email_request": email,  
            "status": True            
        }
        status_code = 201

    else:
        response_body = {              
            "message": "Utente già registrato, email presente in archivio.",
            "email_request": email,  
            "status": False            
        }
        status_code = 409

    cache_packet = {
        "body": response_body,
        "status_code": status_code,
    }
    redis_client.setex(cache_key, 3600, json.dumps(cache_packet))  #3600 --> la cache di redis terrà i dati per 1 ora

    return jsonify(response_body), status_code


@app.route('/delete', methods = ['POST'])
def delete_user():

    #applico l'AT-MOST-ONCE anche in delete_user() --> l'idea è quella di conservare i dati nella cache per meno tempo rispetto alla registrazione --> L'Idempotenza in register è più restrittiva, se arriva
    #una richiesta duplicata l'accesso al db è critico. Per la delete, invece, è meno critico. Quindi, se avviene una richiesta duplicata ( magari si è perso il messaggio di ritorno e l'utente non sa se l'operazione è andata a buon fine)
    #ritorno lo stato salvato nella cache. Dopo qualche minuto cancello i dati nella cache e se arriva un duplicato torno il jsonify con body "email non presente in archivio". 

    request_id = request.headers.get('X-Request-ID')
    client_id = request.headers.get('X-Client-ID')

    if not request_id or not client_id:
        return jsonify({"error": "X-REQUEST-ID/X-Client-ID mancante nell'header della richiesta HTTP."}), 400 

    cache_key = f"{client_id}:delete:{request_id}"
    cached_data = redis_client.get(cache_key)

    if cached_data:
        print(f"Risposta presente nella cache. Cache Data: {cached_data}, CacheKey {request_id}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']
    
    data = request.get_json()

    if not data or 'email' not in data: 
        return jsonify({"errore" : "email non inserita. Perfavore inserisci email"})

    email = data['email']
    success = User.delete_user(email)

    if success: 
        response_body = {              
            "message": "utente correttamente eliminato dall'archivio",
            "email_request": email,  
            "status": True            
        }
        status_code = 200

    else:
        response_body = {              
            "message": "utente non presente in archivio",
            "email_request": email,  
            "status": True            
        }
        status_code = 404
    
    cache_packet = {
        "body": response_body,
        "status_code": status_code
    }

    redis_client.setex(cache_key, 180, json.dumps(cache_packet))

    return jsonify(response_body), status_code



if __name__ == '__main__':
    
    """utilizzo un thread per il server grpc in modo tale che quando viene eseguita
    wait_for_termination() non viene bloccato il main thread che gestisce le api request"""
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()

    print("REST Server in ascolto sulla porta 5000...")
    app.run(host='0.0.0.0', port=5000, debug=False)

