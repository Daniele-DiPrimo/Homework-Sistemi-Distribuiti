from flask import Flask, request, jsonify 
import grpc 
from concurrent import futures 
import sys #serve per importare i file pb2 dalla cartella grpc_generated
import os
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))

import user_service_pb2
import user_service_pb2_grpc

from database import UserDB

app = Flask(__name__)
db = UserDB() 

class CheckUserHandler(user_service_pb2_grpc.CheckUserServiceServicer): 
    def CheckUserExsist(self, request, context): 
        email = request.email
        print(f"Controllo se esiste {email} nel DB")

        esiste = db.user_exist(email) 

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
    data = request.get_json()

    if not data or 'email' not in data: 
        return jsonify({"errore" : "EMAIL NON INSERITA"}), 400
    
    email = data['email']
    print(f"TENTATIVO DI REGISTRAZIONE PER {email}")
    success = db.add_user(email)

    if success: 
        return jsonify({"message": "UTENTE REGISTRATO CON SUCCESSO"}), 201
    else: 
        return jsonify({"message": "EMAIL GIA' PRESENTE NEL SISTEMA"}), 409

@app.route('/deleteUser', methods = ['POST'])
def delete_user(): 
    data = request.get_json()

    if not data or 'email' not in data: 
        return jsonify({"errore" : "email non inserita. Perfavore inserisci email"})

    email = data['email']

    print(f"TENTATIVO DI DELETE SU {email}")
    success = db.delete_user(email)

    if success: 
        return jsonify({"message": "utente correttamente eliminato dall'archivio"})
    else:
        return jsonify({"message": "la mail non è presente nell'archivio"})


if __name__ == '__main__':
    # 1. Avviamo il server gRPC in un thread separato (background)
    # 'daemon=True' significa che se chiudi il programma principale, muore anche questo thread
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()

    # 2. Avviamo Flask nel thread principale (foreground)
    print("🌍 REST Server in ascolto sulla porta 5000...")
    
    # host='0.0.0.0' è FONDAMENTALE per farlo funzionare dentro Docker in futuro
    app.run(host='0.0.0.0', port=5000, debug=False)

