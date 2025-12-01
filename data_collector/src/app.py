import os
from flask import Flask, request, jsonify
from extensions import db, scheduler
from models import AirportsOfInterest
import tasks

app = Flask(__name__)


db_user = os.getenv('FLIGHTSDB_USER')
db_password = os.getenv('FLIGHTSDB_PASSWORD')
db_host = os.getenv('FLIGHTSDB_HOST')
db_port = os.getenv('FLIGHTSDB_PORT')
db_name = os.getenv('FLIGHTSDB_DATABASE')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()

scheduler.init_app(app)
scheduler.start()


@app.route('/airport-of-interest/add', methods=['POST'])
def add_airports_of_interest():
    data = request.get_json()
    email = data.get('email')
    airports = tuple(data.get('airports'))

    for airport in airports:
        db.session.add(AirportsOfInterest(email=email, icao=airport))

    db.session.commit()
    return jsonify({"message": "Airports added"}), 201

@app.route('/airport-of-interest/average', methods=['POST'])
def average():
    data = request.get_json()


#Calcolo della Media degli ultimi X giorni: 
# Fornisce una funzione per calcolare e restituire la media degli ultimi 
# X giorni sul numero di voli in partenza e/o in arrivo da un dato 
# aeroporto: il DataCollector restituisce la media di quanti voli in 
# partenza e/o quanti in arrivo ci sono stati negli ultimi X giorni da un 
# dato aeroporto. 


if __name__ == '__main__':
    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)