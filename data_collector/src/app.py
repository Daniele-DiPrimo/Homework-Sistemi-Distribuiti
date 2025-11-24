import os
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from database import db
from models import Flights

app = Flask(__name__)


db_user = os.getenv('MARIADB_USER')
db_password = os.getenv('MARIADB_PASSWORD')
db_host = os.getenv('MARIADB_HOST')
db_port = os.getenv('MARIADB_PORT')
db_name = os.getenv('MARIADB_DATABASE')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()


@app.route('/airport-of-interest/add', methods=['POST'])
def add_airports_of_interest():
    data = request.get_json()
    airports = tuple(data.get('airports'))
    return jsonify({"message": f"Airports added", "status": "success"})


if __name__ == '__main__':
    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)