from database import db

class Flights(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    icao24 = db.Column(db.CHAR(6), nullable=False)
    firstSeen = db.Column(db.DateTime, nullable=False)
    estDepartureAirport = db.Column(db.CHAR(4), nullable=False)
    lastseen = db.Column(db.DateTime, nullable=False)
    estArrivalAirport = db.Column(db.String(4), nullable=False)
    callsign = db.Column(db.CHAR(4), nullable=False)