from extensions import db
from sqlalchemy.orm import validates

class Flights(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    icao24 = db.Column(db.CHAR(6), nullable=False)
    firstSeen = db.Column(db.DateTime, nullable=False)
    estDepartureAirport = db.Column(db.CHAR(4), nullable=False)
    lastSeen = db.Column(db.DateTime, nullable=False)
    estArrivalAirport = db.Column(db.String(4), nullable=False)
    callsign = db.Column(db.CHAR(8), nullable=False)

class AirportsOfInterest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(50), nullable=False)
    icao = db.Column(db.CHAR(4), nullable=False)

    # uniqueness constraint
    __table_args__ = (
        db.UniqueConstraint('email', 'icao', name='unique_email_icao'),
    )