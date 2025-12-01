from extensions import db

class Flights(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    icao24 = db.Column(db.CHAR(6), nullable=False)
    firstSeen = db.Column(db.DateTime, nullable=False)
    estDepartureAirport = db.Column(db.CHAR(4), nullable=False)
    lastSeen = db.Column(db.DateTime, nullable=False)
    estArrivalAirport = db.Column(db.String(4), nullable=False)
    callsign = db.Column(db.CHAR(8), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('icao24',
            'firstSeen',
            'estDepartureAirport',
            'lastSeen',
            'estArrivalAirport',
            'callsign',
            name='unique_flight'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "icao24": self.icao24,
            "firstSeen": self.firstSeen,
            "estDepartureAirport": self.estDepartureAirport,
            "lastSeen": self.lastSeen,
            "estArrivalAirport": self.estArrivalAirport,
            "callsign": self.callsign
        }

class AirportsOfInterest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(50), nullable=False)
    icao = db.Column(db.CHAR(4), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('email', 'icao', name='unique_email_icao'),
    )