"""Modeli SQLAlchemy per flights e airports of interest.

Le classi mappano le tabelle del DB usate dall'app.
"""

from extensions import db


class Flights(db.Model):
    """Rappresenta un volo registrato dall'API opensky.

    Le colonne corrispondono ai campi principali necessari per il calcolo
    e l'aggregazione. È presente una UniqueConstraint per evitare duplicati.
    """
    id = db.Column(db.Integer, primary_key=True)
    icao24 = db.Column(db.CHAR(6), nullable=False)
    firstSeen = db.Column(db.DateTime, nullable=False)
    estDepartureAirport = db.Column(db.CHAR(4), nullable=False)
    lastSeen = db.Column(db.DateTime, nullable=False)
    estArrivalAirport = db.Column(db.String(4), nullable=False)
    callsign = db.Column(db.CHAR(8), nullable=False)

    __table_args__ = (
        db.UniqueConstraint(
            'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen', 'estArrivalAirport', 'callsign',
            name='unique_flight'
        ),
    )

    def to_dict(self):
        """Restituisce una rappresentazione serializzabile del record."""
        return {
            "id": self.id,
            "icao24": self.icao24,
            "firstSeen": self.firstSeen.isoformat(),
            "estDepartureAirport": self.estDepartureAirport,
            "lastSeen": self.lastSeen.isoformat(),
            "estArrivalAirport": self.estArrivalAirport,
            "callsign": self.callsign,
        }


class AirportsOfInterest(db.Model):
    """Preferenze dell'utente: quali aeroporti monitorare e relative soglie."""
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(50), nullable=False)
    icao = db.Column(db.CHAR(4), nullable=False)
    high_value = db.Column(db.Integer, nullable=True)
    low_value = db.Column(db.Integer, nullable=True)

    __table_args__ = (
        db.UniqueConstraint('email', 'icao', name='unique_email_icao'),
        db.CheckConstraint('high_value > low_value', name='check_high_gt_low'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "icao": self.icao,
            "email": self.email,
            "high_value": self.high_value,
            "low_value": self.low_value,
        }