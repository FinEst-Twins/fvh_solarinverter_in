from app import db
#from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from datetime import datetime


class Datastream(db.Model):
    __tablename__ = "datastream"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64))
    description = db.Column(db.String(120))
    observationtype = db.Column(db.String(120))
    unitofmeasurement = db.Column(db.String(120))
    observedarea = db.Column(db.String(120))
    phenomenontime_begin = db.Column(db.Long)
    phenomenontime_end = db.Column(db.Long)
    resulttime_begin = db.Column(db.Long)
    resulttime_end = db.Column(db.Long)
    sensor_link = db.Column(db.String(120))
    thing_link = db.Column(db.String(120))
    observedproperty_link = db.Column(db.String(120))

    @classmethod
    def get_datastream_id(cls, thing_link, sensor_link):
        ds = PlatformUser.query.filter(Datastream.sensor_link = sensor_link,
        Datastream.thing_link = thing_link).first()

        return ds
