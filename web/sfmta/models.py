from sqlalchemy import Column, String, DateTime, BigInteger, Integer, Float

from sfmta.db import Base


class Vehicle(Base):
    __tablename__ = 'vehicles_t'

    vehicle_tag = Column(String(), primary_key=True)

    def __init__(self, vehicle_tag):
        self.vehicle_tag = vehicle_tag

    def __repr__(self):
        return '<vehicle_tag {}>'.format(self.vehicle_tag)


class VehicleStop(Base):
    __tablename__ = 'sfmta_stops_det_10'

    vehicle_tag = Column(String(), primary_key=True)
    report_time = Column(DateTime(), primary_key=True)
    stopped_for = Column(BigInteger())
    hour_of_day = Column(Integer())
    latitude = Column(Float())
    longitude = Column(Float())

    def __init__(self, vehicle_tag, report_time, stopped_for, hour_of_day, latitude, longitude):
        self.vehicle_tag = vehicle_tag
        self.report_time = report_time
        self.stopped_for = stopped_for
        self.hour_of_day = hour_of_day
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return '<vehicle_tag {}, report_time {}, stopped_for {}, hour_of_day {}, latitude {}, longitude{}>'\
            .format(self.vehicle_tag, self.report_time, self.stopped_for, self.hour_of_day, self.latitude, self.longitude)
