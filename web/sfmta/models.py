from sqlalchemy import Column, String
from sfmta.db import Base


class Vehicle(Base):
    __tablename__ = 'vehicles_t'

    vehicle_tag = Column(String(), primary_key=True)

    def __init__(self, vehicle_tag):
        self.vehicle_tag = vehicle_tag

    def __repr__(self):
        return '<vehicle_tag {}>'.format(self.vehicle_tag)