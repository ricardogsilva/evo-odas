"""sqlalchemy declarative models for the landsat8 scene list database."""

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()


class Scene(Base):
    __tablename__ = "scenes"

    id = Column(Integer, primary_key=True)
    product_id = Column(String)
    entity_id = Column(String)
    acquisition_date = Column(DateTime)
    cloud_cover = Column(Float)
    processing_level = Column(String)
    path = Column(Integer)
    row = Column(Integer)
    bbox = Column(Geometry("POLYGON"))
    download_url = Column(String)
