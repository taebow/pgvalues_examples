from sqlalchemy import Column, String

from .base import Base


class Dictionnary(Base):
    __tablename__ = "dictionnary"
    word = Column(String, primary_key=True)
    pos = Column(String, primary_key=True)
