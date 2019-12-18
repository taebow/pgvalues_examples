from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://test:test@localhost:7777/test")
Base = declarative_base()


class Dictionnary(Base):
    __tablename__ = "dictionnary"
    word = Column(String, primary_key=True)
    pos = Column(String, primary_key=True)


session = sessionmaker(bind=engine)()
