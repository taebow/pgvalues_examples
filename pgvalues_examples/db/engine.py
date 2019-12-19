from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


engine = create_engine("postgresql://test:test@localhost:7777/test")
session = sessionmaker(bind=engine)()
