from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# SQL expression:
# CREATE TABLE customers (
#    id INTEGER NOT NULL,
#    name VARCHAR,
#    address VARCHAR,
#    email VARCHAR,
#    PRIMARY KEY (id)
# )

class Customers(Base):
   __tablename__ = 'customers'

   id = Column(Integer, primary_key = True)
   name = Column(String)
   address = Column(String)
   email = Column(String)

engine = create_engine('sqlite:///sales.db', echo = True)
Base.metadata.create_all(engine)
