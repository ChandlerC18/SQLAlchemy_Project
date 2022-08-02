from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# declaring mapping
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

# creating session
Session = sessionmaker(bind = engine)
session = Session()

# adding objects
c1 = Customers(name = 'Ravi Kumar', address = 'Station Road Nanded', email = 'ravi@gmail.com') # single object
session.add(c1)
session.commit()

session.add_all([ # multiple objects at the same time
   Customers(name = 'Komal Pande', address = 'Koti, Hyderabad', email = 'komal@gmail.com'),
   Customers(name = 'Rajender Nath', address = 'Sector 40, Gurgaon', email = 'nath@gmail.com'),
   Customers(name = 'S.M.Krishna', address = 'Budhwar Peth, Pune', email = 'smk@gmail.com')]
)
session.commit()
