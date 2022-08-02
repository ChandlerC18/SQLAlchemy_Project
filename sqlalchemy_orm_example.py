from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_

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

# using Query
# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
result = session.query(Customers).all()

for row in result:
   print ("Name: ",row.name, "Address:",row.address, "Email:",row.email)

# updating objects
x = session.query(Customers).get(2)
x.address = 'Banjara Hills Secunderabad'
session.commit()

# session.query(Customers).filter(Customers.id! = 2).update({Customers.name:"Mr."+Customers.name}, synchronize_session = False) # use update method of Query object for bulk updates
# session.rollback() # retain earlier persistent position

# applying filters
# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id > ?
result = session.query(Customers).filter(Customers.id>2)

# filter operation
# filter equal SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id = ?
result = session.query(Customers).filter(Customers.id == 2)

# filter not equal SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id != ?
result = session.query(Customers).filter(Customers.id != 2)

# filter like SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.name LIKE ?
result = session.query(Customers).filter(Customers.name.like('Ra%'))

# filter in SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id IN (?, ?)
result = session.query(Customers).filter(Customers.id.in_([1,3]))

# filter and SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id > ? AND customers.name LIKE ?
result = session.query(Customers).filter(Customers.id>2, Customers.name.like('Ra%')) # using multiple commas
result = session.query(Customers).filter(and_(Customers.id>2, Customers.name.like('Ra%'))) # using and_()

# filter or SQL expression:
# SELECT customers.id 
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id > ? OR customers.name LIKE ?
result = session.query(Customers).filter(or_(Customers.id>2, Customers.name.like('Ra%')))

# for row in result:
#    print ("ID:", row.id, "Name: ",row.name, "Address:",row.address, "Email:",row.email)
