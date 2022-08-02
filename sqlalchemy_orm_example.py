from sqlalchemy import create_engine. text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, subqueryload, joinedload
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

# Query methods
# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
session.query(Customers).all() # returns a list

# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# LIMIT ? OFFSET ?
session.query(Customers).first() # returns a scalar (bound parameters for LIMIT is 1 and for OFFSET is 0.)

session.query(Customers).one() # fetches all rows (no one object identity or composite row present in the result, raise error)

# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.id = ?
session.query(Customers).filter(Customers.id == 3).scalar() # returns first column of row

# Textual SQL
# SQL expression
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE id = ?
cust = session.query(Customers).filter(text("id = :value")).params(value = 1).one()

# link textual SQL to Core or ORM-mapped column expressions positionally
stmt = text("SELECT name, id, name, address, email FROM customers")
stmt = stmt.columns(Customers.id, Customers.name)
session.query(Customers.id, Customers.name).from_statement(stmt).all()

# building relationship
# SQL expression
# CREATE TABLE invoices (
#    id INTEGER NOT NULL,
#    custid INTEGER,
#    invno INTEGER,
#    amount INTEGER,
#    PRIMARY KEY (id),
#    FOREIGN KEY(custid) REFERENCES customers (id)
# )
class Invoice(Base):
   __tablename__ = 'invoices'

   id = Column(Integer, primary_key = True)
   custid = Column(Integer, ForeignKey('customers.id'))
   invno = Column(Integer)
   amount = Column(Integer)
   customer = relationship("Customer", back_populates = "invoices")

Customer.invoices = relationship("Invoice", order_by = Invoice.id, back_populates = "customer")
Base.metadata.create_all(engine)

# related objects
c1 = Customer(name = "Gopal Krishna", address = "Bank Street Hydarebad", email = "gk@gmail.com")
c1.invoices = [Invoice(invno = 10, amount = 15000), Invoice(invno = 14, amount = 3850)]

Session = sessionmaker(bind = engine)
session = Session()
session.add(c1)
session.commit()

c2 = [
   Customer(
      name = "Govind Pant",
      address = "Gulmandi Aurangabad",
      email = "gpant@gmail.com",
      invoices = [Invoice(invno = 3, amount = 10000),
      Invoice(invno = 4, amount = 5000)]
   )
]

session.add(c2)
session.commit()

# add multiple customers
rows = [
   Customer(
      name = "Govind Kala",
      address = "Gulmandi Aurangabad",
      email = "kala@gmail.com",
      invoices = [Invoice(invno = 7, amount = 12000), Invoice(invno = 8, amount = 18500)]),

   Customer(
      name = "Abdul Rahman",
      address = "Rohtak",
      email = "abdulr@gmail.com",
      invoices = [Invoice(invno = 9, amount = 15000),
      Invoice(invno = 11, amount = 6000)
   ])
]

session.add_all(rows)
session.commit()

# join
# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email, invoices.id
# AS invoices_id, invoices.custid
# AS invoices_custid, invoices.invno
# AS invoices_invno, invoices.amount
# AS invoices_amount
# FROM customers, invoices
# WHERE customers.id = invoices.custid
for c, i in session.query(Customer, Invoice).filter(Customer.id == Invoice.custid).all():
   print ("ID: {} Name: {} Invoice No: {} Amount: {}".format(c.id,c.name, i.invno, i.amount))

# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers JOIN invoices ON customers.id = invoices.custid
# WHERE invoices.amount = ?
session.query(Customer).join(Invoice).filter(Invoice.amount == 8500).all()

result = session.query(Customer).join(Invoice).filter(Invoice.amount == 8500)
for row in result:
   for inv in row.invoices:
      print (row.id, row.name, inv.invno, inv.amount)

for u, count in session.query(Customer, stmt.c.invoice_count).outerjoin(stmt, Customer.id == stmt.c.custid).order_by(Customer.id):
   print(u.name, count)

# relationship operators
# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers, invoices
# WHERE invoices.invno = ?
s = session.query(Customer).filter(Invoice.invno.__eq__(12)) # many-to-one "equals" comparison

# SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers, invoices
# WHERE invoices.custid != ?
s = session.query(Customer).filter(Invoice.custid.__ne__(2)) # many-to-one "not equals" comparison

# SQL expression:
# SELECT invoices.id
# AS invoices_id, invoices.custid
# AS invoices_custid, invoices.invno
# AS invoices_invno, invoices.amount
# AS invoices_amount
# FROM invoices
# WHERE (invoices.invno LIKE '%' + ? || '%')
s = session.query(Invoice).filter(Invoice.invno.contains([3,4,5])) # used for one-to-many collections

# SQL expressions:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE EXISTS (
#    SELECT 1
#    FROM invoices
#    WHERE customers.id = invoices.custid
#    AND invoices.invno = ?)
s = session.query(Customer).filter(Customer.invoices.any(Invoice.invno==11))

# SQL expressions:
# SELECT invoices.id
# AS invoices_id, invoices.custid
# AS invoices_custid, invoices.invno
# AS invoices_invno, invoices.amount
# AS invoices_amount
# FROM invoices
# WHERE EXISTS (
#    SELECT 1
#    FROM customers
#    WHERE customers.id = invoices.custid
#    AND customers.name = ?)
s = session.query(Invoice).filter(Invoice.customer.has(name = 'Arjun Pandit'))

# Eager Loading
# subquery load SQL expression:
# SELECT customers.id
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email
# FROM customers
# WHERE customers.name = ?
# ('Govind Pant',)
#
# SELECT invoices.id
# AS invoices_id, invoices.custid
# AS invoices_custid, invoices.invno
# AS invoices_invno, invoices.amount
# AS invoices_amount, anon_1.customers_id
# AS anon_1_customers_id
# FROM (
#    SELECT customers.id
#    AS customers_id
#    FROM customers
#    WHERE customers.name = ?)
#
# AS anon_1
# JOIN invoices
# ON anon_1.customers_id = invoices.custid
# ORDER BY anon_1.customers_id, invoices.id 2018-06-25 18:24:47,479
# INFO sqlalchemy.engine.base.Engine ('Govind Pant',)
c1 = session.query(Customer).options(subqueryload(Customer.invoices)).filter_by(name = 'Govind Pant').one()
print (c1.name, c1.address, c1.email)

for x in c1.invoices:
   print ("Invoice no : {}, Amount : {}".format(x.invno, x.amount))

# join load SQL expression:
# SELECT customers.id 
# AS customers_id, customers.name
# AS customers_name, customers.address
# AS customers_address, customers.email
# AS customers_email, invoices_1.id
# AS invoices_1_id, invoices_1.custid
# AS invoices_1_custid, invoices_1.invno
# AS invoices_1_invno, invoices_1.amount
# AS invoices_1_amount
#
# FROM customers
# LEFT OUTER JOIN invoices
# AS invoices_1
# ON customers.id = invoices_1.custid
#
# WHERE customers.name = ? ORDER BY invoices_1.id
# ('Govind Pant',)
c1 = session.query(Customer).options(joinedload(Customer.invoices)).filter_by(name='Govind Pant').one()
