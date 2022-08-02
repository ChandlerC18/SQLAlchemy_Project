from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy import join

engine = create_engine('sqlite:///college.db', echo=True)
meta = MetaData()
                                                            # SQL expression
students = Table(                                           # CREATE TABLE students (
   'students', meta,                                        #
   Column('id', Integer, primary_key = True),               # id INTEGER NOT NULL,
   Column('name', String),                                  # name VARCHAR,
   Column('lastname', String),                              # lastname VARCHAR,
)                                                           # PRIMARY KEY (id) )

addresses = Table(                                          # CREATE TABLE addresses (
   'addresses', meta,                                       #
   Column('id', Integer, primary_key = True),               # id INTEGER NOT NULL,
   Column('st_id', Integer, ForeignKey('students.id')),     # st_id INTEGER, FOREIGN KEY(st_id) REFERENCES students (id),
   Column('postal_add', String),                            # postal_add VARCHAR,
   Column('email_add', String)                              # email_add VARCHAR,
)                                                           # PRIMARY KEY (id) )

meta.create_all(engine)

conn = engine.connect()

# add data to students table
conn.execute(students.insert(), [
   {'name':'Ravi', 'lastname':'Kapoor'},
   {'name':'Rajiv', 'lastname' : 'Khanna'},
   {'name':'Komal','lastname' : 'Bhandari'},
   {'name':'Abdul','lastname' : 'Sattar'},
   {'name':'Priya','lastname' : 'Rajhans'},
])

# add data to addresses table
conn.execute(addresses.insert(), [
   {'st_id':1, 'postal_add':'Shivajinagar Pune', 'email_add':'ravi@gmail.com'},
   {'st_id':1, 'postal_add':'ChurchGate Mumbai', 'email_add':'kapoor@gmail.com'},
   {'st_id':3, 'postal_add':'Jubilee Hills Hyderabad', 'email_add':'komal@gmail.com'},
   {'st_id':5, 'postal_add':'MG Road Bangaluru', 'email_add':'as@yahoo.com'},
   {'st_id':2, 'postal_add':'Cannought Place new Delhi', 'email_add':'admin@khanna.com'},
])

# fetch data from both tables
s = select([students, addresses]).where(students.c.id == addresses.c.st_id) # SQL expression: SELECT students.id, students.name, students.lastname, addresses.id,
                                                                            #                        addresses.st_id, addresses.postal_add, addresses.email_add
                                                                            #                 FROM students, addresses WHERE students.id = addresses.st_id

result = conn.execute(s)

for row in result:
   print (row)

# multiple tables update (SQLite doesn't support multiple-table criteria within UPDATE)
                                              # SQL expession:
# stmt = students.update().\                  # UPDATE students
# values({                                    # SET email_add = :addresses_email_add, name = :name
#    students.c.name:'xyz',                   # FROM addresses
#    addresses.c.email_add:'abc@xyz.com'      # WHERE students.id = addresses.id
# }).\
# where(students.c.id == addresses.c.id)

# multiple tables delete (NotImplementedError if database does not support this method)
                                                            # SQL expression:
stmt = users.delete().\                                     # DELETE FROM users USING addresses
   where(users.c.id == addresses.c.id).\                    # WHERE users.id = addresses.id
   where(addresses.c.email_address.startswith('xyz%'))      # AND (addresses.email_address LIKE %(email_address_1)s || '%%')
conn.execute(stmt)

# join example
j = students.join(addresses, students.c.id == addresses.c.st_id) # SQL expression: 'students JOIN addresses ON students.id = addresses.st_id'
stmt = select([students]).select_from(j)
result = conn.execute(stmt)
result.fetchall()
