from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.sql import select, func
from sqlalchemy import join
from sqlalchemy import and_, or_, not_, asc, desc, between
from sqlalchemy import union, union_all, except_, intersect

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

# conjunction examples

# AND
stmt = select([students]).where(and_(students.c.name == 'Ravi', students.c.id <3)) # SELECT students.id, students.name, students.lastname
                                                                                   # FROM students WHERE students.name = :name_1 AND students.id < :id_1
result = conn.execute(stmt)
print (result.fetchall())

# OR
stmt = select([students]).where(or_(students.c.name == 'Ravi', students.c.id <3))  # WHERE students.name = :name_1 OR students.id < :id_1

# NOT: not_()

# ascending ORDER BY clause
stmt = select([students]).order_by(asc(students.c.name))                           # ORDER BY students.name ASC

# descending ORDER BY clause
stmt = select([students]).order_by(desc(students.c.lastname))                      # ORDER BY students.lastname DESC

# between
stmt = select([students]).where(between(students.c.id,2,4))                        # BETWEEN :id_1 AND :id_2

# functions example
result = conn.execute(select([func.now()])).fetchone() # now function
result = conn.execute(select([func.count(students.c.id)])) # count function
result = conn.execute(select([func.max(employee.c.marks)])) # max function
result = conn.execute(select([func.min(employee.c.marks)])) # min function
result = conn.execute(select([func.avg(employee.c.marks)])) # avg function

# set operations example
# union SQL expression:
# SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ? UNION SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ?

u = union(addresses.select().where(addresses.c.email_add.like('%@gmail.com addresses.select().where(addresses.c.email_add.like('%@yahoo.com'))))

result = conn.execute(u)
result.fetchall()

# union_all SQL expression:
# SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ? UNION ALL SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ?
u = union_all(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.email_add.like('%@yahoo.com')))

# except_ SQL expression:
# SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ? EXCEPT SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.postal_add LIKE ?
u = except_(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.postal_add.like('%Pune')))

# intersect SQL expression:
# SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.email_add LIKE ? INTERSECT SELECT addresses.id,
#    addresses.st_id,
#    addresses.postal_add,
#    addresses.email_add
# FROM addresses
# WHERE addresses.postal_add LIKE ?
u = intersect(addresses.select().where(addresses.c.email_add.like('%@gmail.com')), addresses.select().where(addresses.c.postal_add.like('%Pune')))
