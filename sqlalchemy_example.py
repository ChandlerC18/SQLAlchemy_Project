from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

engine = create_engine('sqlite:///college.db', echo = True) # create database indicating database dialect and connection arguments
meta = MetaData()

# create students table within college database
students = Table(
   'students', meta,
   Column('id', Integer, primary_key = True),
   Column('name', String),
   Column('lastname', String),
)

meta.create_all(engine)

ins = students.insert() # insert object
                        # str(ins) would result in 'INSERT INTO students (id, name, lastname) VALUES (:id, :name, :lastname)'
                        # ins.compile().params shows binded parameters

# example insertion
ins = students.insert().values(name = 'Ravi', lastname = 'Kapoor')
conn = engine.connect()
result = conn.execute(ins) # result is a ResultProxy object; can acquire information about primary key values using 'result.inserted_primary_key'

# example issue many inserts
conn.execute(students.insert(), [
   {'name':'Rajiv', 'lastname' : 'Khanna'},
   {'name':'Komal','lastname' : 'Bhandari'},
   {'name':'Abdul','lastname' : 'Sattar'},
   {'name':'Priya','lastname' : 'Rajhans'},
])

# selecting rows
s = students.select() # construct SELECT expression
                      # translates to 'SELECT students.id, students.name, students.lastname FROM students'
# s = students.select().where(students.c.id>2) # using the WHERE clause for id > 2; the c attribute is an alias for column
result = conn.execute(s)
row = result.fetchone() # fetch records

for row in result: # print each row of table
   print (row)

# alternative for SELECT using the select function
# from sqlalchemy.sql import select
# s = select([users])
# result = conn.execute(s)

# using the text construct for passing SQL expressions as strings
from sqlalchemy import text
t = text("SELECT * FROM students")
result = connection.execute(t)

s = text("select students.name, students.lastname from students where students.name between :x and :y") # using bound parameters
conn.execute(s, x = 'A', y = 'L').fetchall()

stmt = text("SELECT * FROM students WHERE students.name BETWEEN :x AND :y")
stmt = stmt.bindparams( # pre-established bound values
   bindparam("x", type_= String),
   bindparam("y", type_= String)
)
result = conn.execute(stmt, {"x": "A", "y": "L"})

# from sqlalchemy.sql import select
# s = select([text("students.name, students.lastname from students")]).where(text("students.name between :x and :y")) # can use the select function
# conn.execute(s, x = 'A', y = 'L').fetchall()
