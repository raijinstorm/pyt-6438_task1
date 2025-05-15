import psycopg2
from dotenv import load_dotenv
import os
import json

load_dotenv()

conn = psycopg2.connect(
    host = os.getenv("PG_HOST"),
    port = os.getenv("PG_PORT"),
    database = os.getenv("PG_DB"),
    user = os.getenv("PG_USER"),
    password = os.getenv("PG_PASSWORD")
)

cur = conn.cursor()

def drop_tables(coon, cur):
    #We need it so that drop tables work 
    conn.autocommit = True 

    #Clean up to start always from scratch
    cur.execute("DROP TABLE IF EXISTS students")
    cur.execute("DROP TABLE IF EXISTS rooms")

def create_tables(cur):
    #Create tables in DB
    cur.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
            id INTEGER PRIMARY KEY,
            name VARCHAR(20) NOT NULL
            )  
    """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY,
            birthday DATE NOT NULL,
            name VARCHAR(30) NOT NULL,
            room INTEGER REFERENCES rooms(id),
            sex CHAR(1) CHECK(sex IN ('M', 'F'))
            )  
    """)

def list_tables(cur):
    # Check that tables actually exist
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    """)

    print(cur.fetchall())
     
def extract(rooms_json, students_json):
    #Write data from json to DB
    with open(rooms_json, "r") as f:
        rooms_data = json.load(f)
        
    rows_rooms = [[entry[k] for k in entry.keys()] for entry in rooms_data]

    with open(students_json, "r") as f:
        students_data = json.load(f)
        
    rows_students = [[entry[k] for k in entry.keys()] for entry in students_data]

    return (rows_rooms, rows_students)

def load(rows_rooms, rows_students, cur):
    cur.executemany(
        "INSERT INTO rooms (id, name) VALUES (%s, %s)",
        rows_rooms
    )

    cur.executemany(
        "INSERT INTO students (birthday, id, name, room, sex) VALUES (%s, %s, %s, %s, %s)",
        rows_students
    )
    
    
drop_tables(conn, cur)
create_tables(cur)

list_tables(cur)

rows_rooms, rows_students = extract("data/rooms.json","data/students.json" )
load(rows_rooms, rows_students, cur)

#Check the result
cur.execute("SELECT * FROM rooms")

res = cur.fetchmany(5)
for el in res:
    print(el)
    
cur.execute("SELECT * FROM students")

res = cur.fetchmany(5)
for el in res:
    print(el)

#Close 
cur.close()
conn.close()