import psycopg2
from dotenv import load_dotenv
import os
import json
from lxml import etree
import argparse


def connect_db():
    load_dotenv()
    conn = psycopg2.connect(
        host = os.getenv("PG_HOST"),
        port = os.getenv("PG_PORT"),
        database = os.getenv("PG_DB"),
        user = os.getenv("PG_USER"),
        password = os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()
    
    return conn, cur

def drop_tables(conn, cur):
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

def create_indices(cur):
    cur.execute("CREATE INDEX IF NOT EXISTS room_idx ON students(room);")
    
def extract_from_json(rooms_json, students_json):
    #Write data from json to DB
    with open(rooms_json, "r") as f:
        rooms_data = json.load(f)
        
    rows_rooms = [[entry[k] for k in entry.keys()] for entry in rooms_data]

    with open(students_json, "r") as f:
        students_data = json.load(f)
        
    rows_students = [[entry[k] for k in entry.keys()] for entry in students_data]

    return (rows_rooms, rows_students)

def load_to_db(rows_rooms, rows_students, cur):
    cur.executemany(
        "INSERT INTO rooms (id, name) VALUES (%s, %s)",
        rows_rooms
    )

    cur.executemany(
        "INSERT INTO students (birthday, id, name, room, sex) VALUES (%s, %s, %s, %s, %s)",
        rows_students
    )
   
def extract_from_db(cur, query):
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def load_to_json(data, filename, ouput_folder):
    filepath = f"{ouput_folder}/{filename}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

def load_to_xml(data, filename,ouput_folder, record_tag):
    root = etree.Element(f"_{filename}")
    
    for item in data:
        record = etree.SubElement(root, record_tag) 
        for key, value in item.items():
            child = etree.SubElement(record, key)
            child.text = str(value)
    
    tree = etree.ElementTree(root)
    filepath=f"{ouput_folder}/{filename}.xml"
    tree.write(
        filepath,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8"
    )

def get_queries(folder_path):
    queries = {}
    for f in os.listdir(folder_path):
        if not f.endswith(".sql"):
            continue
        filename = os.path.splitext(f)[0]
        content = open(os.path.join(folder_path, f)).read()
        queries[filename] = content
    return queries  

def fill_db(conn, cur,rooms_path, student_path):
    drop_tables(conn, cur)
    create_tables(cur)
    rows_rooms, rows_students = extract_from_json(rooms_path, student_path)
    load_to_db(rows_rooms, rows_students, cur)
    
def query_db(cur, queries_folder, format, ouput_folder, record_teg_xml):
    #Create indices 
    create_indices(cur)
    
    #get a list of queries 
    queries = get_queries(queries_folder)

    #Load resulsts of queires into json / xml
    os.makedirs(ouput_folder, exist_ok=True)
    
    for name, sql in queries.items():
        data = extract_from_db(cur, sql)
        if format=="json":
            load_to_json(data, name, ouput_folder)
            print(f"Loaded data to {ouput_folder}/{name}.json")
        else:
            load_to_xml(data, name, ouput_folder, record_teg_xml)
            print(f"Loaded data to {ouput_folder}/{name}.xml")
       
def main():
    #CLI
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--students", required=True)
    parser.add_argument("--rooms", required=True)
    parser.add_argument("--format", required=True, choices=["json", "xml"])
    parser.add_argument("--queries_folder", required=True)
    parser.add_argument("--output_folder", required=True)
    parser.add_argument("--record_teg_xml", default="record")
    
    args = parser.parse_args()
    
    student_path = args.students
    rooms_path = args.rooms
    format = args.format
    queries_folder = args.queries_folder
    ouput_folder = args.output_folder
    record_teg_xml = args.record_teg_xml
    
    #Set connection to DB
    conn, cur = connect_db()
    
    #Create tables and load data from json to them
    fill_db(conn, cur, rooms_path, student_path)

    #Query DB and and save results
    query_db(cur, queries_folder, format, ouput_folder, record_teg_xml)
    
    #Close connection to DB
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()