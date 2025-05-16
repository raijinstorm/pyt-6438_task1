import psycopg2
from dotenv import load_dotenv
import os
import json
from lxml import etree
import argparse
import logging
from typing import Tuple, List, Dict, Any

def connect_db() -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    load_dotenv()
    conn = psycopg2.connect(
        host = os.getenv("PG_HOST"),
        port = os.getenv("PG_PORT"),
        database = os.getenv("PG_DB"),
        user = os.getenv("PG_USER"),
        password = os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()
    logging.info("DataBase connection is set\n")
    return conn, cur

def drop_tables(conn: psycopg2.extensions.connection, cur: psycopg2.extensions.cursor) -> None:
    #Clean up to start always from scratch
    cur.execute("DROP TABLE IF EXISTS students")
    cur.execute("DROP TABLE IF EXISTS rooms")
    
    conn.commit()
    logging.info("Table are dropped")

def create_tables(cur: psycopg2.extensions.cursor) -> None:
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
    logging.info("Tables 'students' and 'rooms' are created")

def create_indices(cur: psycopg2.extensions.cursor) -> None:
    cur.execute("CREATE INDEX IF NOT EXISTS room_idx ON students(room);")
    logging.info("Indices are created")
    
def extract_from_json(rooms_json: str, students_json: str) -> Tuple[List[List[Any]], List[List[Any]]]:
    #Write data from json to DB
    with open(rooms_json, "r") as f:
        rooms_data = json.load(f)
        
    rows_rooms = [[entry[k] for k in entry.keys()] for entry in rooms_data]

    with open(students_json, "r") as f:
        students_data = json.load(f)
        
    rows_students = [[entry[k] for k in entry.keys()] for entry in students_data]

    logging.info("Data is extracted from json files")
    return (rows_rooms, rows_students)

def load_to_db(rows_rooms: List[List[Any]], 
               rows_students:List[List[Any]], 
               cur:psycopg2.extensions.cursor) -> None:
    cur.executemany(
        "INSERT INTO rooms (id, name) VALUES (%s, %s)",
        rows_rooms
    )

    cur.executemany(
        "INSERT INTO students (birthday, id, name, room, sex) VALUES (%s, %s, %s, %s, %s)",
        rows_students
    )
    logging.info("Data is loaded into DB")
   
def extract_from_db(cur: psycopg2.extensions.cursor, query: str) -> List[Dict[str, Any]]:
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def load_to_json(data: List[Dict[str, Any]], 
                filename:str , 
                ouput_folder: str, 
                rewrite_ouput: bool) -> None:
    filepath = f"{ouput_folder}/{filename}.json"
    if (os.path.exists(filepath) and not rewrite_ouput):
        logging.info(f"File {filepath} was not rewritten")
        return
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logging.info("Data is loaded into JSON: {filepath}")

def load_to_xml(data: List[Dict[str, Any]], 
                filename: str,
                ouput_folder: str, 
                record_tag:str, 
                rewrite_ouput: bool) -> None:
    filepath=f"{ouput_folder}/{filename}.xml"
    if (os.path.exists(filepath) and not rewrite_ouput):
        logging.info(f"File {filepath} was not rewritten")
        return
    
    root = etree.Element(f"_{filename}")
    
    for item in data:
        record = etree.SubElement(root, record_tag) 
        for key, value in item.items():
            child = etree.SubElement(record, key)
            child.text = str(value)
    
    tree = etree.ElementTree(root)
    
    tree.write(
        filepath,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8"
    )
    
    logging.info(f"Data is loaded into XML: {filepath}")

def get_queries(folder_path: str) -> Dict[str, str]:
    queries = {}
    for f in os.listdir(folder_path):
        if not f.endswith(".sql"):
            continue
        filename = os.path.splitext(f)[0]
        content = open(os.path.join(folder_path, f)).read()
        queries[filename] = content
    return queries  

def fill_db(conn:psycopg2.extensions.connection, 
            cur:  psycopg2.extensions.cursor,
            rooms_path: str, 
            student_path:str) -> None:
    logging.info("=== Inserting data into DB phase ===")
    drop_tables(conn, cur)
    create_tables(cur)
    rows_rooms, rows_students = extract_from_json(rooms_path, student_path)
    load_to_db(rows_rooms, rows_students, cur)
    logging.info("=== DataBase if is filled ===\n")
    
def query_db(cur: psycopg2.extensions.cursor, 
            queries_folder: str, 
            format: str, 
            ouput_folder: str, 
            record_teg_xml: str, 
            rewrite_ouput: bool =True) -> None:
    logging.info("=== Quering DB phase ===")
    #Create indices 
    create_indices(cur)
    
    #get a list of queries 
    queries = get_queries(queries_folder)

    #Load resulsts of queires into json / xml
    os.makedirs(ouput_folder, exist_ok=True)
    
    for name, sql in queries.items():
        data = extract_from_db(cur, sql)
        if format=="json":
            load_to_json(data, name, ouput_folder, rewrite_ouput)
        else:
            load_to_xml(data, name, ouput_folder, record_teg_xml, rewrite_ouput)
    
    logging.info("=== Results of queriing DB are saved ===\n")
       
def parse_cli() -> argparse.Namespace:
    #CLI
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--students", required=True)
    parser.add_argument("--rooms", required=True)
    parser.add_argument("--format", required=True, choices=["json", "xml"])
    parser.add_argument("--queries_folder", required=True)
    parser.add_argument("--output_folder", required=True)
    parser.add_argument("--record_teg_xml", default="record")
    parser.add_argument("--allow_rewrite", choices=["yes", "no"], default="yes")
    
    return parser.parse_args()
    
def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_cli()
      
    #Set connection to DB
    conn, cur = connect_db()

    #Create tables and load data from json to them
    fill_db(conn, cur, args.rooms, args.students)

    #Query DB and and save results
    query_db(cur, 
            args.queries_folder, 
            args.format, 
            args.output_folder, 
            args.record_teg_xml,
            args.allow_rewrite == "yes"
    )
    
    #Close connection to DB
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()