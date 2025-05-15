import psycopg2
from dotenv import load_dotenv
import os
import json


def extract(cur, query):
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def load_to_json(data: dict, filepath: str):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

def get_queries(folder_path):
    queries = {}
    for f in os.listdir(folder_path):
        if not f.endswith(".sql"):
            continue
        filename = os.path.splitext(f)[0]
        content = open(os.path.join(folder_path, f)).read()
        queries[filename] = content
    return queries  

def main():
    load_dotenv()

    conn = psycopg2.connect(
        host = os.getenv("PG_HOST"),
        port = os.getenv("PG_PORT"),
        database = os.getenv("PG_DB"),
        user = os.getenv("PG_USER"),
        password = os.getenv("PG_PASSWORD")
    )

    cur = conn.cursor()
    
    queries = get_queries("queries")

    os.makedirs("output", exist_ok=True)
    
    for name, sql in queries.items():
        data = extract(cur, sql)
        filename = f"output/{name}.json"
        load_to_json(data, filename)
        print(f"Loaded data to {filename}")
    
    cur.close()
    conn.close()
    
    
if __name__ == "__main__":
    main()