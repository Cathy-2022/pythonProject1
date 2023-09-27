import datetime
import requests
from dotenv import load_dotenv, dotenv_values
import os
import psycopg2 as sql

# Login SpeedStore
def conn_api():
    url = "http://10.1.1.117/BI/"
    load_dotenv()
    headers = {
        "user": os.getenv("API_USERNAME"),
        "password": os.getenv("API_PASSWORD"),
        "registration": os.getenv("API_REGCODE")
    }
    request = requests.get(url=url, headers=headers)
    print(request.status_code)
    return request.status_code

#Connect to Redshift
def conn_db():
    conn = sql.connect(
        host=os.getenv('DB_SERVER'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USERNAME'),
        password=os.getenv('DB_PASSWORD')
    )
    return conn
# Check if connect to Database
def info_conn_db():
    try:
        conn_db()
        print("DataBase connected!")
    except Exception:
        print("Expection")

#Check if txt contains header, if it has, read value from second line
def check_header(filename):
    with requests.get(filename, stream=True) as response:
        first_line = response.iter_lines().__next__()
    return first_line.decode('utf-8')[:1] not in '.-0123456789'

def check_header1(url, n, velue):
    res_file = requests.get(url)
    lines = res_file.text.splitlines()
    if len(lines) > 0:
        return lines[n].startswith(velue)
    return False

def truncate_table(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(f"truncate table (table_name)")
            print(f"(table_name) Data Deleted!")
    except Exception as err:
        print(err)









