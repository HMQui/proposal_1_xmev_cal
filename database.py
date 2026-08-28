import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    conn = duckdb.connect('artifact/duckdb/xmev.duckdb')
    
    conn.execute("SET threads = 1;")
    conn.execute("SET memory_limit = '4GB';")
    conn.execute("SET temp_directory = 'artifact/duckdb/tmp';")
    conn.execute("SET preserve_insertion_order = false;")
    
    return conn