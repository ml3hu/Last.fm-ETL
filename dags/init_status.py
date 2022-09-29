from dotenv import load_dotenv
import os
import sqlite3

# check if sqlite database already exists
# if not, raise exception
def check_initialized():
    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")

    # if db does not exist, raise exception to fail airflow task
    try:
        conn = sqlite3.connect("file:" + database_name + "?mode=rw", uri=True)
        print("Database found")
        conn.close()
    except sqlite3.OperationalError:
        raise Exception("Database not found")