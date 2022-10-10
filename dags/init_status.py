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
        return "update_extract"
    except sqlite3.OperationalError:
        pass

    return "initial_extract"