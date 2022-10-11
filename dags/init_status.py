from dotenv import load_dotenv
import os
import sqlite3

# check if sqlite database already exists
# returns the name of the next airflow task for BranchPythonOperator to select
def check_initialized():
    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")

    # if exists, proceed with update task stream
    # else, proceed with initialization task stream
    try:
        conn = sqlite3.connect("file:" + database_name + "?mode=rw", uri=True)
        print("Database found")
        conn.close()
        return "update_extract"
    except sqlite3.OperationalError:
        pass

    return "initial_extract"