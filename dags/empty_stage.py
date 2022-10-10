import sqlite3
from dotenv import load_dotenv
import os


# migrate data from staging to db

def empty():
    # get env variables
    load_dotenv()
    stage_name = os.getenv("STAGE_NAME")

    conn = sqlite3.connect(stage_name)
    cursor = conn.cursor()

    # attach stage to db
    print("Connecting to staging area")
    
    # template sql to insert data into table
    delete = """
        DELETE FROM {table}
    """

    print("Deleting data from staging area")

    try:
        cursor.execute(delete.format(table="listening_fact"))
        cursor.execute(delete.format(table="artist_group_bridge"))
        cursor.execute(delete.format(table="artist_dim"))
        cursor.execute(delete.format(table="artist_group_dim"))
        cursor.execute(delete.format(table="track_dim"))
        cursor.execute(delete.format(table="time_of_day_dim"))
        cursor.execute(delete.format(table="date_dim"))
    except Exception as e:
        print(e)
        raise Exception("Error emptying stage")
    
    # detach stage from db
    conn.commit()
    conn.close()
    
    print("Stage emptied successfully")
