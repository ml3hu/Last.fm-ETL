import sqlite3
from dotenv import load_dotenv
import os

# clear staging area of previous data
def empty():
    # get env variables
    load_dotenv()
    stage_name = os.getenv("STAGE_NAME")

    # connect to staging area
    conn = sqlite3.connect(stage_name)
    cursor = conn.cursor()

    print("Connecting to staging area")
    
    # template sql to remove data from tables
    delete = """
        DELETE FROM {table}
    """

    # delete all data from staging tables in reverse order of foreign key dependencies
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
        # fail task if error occurs by raising exception
        print(e)
        raise Exception("Error emptying stage")
    
    # detach stage from db
    conn.commit()
    conn.close()
    
    print("Stage emptied successfully")
