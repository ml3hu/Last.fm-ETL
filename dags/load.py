import sqlite3
from dotenv import load_dotenv
import os


# migrate data from staging to db

def load():
    # get env variables
    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")
    stage_name = os.getenv("STAGE_NAME")

    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # attach stage to db
    print("Connecting staging table to database")
    cursor.execute("ATTACH DATABASE \'"  + stage_name + "\' AS stage")
    
    # template sql to insert data into table
    insert_ignore = """
        INSERT OR IGNORE INTO main.{table} SELECT * FROM stage.{table}
    """

    print("Loading new data into tables")

    try:
        cursor.execute(insert_ignore.format(table="date_dim"))
        cursor.execute(insert_ignore.format(table="time_of_day_dim"))
        cursor.execute(insert_ignore.format(table="track_dim"))
        cursor.execute(insert_ignore.format(table="artist_dim"))
        cursor.execute(insert_ignore.format(table="artist_group_dim"))
        cursor.execute(insert_ignore.format(table="artist_group_bridge"))
        cursor.execute(insert_ignore.format(table="listening_fact"))
    except Exception as e:
        print(e)
        raise Exception("Error loading data into tables")
    
    # detach stage from db
    conn.commit()
    cursor.execute("DETACH DATABASE stage")
    conn.close()
    
    print("Data loaded successfully")
