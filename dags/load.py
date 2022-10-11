import sqlite3
from dotenv import load_dotenv
import os


# load data from staging tables to final db
def load():
    # get env variables
    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")
    stage_name = os.getenv("STAGE_NAME")

    # connect to db
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # attach stage to db
    print("Connecting staging table to database")
    cursor.execute("ATTACH DATABASE \'"  + stage_name + "\' AS stage")
    
    # template sql to insert data from staging tables into main db tables
    insert_ignore = """
        INSERT OR IGNORE INTO main.{table} SELECT * FROM stage.{table}
    """

    # insert data
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
        # raise exception if error occurs
        raise e
    
    # detach stage from db and close connection
    conn.commit()
    cursor.execute("DETACH DATABASE stage")
    conn.close()
    
    print("Data loaded successfully")
