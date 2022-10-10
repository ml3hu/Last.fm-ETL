import sqlite3
import os
import csv
from dotenv import load_dotenv

def to_csv():

    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")

    # connect to db
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    tables = ["date_dim", "time_of_day_dim", "track_dim", "artist_dim", "artist_group_dim", "artist_group_bridge", "listening_fact"]

    for table in tables:
        cursor.execute("SELECT * FROM " + table)
        data = cursor.fetchall()
        with open("/home/ml3hu/Documents/Last.fm-ETL/dags/csv/" + table + ".csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow([i[0] for i in cursor.description])
            writer.writerows(data)

    conn.close()
