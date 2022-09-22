import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import functions
import transform

# Incremental ETL processing


# Extract data from Last.fm API
def update(today_unix, yesterday_unix, yesterday):
    print("Extracting data from Last.fm API")

    print("Requesting data from " + str(yesterday))

    # get data from api
    r = functions.lastfm_getRecent({ "from": yesterday_unix , "to": today_unix})

    # check for error codes
    if r.status_code != 200:
        print(r.text)
        return
    
    # validate data
    tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
    if functions.validate(tracks, False, today_unix, yesterday_unix):
        print("Data valid, proceed to Load stage")

    print("Extraction + Validation Complete")

    return tracks


# Transform + Load
def runUpdate():
    print("Transforming data")

    #time variables
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    today_unix = int(today.timestamp())
    yesterday_unix = int(yesterday.timestamp())

    # tracks are in descending date order.
    tracks = update(today_unix, yesterday_unix, yesterday)

    # stage dimensions
    date_dim = transform.getDates(tracks)
    time_of_day_dim = transform.getTimeOfDay(tracks)
    track_dim = transform.getTracks(tracks)
    artist_dim = transform.getArtists(tracks)
    artist_group_dim = transform.getArtistGroups(tracks)
    artist_group_bridge = transform.getArtistGroupBridge(artist_dim, artist_group_dim)

    # stage fact table
    listening_fact = transform.getListeningFact(tracks)

    print("Transform complete")

    # connect to db
    engine = sqlalchemy.create_engine(functions.DATABASE_LOCATION)

    print("Loading new data into tables")

    # template sql to insert data into table
    insert_ignore = """
        INSERT OR IGNORE INTO {table} SELECT * FROM temp_dim
    """

    drop_temp = """
        DROP TABLE IF EXISTS temp_dim
    """

    # load data to tables
    # new connection needs to be established each time to avoid locking
    # data is first loaded to a temp table, then inserted into the main table to avoid duplicate data
    # without having to insert 1 row at a time from python to sqlite
    try:
        # connecting to the database file
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        date_dim.to_sql("date_dim", engine, index=False, if_exists='append')

        time_of_day_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="time_of_day_dim"))

        # reload connection to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        track_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="track_dim"))

        # reload connection to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_dim"))

        # reload connection to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_group_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_group_dim"))

        # reload connection to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_group_bridge.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_group_bridge"))

        # reload connection to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        # already contains unique rows, so no temp table needed
        listening_fact.to_sql("listening_fact", engine, index=False, if_exists='append')
    except Exception as e:
        print(e)

    # drop temp table from db
    try:
        print("Cleaning up temp table")
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        cursor.execute(drop_temp)

        print("Load stage complete")
    except Exception as e:
        print(e)

    conn.commit()
    conn.close()
    print("Data loaded successfully")