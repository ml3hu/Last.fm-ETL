import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import time
import functions
import transform





# initialize listening history
def init(today_unix, yesterday_unix):
    totalPages = 99999 # dummy value

    results = []

    for i in range(1, totalPages):
        if i == 1:
            print("Requesting page " + str(i))
        else:
            print("Requesting page " + str(i) + " of " + str(totalPages))

        r = functions.lastfm_getRecent({"page": i , "to": today_unix}) 

        
        # check for errors
        if r.status_code != 200:
            print(r.text)
            break
        
        # validate response
        tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
        if functions.validate(tracks, True, today_unix, yesterday_unix):
            print("Data valid")

        # set total page number
        if i == 1:
            totalPages = int(r.json()["recenttracks"]["@attr"]["totalPages"])

        results.append(tracks)

        # check cache
        if not getattr(r, 'from_cache', False):
            time.sleep(0.25)

        if i == totalPages:
            print("Request complete")
            break
    
    

    # flatten data
    tracks = pd.concat(results)

    print("Extraction + Validation Complete")

    return tracks


def runInit():
    #time variables
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    today_unix = int(today.timestamp())
    yesterday_unix = int(yesterday.timestamp())

    # transform

    # tracks are in descending date order.
    tracks = init(today_unix, yesterday_unix)

    # stage dimensions
    date_dim = transform.getDates(tracks)
    time_of_day_dim = transform.getTimeOfDay(tracks)
    track_dim = transform.getTracks(tracks)
    artist_dim = transform.getArtists(tracks)
    artist_group_dim = transform.getArtistGroups(tracks)
    artist_group_bridge = transform.getArtistGroupBridge(artist_dim, artist_group_dim)

    #stage fact table
    listening_fact = transform.getListeningFact(tracks)


    # load
    engine = sqlalchemy.create_engine(functions.DATABASE_LOCATION)
    conn = sqlite3.connect(functions.DATABASE_NAME)
    cursor = conn.cursor()

    table_date = """
        CREATE TABLE IF NOT EXISTS date_dim (
            date_key INTEGER PRIMARY KEY NOT NULL,
            date DATE NOT NULL,
            day INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            UNIQUE (date)
        )
    """

    table_time = """
        CREATE TABLE IF NOT EXISTS time_of_day_dim (
            time_of_day_key INTEGER PRIMARY KEY NOT NULL,
            time TIME NOT NULL,
            hour INTEGER NOT NULL,
            UNIQUE (time)
        )

    """

    table_track = """
        CREATE TABLE IF NOT EXISTS track_dim (
            track_key VARCHAR(32) PRIMARY KEY NOT NULL,
            track_name VARCHAR(200) NOT NULL,
            album_name VARCHAR(200) NOT NULL,
            UNIQUE(track_name, album_name)
        )
    """

    table_artist = """
        CREATE TABLE IF NOT EXISTS artist_dim (
            artist_key VARCHAR(32) PRIMARY KEY NOT NULL,
            artist_name VARCHAR(200) NOT NULL,
            UNIQUE(artist_name)
        )
    """

    table_artist_group = """
        CREATE TABLE IF NOT EXISTS artist_group_dim (
            artist_group_key VARCHAR(32) PRIMARY KEY NOT NULL,
            artist_group_name VARCHAR(200) NOT NULL,
            UNIQUE(artist_group_name)
        )
    """

    table_bridge = """
        CREATE TABLE IF NOT EXISTS artist_group_bridge (
            artist_group_key VARCHAR(32) NOT NULL,
            artist_key VARCHAR(32) NOT NULL,
            FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key),
            FOREIGN KEY (artist_key) REFERENCES artist_dim(artist_key),
            UNIQUE(artist_group_key, artist_key)
        )
    """

    table_fact = """
        CREATE TABLE IF NOT EXISTS listening_fact (
            date_key INTEGER NOT NULL,
            time_of_day_key INTEGER NOT NULL,
            track_key VARCHAR(32) NOT NULL,
            artist_group_key VARCHAR(32) NOT NULL,
            FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
            FOREIGN KEY (time_of_day_key) REFERENCES time_of_day_dim(time_of_day_key),
            FOREIGN KEY (track_key) REFERENCES track_dim(track_key),
            FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key)
        )
    """
    cursor.execute(table_date)
    cursor.execute(table_time)
    cursor.execute(table_track)
    cursor.execute(table_artist)
    cursor.execute(table_artist_group)
    cursor.execute(table_bridge)
    cursor.execute(table_fact)

    print("Opened database successfully")

    try:
        date_dim.to_sql("date_dim", engine, index=False, if_exists='append')
        time_of_day_dim.to_sql("time_of_day_dim", engine, index=False, if_exists='append')
        track_dim.to_sql("track_dim", engine, index=False, if_exists='append')
        artist_dim.to_sql("artist_dim", engine, index=False, if_exists='append')
        artist_group_dim.to_sql("artist_group_dim", engine, index=False, if_exists='append')
        artist_group_bridge.to_sql("artist_group_bridge", engine, index=False, if_exists='append')
        listening_fact.to_sql("listening_fact", engine, index=False, if_exists='append')
    except Exception as e:
        print(e)


    conn.close()
    print("Closed database successfully")