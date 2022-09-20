import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import functions
import transform


# update
def update(today_unix, yesterday_unix, yesterday):
    
    print(yesterday_unix)

    print("Requesting data from " + str(yesterday))
    r = functions.lastfm_getRecent({ "from": yesterday_unix , "to": today_unix})

    # check for errors
    if r.status_code != 200:
        print(r.text)
        return
    
    # validate response
    tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
    if functions.validate(tracks, False, today_unix, yesterday_unix):
        print("Data valid, proceed to Load stage")

    return tracks

def runUpdate():
    #time variables
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    today_unix = int(today.timestamp())
    yesterday_unix = int(yesterday.timestamp())

    # transform
    tracks = update(today_unix, yesterday_unix, yesterday)

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


    insert_ignore = """
        INSERT OR IGNORE INTO {table} SELECT * FROM temp_dim
    """

    drop_temp = """
        DROP TABLE IF EXISTS temp_dim
    """

    
    try:
        print("Openening connection to database")

        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        date_dim.to_sql("date_dim", engine, index=False, if_exists='append')

        # load data to temp, then move unique records to dimension
        time_of_day_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="time_of_day_dim"))

        # reopen connection and reopen to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        track_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="track_dim"))

        # reopen connection and reopen to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_dim"))

        # reopen connection and reopen to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_group_dim.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_group_dim"))

        # reopen connection and reopen to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        artist_group_bridge.to_sql("temp_dim", engine, index=False, if_exists='replace')
        cursor.execute(insert_ignore.format(table="artist_group_bridge"))

        # reopen connection and reopen to avoid locking
        conn.close()
        conn = sqlite3.connect(functions.DATABASE_NAME)
        cursor = conn.cursor()

        # guaranteed unique rows
        listening_fact.to_sql("listening_fact", engine, index=False, if_exists='append')
    except Exception as e:
        print(e)

    # clean up temp table
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
    print("Closed database successfully")

