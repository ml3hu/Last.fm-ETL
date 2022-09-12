from re import A
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import time
import functions
import transform


#time variables
today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - datetime.timedelta(days=1)
today_unix = int(today.timestamp())
yesterday_unix = int(yesterday.timestamp())


# initialize listening history
def init():
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



# transform
tracks = init()
dates = transform.getDates(tracks)


# load
# engine = sqlalchemy.create_engine(functions.DATABASE_LOCATION)
# conn = sqlite3.connect(functions.DATABASE_NAME)
# cursor = conn.cursor()

# table_date = """
#     CREATE TABLE IF NOT EXISTS date_dim (
#         date_key INTEGER PRIMARY KEY,
#         date DATE,
#         day INTEGER,
#         month INTEGER,
#         year INTEGER,
#         week INTEGER,
#         weekday INTEGER
#     )
# """

# table_time = """
#     CREATE TABLE IF NOT EXISTS time_of_day_dim (
#         time_of_day_key INTEGER PRIMARY KEY,
#         time_of_day TIME,
#         hour INTEGER
#     )

# """

# table_track = """
#     CREATE TABLE IF NOT EXISTS track_dim (
#         track_key INTEGER PRIMARY KEY,
#         track_name VARCHAR(200),
#         url VARCHAR(200),
#         album_key INTEGER,
#         album_name VARCHAR(200)
#     )
# """

# table_artist = """
#     CREATE TABLE IF NOT EXISTS artist_dim (
#         artist_key INTEGER PRIMARY KEY,
#         artist_name VARCHAR(200)
#     )
# """

# table_artist_group = """
#     CREATE TABLE IF NOT EXISTS artist_group_dim (
#         artist_group_key INTEGER PRIMARY KEY,
#         artist_group_name VARCHAR(200)
#     )
# """

# table_bridge = """
#     CREATE TABLE IF NOT EXISTS artist_group_bridge (
#         artist_group_key INTEGER,
#         artist_key INTEGER,
#         FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key),
#         FOREIGN KEY (artist_key) REFERENCES artist_dim(artist_key)
#     )
# """

# table_fact = """
#     CREATE TABLE IF NOT EXISTS listening_fact (
#         date_key INTEGER,
#         time_of_day_key INTEGER,
#         track_key INTEGER,
#         artist_group_key INTEGER,
#         FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
#         FOREIGN KEY (time_of_day_key) REFERENCES time_of_day_dim(time_of_day_key),
#         FOREIGN KEY (track_key) REFERENCES track_dim(track_key),
#         FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key),
#         PRIMARY KEY (date_key, time_of_day_key, track_key, artist_group_key)
#     )
# """
# cursor.execute(table_date)
# cursor.execute(table_time)
# cursor.execute(table_track)
# cursor.execute(table_artist)
# cursor.execute(table_artist_group)
# cursor.execute(table_bridge)
# cursor.execute(table_fact)

# print("Opened database successfully")

# try:
#     tracks.to_sql("listening_fact", engine, index=False, if_exists='append')
