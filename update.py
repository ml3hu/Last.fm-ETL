from re import A
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import datetime
import sqlite3
import time
import functions

#time variables
today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - datetime.timedelta(days=1)
today_unix = int(today.timestamp())
yesterday_unix = int(yesterday.timestamp())

# update
def update():
    
    print(yesterday_unix)

    print("Requesting data from " + str(yesterday))
    r = functions.lastfm_getRecent({ "from": yesterday_unix , "to": today_unix})

    # check for errors
    if r.status_code != 200:
        print(r.text)
        return
    
    # validate response
    tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
    if functions.validate(tracks, True, today_unix, yesterday_unix):
        print("Data valid, proceed to Load stage")

    return tracks