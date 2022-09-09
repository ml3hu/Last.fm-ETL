from re import A
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import math
import requests_cache
import time
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_LOCATION = os.getenv("DATABASE_LOCATION")
USER_AGENT = os.getenv("USER_AGENT") 
API_KEY = os.getenv("API_KEY")
RECENT_TRACKS_LIMIT = 200

requests_cache.install_cache('lastfm_cache', backend='sqlite', expire_after=21600)

#time variables
today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - datetime.timedelta(days=1)
today_unix = int(today.timestamp())
yesterday_unix = int(yesterday.timestamp())

# Last.fm API wrapper function, takes additional parameters as array.
# needs method parameter as argument to specify which API method to call
def lastfm_getRecent(payload):
    headers = {
        "user-agent": USER_AGENT
    }
    url = "http://ws.audioscrobbler.com/2.0/"

    # api_key is required, preferred format is json
    payload["api_key"] = API_KEY
    payload["format"] = "json"
    payload["method"] = "user.getrecenttracks"
    payload["user"] = USER_AGENT
    payload["limit"] = RECENT_TRACKS_LIMIT

    response = requests.get(url, headers=headers, params=payload)
    return response

# print json response
def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


# validate dataframe
def validate(df) -> bool:
    if df.empty:
        print("No tracks found")
        return False

    # Primary Key Check
    if pd.Series(df["date"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")

    # check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found.")

    print("Data valid, proceed to Load stage")
    return True

# initialize listening history
def init():
    totalPages = 99999 # dummy value

    results = []

    for i in range(1, totalPages):
        if i == 1:
            print("Requesting page " + str(i))
        else:
            print("Requesting page " + str(i) + " of " + str(totalPages))

        r = lastfm_getRecent({"page": i , "to": today_unix})

        
        # check for errors
        if r.status_code != 200:
            print(r.text)
            break
        
        # check if dataframe is empty
        tracks = pd.DataFrame(r.json()['recenttracks']['track'])
        if tracks.empty:
            print("No tracks found")
            return

        # set total page number
        if i == 1:
            totalPages = int(r.json()["recenttracks"]["@attr"]["totalPages"])

        results.append(r)

        # check cache
        if not getattr(r, 'from_cache', False):
            time.sleep(0.25)

        if i == totalPages:
            print("Request complete")
            break
    
    # clean data
    frames = [pd.DataFrame(r.json()['recenttracks']['track']) for r in results]
    tracks = pd.concat(frames)
    tracks = tracks.drop(["streamable","image"], axis=1)

    print(tracks.head())
    print(tracks.info())
    print(tracks.describe())

# init()

# update
def update():
    
    print(yesterday_unix)

    print("Requesting data from " + str(yesterday))
    r = lastfm_getRecent({ "from": yesterday_unix , "to": today_unix})

    if r.status_code != 200:
        print(r.text)
        return
    
    tracks = pd.DataFrame(r.json()['recenttracks']['track'])

    # check if dataframe is empty
    if tracks.empty:
        print("No tracks found")
        return

    tracks = tracks.drop(["streamable","image"], axis=1)
    # jprint(r.json())
    print(tracks.head())
    # print(tracks.info())
    # print(tracks.describe())

init()
