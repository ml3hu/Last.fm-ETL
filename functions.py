import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os

import requests_cache

load_dotenv()

DATABASE_LOCATION = os.getenv("DATABASE_LOCATION")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USER_AGENT = os.getenv("USER_AGENT") 
API_KEY = os.getenv("API_KEY")
RECENT_TRACKS_LIMIT = 200

requests_cache.install_cache('lastfm_cache', backend='sqlite', expire_after=21600)

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
def validate(df, isInit, today_unix, yesterday_unix) -> bool:
    if df.empty:
        print("No tracks found")
        return False

    # Primary Key Check
    if pd.Series(df["date.uts"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")

    # check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found.")
    
    dates = df["date.uts"].astype(int)

    # check datetime constraint
    if isInit:
        if dates.max() >= today_unix:
            raise Exception("Initial load contains excess data.")
    else:
        if dates.max() >= today_unix:
            raise Exception("Update contains data from today.")
        if dates.min() < yesterday_unix:
            raise Exception("Update contains data from before yesterday.")

    return True