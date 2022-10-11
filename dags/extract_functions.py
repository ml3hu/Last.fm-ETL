import requests
from dotenv import load_dotenv
import os
import pandas as pd

# Last.fm API wrapper function, takes additional parameters as array.
# needs method parameter as argument to specify which API method to call
def lastfm_getRecent(payload):

    # load environment variables
    load_dotenv()

    USER_AGENT = os.getenv("USER_AGENT") 
    API_KEY = os.getenv("API_KEY")
    RECENT_TRACKS_LIMIT = 200

    # create request header
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


# validate dataframe
def validate(df, today_unix) -> bool:
    # check for empty dataframe, returns false for ShortCircuitOperator to skip downstream tasks
    if df.empty:
        print("No tracks found")
        return False

    # check primary key uniqueness
    if pd.Series(df["date.uts"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")

    # check for nulls
    # Last.fm API should not return nulls, 
    # if nulls are found, the data is invalid
    if df.isnull().values.any():
        raise Exception("Null values found.")
    
    dates = df["date.uts"].astype(int)

    # check datetime constraint
    # no tracks should have a timestamp greater than today's date
    if dates.max() >= today_unix:
        raise Exception("Initial load contains excess data.")

    return True