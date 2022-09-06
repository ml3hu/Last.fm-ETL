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
from IPython.display import clear_output
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_LOCATION = os.getenv("DATABASE_LOCATION")
USER_AGENT = os.getenv("USER_AGENT") 
API_KEY = os.getenv("API_KEY")
RECENT_TRACKS_LIMIT = 200

# Last.fm API wrapper function, takes additional parameters as array.
# needs method parameter as argument to specify which API method to call
def lastfm_get(payload):
    headers = {
        "user-agent": USER_AGENT
    }
    url = "http://ws.audioscrobbler.com/2.0/"

    # api_key is required, preferred format is json
    payload["api_key"] = API_KEY
    payload["format"] = "json"

    response = requests.get(url, headers=headers, params=payload)
    return response

# print json response
def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


# get all listening history from last.fm
requests_cache.install_cache()

totalPages = 99999 # dummy value

results = []

for i in range(1, totalPages):
    if i == 1:
        print("Requesting page " + str(i))
    else:
        print("Requesting page " + str(i) + " of " + str(totalPages))
    
    # clear the output to make things neater
    clear_output(wait = True)

    r = lastfm_get({ "method": "user.getrecenttracks", "user": USER_AGENT, "limit": RECENT_TRACKS_LIMIT, "page": i })

    
    # check for errors
    if r.status_code != 200:
        print(r.text)
        break
    
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

r0 = results[0]
r0_json = r0.json()
r0_track = r0_json['recenttracks']['track']
r0_df = pd.DataFrame(r0_track)
print(r0_df.head())