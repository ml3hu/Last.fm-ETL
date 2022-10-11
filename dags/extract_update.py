import pandas as pd
from datetime import datetime
import datetime
import pandas as pd
import requests_cache
import extract_functions as ef


# Extract previous day's data from Last.fm API, then store files locally as raw data
# Assumes user does not listen to more than 200 tracks per day
def update_extract():
    # set up cache to store API responses for 6 hours to speed up duplicate requests during testing
    # uncomment line below to enable caching
    # requests_cache.install_cache('lastfm_cache', backend='sqlite', expire_after=21600)

    #time variables
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    today_unix = int(today.timestamp())
    yesterday_unix = int(yesterday.timestamp())

    print("Extracting data from Last.fm API")
    
    # request data from previous day
    print("Requesting data from " + str(yesterday))
    r = ef.lastfm_getRecent({"from": yesterday_unix , "to": today_unix}) 

    # check for errors codes
    if r.status_code != 200:
        raise Exception(r.text)
    
    # validate data
    # ef.validate() will return false if no tracks are found
    # other data validation errors will raise an exception and fail the task during the ef.validate() call
    tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
    if ef.validate(tracks, today_unix):
        print("Data valid")
    else:
        # returning false will short circuit the task and skip all downstream tasks
        return False

    # save raw data
    # change file path to match your local environment
    with open("/home/ml3hu/Documents/Last.fm-ETL/dags/raw/" + str(today) + " page1.json", "w") as outfile:
        outfile.write(r.text)

    print("Request complete")
    
    # returning true to ShortCircuitOperator will allow the task to continue to downstream tasks
    return True
