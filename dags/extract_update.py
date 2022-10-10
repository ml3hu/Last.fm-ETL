import pandas as pd
from datetime import datetime
import datetime
import pandas as pd
import requests_cache
import extract_functions as ef


# Extract data from Last.fm API, then store files locally
def update_extract():
    # set up cache to store API responses for 6 hours to speed up duplicate requests during testing
    requests_cache.install_cache('lastfm_cache', backend='sqlite', expire_after=21600)

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
    tracks = pd.DataFrame(pd.json_normalize(r.json()['recenttracks']['track']))
    if ef.validate(tracks, today_unix):
        print("Data valid")
    else:
        # for short circuiting rather than failing task
        return False

    # save raw data
    with open("/home/ml3hu/Documents/Last.fm-ETL/dags/raw/" + str(today) + " page1.json", "w") as outfile:
        outfile.write(r.text)

    print("Request complete")
    return True
