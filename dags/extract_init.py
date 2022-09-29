import pandas as pd
from datetime import datetime
import datetime
import time
import pandas as pd
import requests_cache
import extract_functions as ef


# Extract data from Last.fm API, then store files locally
def extract():
    # set up cache to store API responses for 6 hours to speed up duplicate requests during testing
    requests_cache.install_cache('lastfm_cache', backend='sqlite', expire_after=21600)

    #time variables
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_unix = int(today.timestamp())

    print("Extracting data from Last.fm API")

    totalPages = 99999 # dummy value


    # loop through api call pages to get all historic data
    for i in range(1, totalPages):
        if i == 1:
            print("Requesting page " + str(i))
        else:
            print("Requesting page " + str(i) + " of " + str(totalPages))

        # get data from api
        r = ef.lastfm_getRecent({"page": i , "to": today_unix}) 

        
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

        # set total page number on first run
        if i == 1:
            totalPages = int(r.json()["recenttracks"]["@attr"]["totalPages"])

        # save raw data
        with open("raw/" + str(today) + " page" + str(i) + ".json", "w") as outfile:
            outfile.write(r.text)

        # check cache control header to see if we need to wait
        if not getattr(r, 'from_cache', False):
            time.sleep(0.25)

        # range(1, totalPages) is not recalculated per iteration
        # check if we are on the last page to end loop
        if i == totalPages:
            print("Request complete")
            break
    
    return True

extract()