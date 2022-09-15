import pandas as pd
import hashlib

def getDates(df):
    print("Transforming Datetime Data")

    df = df[["date.#text"]].copy()
    df["date"] = pd.to_datetime(df["date.#text"]).dt.date
    df = df.drop(columns=["date.#text"])
    df = df.sort_values(by="date")
    df = df.drop_duplicates(ignore_index=True)
    df["date_key"] = df["date"].astype(str).str.replace("-", "").astype(int)
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["day"] = pd.to_datetime(df["date"]).dt.day
    df["day_of_week"] = pd.to_datetime(df["date"]).dt.dayofweek

    return df


def getTimeOfDay(df):
    print("Transforming Time Data")

    df = df[["date.#text"]].copy()
    df["time"] = pd.to_datetime(df["date.#text"]).dt.time
    df["time_of_day_key"] = df["time"].astype(str).str.replace(":", "").astype(int)
    df["hour"] = pd.to_datetime(df["date.#text"]).dt.hour
    df = df.drop(columns=["date.#text"])
    df = df.sort_values(by="time")
    df = df.drop_duplicates(ignore_index=True)
    
    return df

def getTracks(df):
    print("Transforming Track Data")
    df = df[["name", "album.#text"]].copy()
    df = df.drop_duplicates(keep="first", ignore_index=True)
    df = df.rename(columns={"name": "track_name", "album.#text": "album_name"})

    # md5 hash of track name and album name for track_key https://docs.getdbt.com/blog/sql-surrogate-keys
    df["temp"] = df["track_name"] + df["album_name"]
    df["track_key"] = [hashlib.md5(key.encode('utf-8')).hexdigest() for key in df["temp"]]
    df = df.drop(columns=["temp"])

    return df

def getArtists(df):
    print("Transforming Artist Data")

    df = df[["artist.#text"]].copy()
    df = df["artist.#text"].str.split(", ")
    df = df.explode("artist.#text").to_frame()
    df = df.drop_duplicates(ignore_index=True)
    df = df.rename(columns={"artist.#text": "artist_name"})
    df["artist_key"] = [hashlib.md5(key.encode('utf-8')).hexdigest() for key in df["artist_name"]]

    return df

def getArtistGroups(df):
    print("Transforming Artist Group Data")

    df = df[["artist.#text"]].copy()
    df = df.drop_duplicates(ignore_index=True)
    df = df.rename(columns={"artist.#text": "artist_group_name"})
    df["artist_group_key"] = [hashlib.md5(key.encode('utf-8')).hexdigest() for key in df["artist_group_name"]]

    return df


def getArtistGroupBridge(artist_dim, artist_group_dim):
    print("Transforming Artist Group Bridge Data")

    df = artist_group_dim[["artist_group_key", "artist_group_name"]].copy()
    df["artist_group_name"] = df["artist_group_name"].str.split(", ")
    df = df.explode("artist_group_name")
    df = df.merge(artist_dim, how="left", left_on="artist_group_name", right_on="artist_name")
    df = df.drop(columns=["artist_group_name", "artist_name"])
    df = df.drop_duplicates(ignore_index=True)

    return df


def getListeningFact(tracks):
    print("Transforming Listening Fact Data")

    df = tracks[["date.#text", "name", "album.#text", "artist.#text"]].copy()
    df["date_key"] = pd.to_datetime(df["date.#text"]).dt.date
    df["date_key"] = df["date_key"].astype(str).str.replace("-", "").astype(int)
    df["time_of_day_key"] = pd.to_datetime(df["date.#text"]).dt.time
    df["time_of_day_key"] = df["time_of_day_key"].astype(str).str.replace(":", "").astype(int)
    df["track_key"] = df["name"] + df["album.#text"]
    df["track_key"] = [hashlib.md5(key.encode('utf-8')).hexdigest() for key in df["track_key"]]
    df["artist_group_key"] = [hashlib.md5(key.encode('utf-8')).hexdigest() for key in df["artist.#text"]]
    df = df.drop(columns=["date.#text", "name", "album.#text", "artist.#text"]) 

    return df