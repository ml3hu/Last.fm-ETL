import pandas as pd
import sys

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

    return df

def getArtists(df):
    print("Transforming Artist Data")

    df = df[["artist.#text"]].copy()
    df = df["artist.#text"].str.split(", ")
    df = df.explode("artist.#text").to_frame()
    df = df.drop_duplicates(ignore_index=True)
    df = df.rename(columns={"artist.#text": "artist_name"})

    return df

def getArtistGroups(df):
    print("Transforming Artist Group Data")

    df = df[["artist.#text"]].copy()
    df = df.drop_duplicates(ignore_index=True)

    print(df.head())
    print(df.info())
    print(df.describe())

    return df