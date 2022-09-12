import pandas as pd

def getDates(df):
    print("Transforming Datetime Data")

    df = df[["date.uts", "date.#text"]].copy()
    df["date.uts"] = df["date.uts"].astype(int)
    df[["date", "time"]] = df["date.#text"].str.split(', ', expand=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop(columns=["date.uts", "date.#text", "time"])
    df = df.drop_duplicates(ignore_index=True)

    
    # dates[["day", "month", "year"]] = dates["date"].str.split(' ', expand=True)
    # dates = dates.drop(columns=["date.#text"])
    #tracks[["date", "time"]] = tracks["date.#text"].str.split(', ', expand=True)
    #tracks = tracks.drop(["streamable","image", "mbid", "artist.mbid", "album.mbid", "date.#text"], axis=1)
    print(df.head())
    print(df.info())
    print(df.describe())
    # start = tracks["date.uts"].min()

    # date_dim = tracks[["date.uts", "date.#text"]].rename(columns={"date.uts": "date_key", "date.#text": "date"})

    return df
