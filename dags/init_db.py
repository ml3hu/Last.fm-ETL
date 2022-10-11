from dotenv import load_dotenv
import os
import sqlite3

# helper function to build db
def build_db(name):
    # connect to db
    conn = sqlite3.connect(name)
    cursor = conn.cursor()

    # define db schema
    # unique constraints are placed to filter out duplicate data when appending from stage to final db
    
    # dimension table schema
    table_date = """
        CREATE TABLE IF NOT EXISTS date_dim (
            date_key INTEGER PRIMARY KEY NOT NULL,
            date DATE NOT NULL,
            day INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            UNIQUE (date)
        )
    """

    table_time = """
        CREATE TABLE IF NOT EXISTS time_of_day_dim (
            time_of_day_key INTEGER PRIMARY KEY NOT NULL,
            time TIME NOT NULL,
            hour INTEGER NOT NULL,
            UNIQUE (time)
        )

    """

    table_track = """
        CREATE TABLE IF NOT EXISTS track_dim (
            track_key VARCHAR(32) PRIMARY KEY NOT NULL,
            track_name VARCHAR(200) NOT NULL,
            album_name VARCHAR(200) NOT NULL,
            UNIQUE(track_name, album_name)
        )
    """

    table_artist = """
        CREATE TABLE IF NOT EXISTS artist_dim (
            artist_key VARCHAR(32) PRIMARY KEY NOT NULL,
            artist_name VARCHAR(200) NOT NULL,
            UNIQUE(artist_name)
        )
    """

    table_artist_group = """
        CREATE TABLE IF NOT EXISTS artist_group_dim (
            artist_group_key VARCHAR(32) PRIMARY KEY NOT NULL,
            artist_group_name VARCHAR(200) NOT NULL,
            UNIQUE(artist_group_name)
        )
    """

    table_bridge = """
        CREATE TABLE IF NOT EXISTS artist_group_bridge (
            artist_group_key VARCHAR(32) NOT NULL,
            artist_key VARCHAR(32) NOT NULL,
            FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key),
            FOREIGN KEY (artist_key) REFERENCES artist_dim(artist_key),
            UNIQUE(artist_group_key, artist_key)
        )
    """

    # fact table schema
    table_fact = """
        CREATE TABLE IF NOT EXISTS listening_fact (
            date_key INTEGER NOT NULL,
            time_of_day_key INTEGER NOT NULL,
            track_key VARCHAR(32) NOT NULL,
            artist_group_key VARCHAR(32) NOT NULL,
            FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
            FOREIGN KEY (time_of_day_key) REFERENCES time_of_day_dim(time_of_day_key),
            FOREIGN KEY (track_key) REFERENCES track_dim(track_key),
            FOREIGN KEY (artist_group_key) REFERENCES artist_group_dim(artist_group_key)
        )
    """

    # run create table queries
    cursor.execute(table_date)
    cursor.execute(table_time)
    cursor.execute(table_track)
    cursor.execute(table_artist)
    cursor.execute(table_artist_group)
    cursor.execute(table_bridge)
    cursor.execute(table_fact)

    # close connection
    conn.close()

# create staging layer and final database(db)
def initialize_database():

    # get env variables
    load_dotenv()
    database_name = os.getenv("DATABASE_NAME")
    stage_name = os.getenv("STAGE_NAME")

    # call helper function to build db
    build_db(database_name)
    build_db(stage_name)
