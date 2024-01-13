import pypyodbc as odbc
import sqlalchemy
import pandas as pd
from dotenv import load_dotenv
import os
# import get_static_tables function from extract_transform_data.py
from extract_transform_data import get_static_tables


# connect to database
# specify server and DB name
server = "spotifyrockdb.database.windows.net"
database = "SpotifyRockDB"
# load credentials
load_dotenv()
username = os.getenv("username")
password = os.getenv("password")
# set connection string
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:spotifyrockdb.database.windows.net,1433;Database=SpotifyRockDB;Uid=sqladmin;Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
# connect to databse
conn = odbc.connect(connection_string)

# Create tables

def execute_commit_sql(sql):
    # Execute sql query
    cursor = conn.cursor()
    cursor.execute(sql)
    # Commit changes to the database 
    conn.commit()
    cursor.close()


# SQL statement to insert data into the table
# for static data
sql_create_artists_table ='''
CREATE TABLE artists_table (
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    PRIMARY KEY (artist_id)
);'''
sql_create_albums_table ='''
CREATE TABLE albums_table (
    album_id VARCHAR(255),
    artist_id VARCHAR(255),
    album_name VARCHAR(255),
    album_release_date INT,
    album_total_tracks INT,
    album_image_large VARCHAR(255),
    album_image_medium VARCHAR(255),
    album_image_small VARCHAR(255),
    original_album_name VARCHAR(255),
    PRIMARY KEY (album_id),
    FOREIGN KEY (artist_id) REFERENCES artists_table(artist_id)
);'''
sql_create_tracks_table = '''
CREATE TABLE tracks_table(
    track_id VARCHAR(255),
    album_id VARCHAR(255),
    track_name VARCHAR(255),
    track_duration_ms INT,
    track_spotify_url VARCHAR(255),
    track_preview_url VARCHAR(255),
    track_duration_display VARCHAR(50),
    original_track_name VARCHAR(255),
    PRIMARY KEY (track_id),
    FOREIGN KEY (album_id) REFERENCES albums_table(album_id)
)
'''
sql_create_tracks_features_table = '''
CREATE TABLE tracks_features_table(
    track_id VARCHAR(255),
    uri VARCHAR(255),
    track_href VARCHAR(255),
    analysis_url VARCHAR(255),
    duration_ms INT,
    time_signature INT,
    danceability FLOAT,
    energy FLOAT,
    track_key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    type VARCHAR(255),
    PRIMARY KEY (track_id),
    FOREIGN KEY (track_id) REFERENCES tracks_table(track_id)
)
'''
# for dynamic data
sql_create_artists_followers_table ='''
CREATE TABLE artists_followers_table (
    date DATE,
    artist_id VARCHAR(255),
    followers INT,
    PRIMARY KEY (date, artist_id),
    FOREIGN KEY (artist_id) REFERENCES artists_table(artist_id)
);'''
sql_create_artists_popularity_table ='''
CREATE TABLE artists_popularity_table (
    date DATE,
    artist_id VARCHAR(255),
    artist_popularity INT,
    PRIMARY KEY (date, artist_id),
    FOREIGN KEY (artist_id) REFERENCES artists_table(artist_id)
);'''
sql_create_albums_popularity_table ='''
CREATE TABLE albums_popularity_table (
    date DATE,
    album_id VARCHAR(255),
    album_popularity INT,
    PRIMARY KEY (date, album_id),
    FOREIGN KEY (album_id) REFERENCES albums_table(album_id)
);'''
sql_create_tracks_popularity_table ='''
CREATE TABLE tracks_popularity_table (
    date DATE,
    track_id VARCHAR(255),
    track_popularity INT,
    PRIMARY KEY (date, track_id),
    FOREIGN KEY (track_id) REFERENCES tracks_table(track_id)
);'''

# excecute queries to create tables
execute_commit_sql(sql_create_artists_table)
execute_commit_sql(sql_create_albums_table)
execute_commit_sql(sql_create_tracks_table)
execute_commit_sql(sql_create_tracks_features_table)
execute_commit_sql(sql_create_artists_followers_table)
execute_commit_sql(sql_create_artists_popularity_table)
execute_commit_sql(sql_create_albums_popularity_table)
execute_commit_sql(sql_create_tracks_popularity_table)
conn.close()



def load_static_data_into_DB(artists_list):
    """This function loads static data into the database.
    Static data does not change over time, so it is loaded only once.

    Args:
        artists_list (list): A list of artist names to load data for.
    """
    # Using SQLAlcehmy engine to load data using pandas
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
    
    try:
        # call get_static_tables function to get static data
        df_artists_table, df_albums_table, df_tracks_table, df_tracks_features_table = get_static_tables(artists_list=artists_list)
        # Load into the database
        df_artists_table.to_sql('artists_table', con=engine, if_exists='append', index=False)
        df_albums_table.to_sql('albums_table', con=engine, if_exists='append', index=False)
        df_tracks_table.to_sql('tracks_table', con=engine, if_exists='append', index=False)
        df_tracks_features_table.to_sql('tracks_features_table', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Exception occured while loading static data: ", e)
    
    # close SQLAlchemy engine
    engine.dispose()
    


# specify artists list
artists_list = ['Pink Floyd', 'The Doors', 'Led Zeppelin', 'Queen', 'Deep Purple',
               'The Rolling Stones', 'AC/DC', 'Fleetwood Mac', 'The Beatles', 'Dire Straits', 'Nirvana',
               'Guns N\'Roses', 'Pixies', 'The Police', 'ZZ Top', 'Aerosmith', 'The Who', 
               'Bon Jovi', 'Lynyrd Skynyrd', 'Scorpions', 'U2', 'David Bowie',
                'Jimi Hendrix', 'Eric Clapton', 'Red Hot Chili Peppers', 'Eagles', 'The Animals',
                'R.E.M.', 'Radiohead', 'The Beach Boys']

# Load static data into DB
load_static_data_into_DB(artists_list=artists_list)