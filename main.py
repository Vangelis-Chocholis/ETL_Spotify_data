from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import pypyodbc as odbc
import pandas as pd
import time
from dotenv import load_dotenv
import os
import logging
import logging.handlers
# imort functions to extract data from Spotify API
from extract_transform_data import extract_artists_followers_table, extract_artists_popularity_table, extract_albums_popularity_table, extract_tracks_popularity_table


# Configure the logging setup
logging.basicConfig(filename='status.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# connect to database
# specify server and DB name
server = "spotifyrockdb.database.windows.net"
database = "SpotifyRockDB"

# load credentials
try:
    #load_dotenv()
    #password = os.getenv("PASSWORD")
    password = os.environ["PASSWORD"]
    # set connection string
    connection_string = (
    'Driver={ODBC Driver 18 for SQL Server};'
    'Server=tcp:' + server + ',1433;'
    'Database=' + database + ';'
    'Uid=sqladmin;'
    'Pwd=' + password + ';'
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=600;')
    
except Exception as e:
    logging.error("An exception occurred: Database PASSWORD not found", exc_info=False)
                                                                    


# connect to database using pyodbc (without SQLAlchemy)
def database_connection(connection_string, max_retries=10, retry_delay=3):
    """Connect to SQL Server using pyodbc

    Args:
        connection_string (string): The connection string to the database
        max_retries (int, optional): Max number of retries. Defaults to 10.
        retry_delay (int, optional): Number of seconds to delay between retries. Defaults to 3.

    Returns:
        odbc connection: The connection to the database
    """
    attempts = 0
    while attempts < max_retries:
        try:
            conn = odbc.connect(connection_string)
            return conn
        except Exception as e:
            #logging.error(f"An exception occurred: connect with DB failed (Attempt {attempts + 1}/{max_retries})", exc_info=False)
            attempts += 1
            time.sleep(retry_delay)

    logging.error(f"Failed to connect to the database after {max_retries} attempts. Exception raised: {e}", exc_info=False)
    return None


# set SQLAlchemy engine
def set_engine(conn, max_retries=5, retry_delay=3):
    """Creates a SQLAlchemy engine using a preexisting connection

    Args:
        conn (odbc deonnection): The connection to the database
        max_retries (int, optional): Max number of retries. Defaults to 5.
        retry_delay (int, optional): Number of seconds to delay between retries. Defaults to 3.

    Returns:
        SQLAlchemy engine: The SQLAlchemy engine
    """
    attempts = 0
    while attempts < max_retries:
        try:
            engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: conn)
            return engine
        except Exception as e:
            logging.error(f"An exception occurred: SQLAlcehmy engine error (Attempt {attempts + 1}/{max_retries})", exc_info=False)
            attempts += 1
            time.sleep(retry_delay)

    logging.error(f"Failed to create the SQLAlchemy engine after {max_retries} attempts. Exception raised: {e}", exc_info=False)
    return None



# get artist_ids, album_ids, track_ids lists
def get_spotify_ids(engine):
    """This function gets the Spotify ids from the database

    Args:
        engine (SQLAlchemy engine): The SQLAlchemy engine

    Returns:
        tuple: A tuple of lists of Spotify ids, corresponding to artists, albums and tracks.
    """
    attempts = 0
    max_retries = 3
    retry_delay = 3
    while attempts < max_retries:
        try:
            query = f'SELECT artist_id FROM artists_table'
            artist_ids = pd.read_sql(query, engine)['artist_id'].to_list() 

            query = f'SELECT album_id FROM albums_table'
            album_ids = pd.read_sql(query, engine)['album_id'].to_list() 

            query = f'SELECT track_id FROM tracks_table'
            track_ids = pd.read_sql(query, engine)['track_id'].to_list()
            
            return artist_ids, album_ids, track_ids 
        
        except Exception as e:
            logging.error(f"An exception occurred: failed to get Spotify ids (Attempt {attempts + 1}/{max_retries})", exc_info=False)
            attempts += 1
            time.sleep(retry_delay)
    # Log the exception with the logging module
    logging.error(f"Failed to get Spotify ids  after {max_retries} attempts. Exception raised: {e}", exc_info=False)
    return None
    
 
# Load
def load_to_database(engine, tuple_ids):
    """Load data into the database, using the SQLAlchemy engine and the Spotify ids

    Args:
        engine (SQLAlchemy engine): The SQLAlchemy engine
        tupl_ids (tuple): A tuple of lists of Spotify ids, corresponding to artists, albums and tracks.
    """      
    try:
        artist_ids, album_ids, track_ids = tuple_ids
        
        df_artists_followers_table = extract_artists_followers_table(artist_ids=artist_ids)
        df_artists_followers_table.to_sql('artists_followers_table', con=engine, if_exists='append', index=False)

        df_artists_popularity_table = extract_artists_popularity_table(artist_ids=artist_ids)
        df_artists_popularity_table.to_sql('artists_popularity_table', con=engine, if_exists='append', index=False)

        df_albums_popularity_table = extract_albums_popularity_table(album_ids=album_ids)
        df_albums_popularity_table.to_sql('albums_popularity_table', con=engine, if_exists='append', index=False)

        df_tracks_popularity_table = extract_tracks_popularity_table(track_ids=track_ids)
        df_tracks_popularity_table.to_sql('tracks_popularity_table', con=engine, if_exists='append', index=False)
        
        # close SQLAlchemy engine
        engine.dispose()
        # close connection
        if conn is not None and conn.connected:
            conn.close()
        # Add a logging statement for successful completion
        logging.info("The database has been updated successfully!")
    except Exception as e:
        # Log the exception
        logging.error("An exception occurred: Failed to load data into the database", exc_info=True)



# Run fucntions
# establish connection
conn = database_connection(connection_string)
# set SQLAlchemy engine
engine = set_engine(conn)
# get Spotify ids
tuple_ids = get_spotify_ids(engine)
# load data into database
load_to_database(engine, tuple_ids)



