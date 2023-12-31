import sqlalchemy
import pandas as pd
import time
from dotenv import load_dotenv
import os
import logging
import logging.handlers
# imort functions to extract dat from Spotify API
from extract_transform_data import extract_artists_followers_table, extract_artists_popularity_table, extract_albums_popularity_table, extract_tracks_popularity_table


# Configure the logging setup
logging.basicConfig(filename='status.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# connect to database
# specify server and DB name
#server = "spotifyrockdb.database.windows.net"
#server = "192.168.1.7"
server = "169.254.147.40"
database = "SpotifyRockDB"

# load credentials
try:
    #load_dotenv()
    #password = os.getenv("PASSWORD")
    password = os.environ["PASSWORD"]
    # set connection string
    #connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:'+server+',1433;Database='+database+';Uid=sqladmin;Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=2000;'
    #connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID=sqladmin;PWD='+password
    connection_string = (
    'Driver={ODBC Driver 18 for SQL Server};'
    'Server=tcp:' + server + ',1433;'
    'Database=' + database + ';'
    'Uid=sqladmin;'
    'Pwd=' + password + ';'
    'Encrypt=yes;'
    'TrustServerCertificate=yes;'
    'Connection Timeout=2000;')
    
except Exception as e:
    logging.error("An exception occurred: Database PASSWORD not found", exc_info=False)
                                                                    


# set SQLAlchemy engine
def set_engine(connection_string, max_retries=5, retry_delay=5):
    attempts = 0
    while attempts < max_retries:
        try:
            engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
            #engine = sqlalchemy.create_engine(f"mssql+pyodbc://scott:tiger^5HHH@mssql2017:1433/test?driver=ODBC+Driver+13+for+SQL+Server&Connect+Timeout=30")
            return engine
        except Exception as e:
            logging.error(f"An exception occurred: SQLAlcehmy engine error (Attempt {attempts + 1}/{max_retries})", exc_info=True)
            attempts += 2
            time.sleep(retry_delay)

    logging.error(f"Failed to create the SQLAlchemy engine after {max_retries} attempts.", exc_info=False)
    return None



# get artist_ids, album_ids, track_ids lists
def get_spotify_ids(engine):
    attempts = 0
    max_retries = 2
    retry_delay = 2
    while attempts < max_retries:
        try:
            query = f'SELECT artist_id FROM artists_table'
            artist_ids = pd.read_sql(query, engine)['artist_id'].to_list() 

            #query = f'SELECT album_id FROM albums_table'
            #album_ids = pd.read_sql(query, engine)['album_id'].to_list() 

            #query = f'SELECT track_id FROM tracks_table'
            #track_ids = pd.read_sql(query, engine)['track_id'].to_list()
            return artist_ids #, album_ids, track_ids 
        except Exception as e:
            logging.error(f"An exception occurred: failed to get Spotify ids (Attempt {attempts + 1}/{max_retries})", exc_info=True)
            attempts += 1
            time.sleep(retry_delay)

    # Log the exception with the logging module
    logging.error(f"Failed to get Spotify ids  after {max_retries} attempts.", exc_info=False)
    return None
    
 
# Load
def load_to_database(engine, tuple_ids):
    """Extracts new data from Spotify API, and load them into the database

    Args:
        engine (SQLAlchemy engine)
        tupl_ids (tuple): tuple of lists of spotify ids
    """      
    try:
        artist_ids = tuple_ids
        #artist_ids, album_ids, track_ids = tuple_ids
        
        #df_artists_followers_table = extract_artists_followers_table(artist_ids=artist_ids)
        #df_artists_followers_table.to_sql('artists_followers_table', con=engine, if_exists='append', index=False)

        df_artists_popularity_table = extract_artists_popularity_table(artist_ids=artist_ids)
        df_artists_popularity_table.to_sql('new_demo_table', con=engine, if_exists='append', index=False)

        #df_albums_popularity_table = extract_albums_popularity_table(album_ids=album_ids)
        #df_albums_popularity_table.to_sql('albums_popularity_table', con=engine, if_exists='append', index=False)

        #df_tracks_popularity_table = extract_tracks_popularity_table(track_ids=track_ids)
        #df_tracks_popularity_table.to_sql('tracks_popularity_table', con=engine, if_exists='append', index=False)

        # close SQLAlchemy engine
        engine.dispose()
        # Add a logging statement for successful completion
        logging.info("Code executed successfully!")
    except Exception as e:
        # Log the exception with the logging module
        logging.error("An exception occurred: Load data into DB", exc_info=False)



# Run fucntions
engine = set_engine(connection_string)
tuple_ids = get_spotify_ids(engine)
load_to_database(engine, tuple_ids)




'''
#########
### CREATING DEMO TABLE to test code

import pypyodbc as odbc
conn = odbc.connect(connection_string)

# Create tables

def execute_commit_sql(sql):
    # Execute sql query
    cursor = conn.cursor()
    cursor.execute(sql)
    # Commit changes to the database 
    conn.commit()
    cursor.close()
    
    
sql_demo_table ="CREATE TABLE new_demo_table (artist_id VARCHAR(255),artist_popularity INT,date DATE);"

execute_commit_sql(sql_demo_table)
conn.close()'''



