
import sqlalchemy
import pandas as pd
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
server = "spotifyrockdb.database.windows.net"
database = "SpotifyRockDB"
# load credentials
try:
    #load_dotenv()
    #password = os.getenv("PASSWORD")
    password = os.environ["PASSWORD"]
    # set connection string
    connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:spotifyrockdb.database.windows.net,1433;Database=SpotifyRockDB;Uid=sqladmin;Pwd='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=2000;'
except Exception as e:
    logging.error("An exception occurred: Database PASSWORD not found", exc_info=False)
                                                                    
                                
try:
    # Using SQLAlcehmy engine to load data with pandas                   
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

    # get artist_ids, album_ids, track_ids lists
    try:
        query = f'SELECT artist_id FROM artists_table'
        artist_ids = pd.read_sql(query, engine)['artist_id'].to_list() 

        #query = f'SELECT album_id FROM albums_table'
        #album_ids = pd.read_sql(query, engine)['album_id'].to_list() 

        #query = f'SELECT track_id FROM tracks_table'
        #track_ids = pd.read_sql(query, engine)['track_id'].to_list()
    except Exception as e:
        # Log the exception with the logging module
        logging.error("An exception occurred: get artist_ids, album_ids, track_ids lists", exc_info=False)
        

    # Load into DB
    try:
        #df_artists_followers_table = extract_artists_followers_table(artist_ids=artist_ids)
        #df_artists_followers_table.to_sql('artists_followers_table', con=engine, if_exists='append', index=False)

        df_artists_popularity_table = extract_artists_popularity_table(artist_ids=artist_ids)
        df_artists_popularity_table.to_sql('new_demo_table', con=engine, if_exists='append', index=False)

        #df_albums_popularity_table = extract_albums_popularity_table(album_ids=album_ids)
        #df_albums_popularity_table.to_sql('albums_popularity_table', con=engine, if_exists='append', index=False)

        #df_tracks_popularity_table = extract_tracks_popularity_table(track_ids=track_ids)
        #df_tracks_popularity_table.to_sql('tracks_popularity_table', con=engine, if_exists='append', index=False)

        # Add a logging statement for successful completion
        logging.info("Code executed successfully!")
    except Exception as e:
        # Log the exception with the logging module
        logging.error("An exception occurred: Load data into DB", exc_info=False)

    # close SQLAlchemy engine
    engine.dispose()
except Exception as e:
    logging.error("An exception occurred: SQLAlcehmy engine error", exc_info=False)






#########
### CREATING DEMO TABLE to test code
'''
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