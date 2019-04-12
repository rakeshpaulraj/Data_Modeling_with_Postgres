import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Function to process records from each JSON song dataset
    
    For the given file, this function reads the JSON file contents into pandas dataframe.
    From the dataframe, song data list is framed with following 5 columns - song_id, title, artist_id, year, duration
    Then the song data list values is inserted into songs table.
    
    Similarly artist data list is framed with following 5 columns - artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
    Then the artist data list values is inserted into artists table.
    
    Parameters: 
    cur      : cursor for the open connection on sparkify database
    filepath : full path of the file location including the file name which needs to be processed
  
    Returns  : None
    '''
       
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = list(df.loc[:,['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df.loc[:,['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Function to process records from each JSON log dataset
    
    For the given file, this function reads the JSON file contents into pandas dataframe by filtering based on NextSong action.
    The file contents (pandas dataframe) is used to load time dimension, users dimension and songplays fact table.
    
    Since the ts column in file has timestamp in milliseconds format, the values are converted to datetime format.
    From the timestamp value, further time related fields such as hour, day, week, month, year, weekday are derived and populated into time data list.
    Then the time data values are inserted into time dimension table.

    For each record in the given file, user related values such as userId, firstName, lastName, gender, level are extracted and populated into user data list.
    Then the user data values are inserted into users dimension table.
    
    For each record in the given file, songplay details are retrieved. 
    Using the song name, artist name and duration dimension tables are looked upon in order to get the corresponding song_id and artist_id. 
    Then the songplay data values are inserted into songplays fact table.
    
    Parameters: 
    cur      : cursor for the open connection on sparkify database
    filepath : full path of the file location including the file name which needs to be processed
  
    Returns  : None
    '''
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column fro milliseconds format to datetime format 
    df['ts_formatted'] = pd.to_datetime(df['ts'], unit='ms')
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # populate time dataframe - Derive other time related fields and frame time data list. Add column labels and convert the time data list into time dataframe.
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict, orient='columns')
    
    # insert time data records by iterating thru each record in dataframe
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # populate user dataframe
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records by iterating thru each record in dataframe
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records by iterating thru each record in dataframe
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        # if results found, assign the output to variables or else assign None values.
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts_formatted, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Function to process JSON files from given filepath
    
    For the given filepath, all underlying JSON files are extracted and in turns invokes the given function to process each of those file contents.
    
    Parameters: 
    cur      : cursor for the opened connection on sparkify database
    conn     : active open connection on sparkify database
    filepath : file location base directory path 
    func     : function name which needs to be invoked while processing each file under the given filepath
  
    Returns  : None
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    # sort the contents
    all_files.sort()
    
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    main function to process song and log datasets
    
    Connects to sparkify database and the cursor is opened for data processing.
    Invokes functions to process song and log datasets in order to load the 4 dimension tables (songs, artists, users, time) and 1 fact table (songplays)
    
    Parameters: None  
    Returns  : None
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    
    print('ETL process completed')


if __name__ == "__main__":
    main()