import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Extracts songs and artists info from song_data. Inserts the data into the related tables.

        Parameters:
                    cur (object): Cursor object of psycopg2.connection
                    filepath (str): song_data filepath

        Returns:
                    None
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Extracts songplays, users and time info from log_data. Inserts the data into the related tables.

        Parameters:
                    cur (object): Cursor object of psycopg2.connection
                    filepath (str): log_data filepath

        Returns:
                    None
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=="NextSong"].reset_index(drop=True)

    # convert timestamp column to datetime
    df["start_time"] = pd.to_datetime(df["ts"], unit="ms")
    t = df["start_time"]
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month_name(), t.dt.year, t.dt.day_name())
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songId, artistId = results
        else:
            songId, artistId = None, None

        # insert songplay record
        songplay_data = (row.start_time, row.userId, row.level, songId, artistId, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Extracts info from JSON file repo. Inserts the data into the related tables by using the related method.


            Parameters:
                    cur (object): Cursor object of conn
                    conn (object): Connection object of psycopg2 library
                    filepath (str): Filepath string of JSON file repository
                    func (method): Predefined method: process_log_data or process_song_data

            Returns:
                    None
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

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
    Main method. It operates each step.
    
        Parameters:
                None
                
        Returns:
                None
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()