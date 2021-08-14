import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from psycopg2.extensions import cursor
from psycopg2.extensions import connection
from typing import Callable


def process_song_file(cur:cursor, filepath:str) -> None:
    """
    Reads a `song` json file from the filepath, loads that to a
    pd.DataFrame object and then inserts the records in artist and song table.
    
        :param cur: psycopg2.extensions.cursor for the default database.
        :param filepath: path of the residing data.
    """
    
    df = pd.read_json(filepath, typ='series', convert_dates=False)
    df = df.to_frame()
    for value in df.T.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, \
        artist_name, song_id, title, duration, year = value
        artist_data = (
            artist_id, artist_name, artist_location, 
            artist_latitude, artist_longitude
        )
        cur.execute(artist_table_insert, artist_data)
        song_data = (
            song_id, title, 
            artist_id, year,
            duration
        )
        cur.execute(song_table_insert, song_data)
        

def process_log_file(cur:cursor, filepath:str) -> None:
    """
    Reads a `log` json file from the filepath loads to a
    pd.DataFrame object and then insert the file records in time,
    user and songplay table respectively.
    
    Performs filtering the page equals to NextSong and then 
    convert the timestamp column into a datatime and retrieves
    required info.
    
        :param cur: psycopg2.extensions.cursor for the default database.
        :param filepath: path of the residing data.
    """
    
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    timestamps = df['ts']
    hours = df['ts'].dt.hour
    days = df['ts'].dt.day
    weeks = df['ts'].dt.week
    months = df['ts'].dt.month
    years = df['ts'].dt.year
    weekdays = df['ts'].dt.day_name()
    
    column_labels=(
        "start_time", "hour",
        "week of year", "month",
        "year", "weekday"
    )
    
    time_df = pd.DataFrame(
        {
            "start_time":timestamps, 
            "hour":hours, 
            "day":days, 
            "week":weeks, 
            "month":months, 
            "year":years, 
            "weekday":weekdays
        }
    )
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))  

    user_df = df[[
        "userId","firstName",
        "lastName","gender","level"
    ]]
    user_df = user_df.drop_duplicates()

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = (
            row.ts, row.userId, row.level, 
            songid, artistid, row.sessionId, 
            row.location, row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur:cursor, conn:connection, filepath:str, func:Callable) -> None :
    """
    Collects all `.json` files from the dir and subdirs of the specified path,
    and executes the `func` by iterate over files, processing and commits 
    the psycopg2 connection.
    
    :param cur: psycopg2.extensions.cursor for the default database.
    :param conn: psycopg2.extensions.connection for the default database.
    :param filepath: path of the residing data.
    :param func: a callable function which is to be executed for inserting records into database.
    """
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
