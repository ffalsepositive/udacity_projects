import os
import configparser

from pyspark.sql import SparkSession

from datetime import datetime as D
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

# get AWS Credentials with proper permissions
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark Session.
    
    Returns 
    -------
        spark : sparksession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data): 
    """
    Processes song data and writes the songs and the artists table into specified S3 bucket in parquet format.
    
    Parameters
    -------
        spark: object
            Spark Session object to handle the Spark Processes
        
        input_data: str
            The location of the files to read from S3 Bucket
        
        output_data: str
            The location of the files to write into S3 Bucket
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(songs_cols).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+'songs/')

    # extract columns to create artists table
    artists_cols = ["artist_id",  "artist_name as name",
                    "artist_location as location",
                    "artist_latitude as latitude",
                    "artist_longitude as longitude"]
    
    artists_table = df.selectExpr(artists_cols).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Processes log data and writes the users, the time, and the songplays table into specified S3 bucket in parquet format.
    
    Parameters
    -------
        spark: object
            Spark Session object to handle the Spark Processes
        
        input_data: str
            The location of the files to read from S3 Bucket
        
        output_data: str
            The location of the files to write into S3 Bucket
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*events.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_cols = ["userId as user_id", "firstName as first_name",
                  "lastName as last_name", "gender",  "level"]
    
    users_table = df.selectExpr(users_cols).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users/")
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: D.fromtimestamp(int(x/1000)), T.TimestampType())
    df = df.withColumn("start_time", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select("start_time") \
                   .withColumn("hour", F.hour("start_time")) \
                   .withColumn("day", F.dayofmonth("start_time")) \
                   .withColumn("week", F.weekofyear("start_time")) \
                   .withColumn("month", F.month("start_time")) \
                   .withColumn("year", F.year("start_time")) \
                   .withColumn("weekday", F.date_format('start_time', 'EEEE')).drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+'time/')
    
    # read in song data to use for songplays table
    songs_table = spark.read.parquet(output_data+"songs/")
    songs_table = songs_table.selectExpr(["song_id", "artist_id as s_artist_id", "title"])
    artists_table = spark.read.parquet(output_data+"artists/")
    artists_table = artists_table.select(["artist_id", "location", "name"]) 
    
    song_df = songs_table.join(artists_table, songs_table.s_artist_id==artists_table.artist_id, "inner")
    
    cols = ["start_time", "userId as user_id", "level", "sessionId as session_id", 
            "userAgent as user_agent", "song", "artist"]
    df = df.selectExpr(cols) \
           .withColumn("songplay_id",  F.monotonically_increasing_id()) \
           .withColumn("month", F.month("start_time")) \
           .withColumn("year", F.year("start_time"))
    
    df = df.join(song_df, (df.song==song_df.title) & (df.artist==song_df.name) , "left")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_cols = ["songplay_id", "start_time", "user_id", "level",  "song_id", "artist_id", 
                      "session_id", "location", "user_agent", "year", "month"]
    songplays_table = df.select(songplays_cols)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+'songplays/')
    
    
def main():
    spark = create_spark_session()
    input_data = config["S3"]["INPUT_DATA"]
    output_data = config["S3"]["OUTPUT_DATA"]
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()