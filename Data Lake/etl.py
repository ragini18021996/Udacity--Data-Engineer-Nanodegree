import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, monotonically_increasing_id, from_unixtime
from pyspark.sql.functions import year, month, dayofweek, hour, weekofyear, dayofmonth
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('IAM_user', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('IAM_user', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Creates a new or uses the existing spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Reads all `song` json file from the input folder, process them and stores them in parquet format in the output folder.
    :param spark: spark session
    :param input_data: path of the residing input data.
    :param output_data: path of the output data.
    """
    
    # get filepath to song data file
    song_data =os.path.join(input_data, 'song_data/*/*/*/*.json') 
    
    song_schema = StructType([
         StructField("artist_id", StringType()),
         StructField("artist_latitude", DoubleType()),
         StructField("artist_location", StringType()),
         StructField("artist_longitude", DoubleType()),
         StructField("artist_name", StringType()),
         StructField("duration", DoubleType()),
         StructField("num_songs", IntegerType()),
         StructField("title", StringType()),
         StructField("year", IntegerType()),
    ])
    # read song data file
    df = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table
    song_columns= ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artist_columns= ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.selectExpr(artist_columns).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Reads all `log` json file from the input folder, process them and stores them in parquet format in the output folder.
    :param spark: spark session
    :param input_data: path of the residing input data.
    :param output_data: path of the output data.
    """
    # get filepath to log data file
    log_data =os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_columns= ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level" ]
    users_table = df.selectExpr(user_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: t/1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda d: datetime.fromtimestamp(d), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    df=df.withColumn("hour", hour("start_time"))\
         .withColumn("day", dayofmonth("start_time"))\
         .withColumn("week", weekofyear("start_time"))\
         .withColumn("month", month("start_time"))\
         .withColumn("year", year("start_time"))\
         .withColumn("weekday", dayofweek("start_time"))
    
    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year" , "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs/*/*/*'))
    song_logs = df.join(song_df, (df.song == song_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    artist_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artist_song_logs = song_logs.join(artist_df, (song_logs.artist == artist_df.artist_name))
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = artist_song_logs.select(
                      col("start_time"),
                      col("userId").alias("user_id"),
                      col("level"),
                      col("song_id"),
                      col("artist_id"),
                      col("sessionId").alias("session_id"),
                      col("location"),
                      col("userAgent").alias("user_agent"),
                      col("year"),
                      col("month"),
                      ).repartition("year", "month")
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session() 
    input_data = 's3a://udacity-dend/'
    output_data = 's3://rag-data-ware-output/'
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
