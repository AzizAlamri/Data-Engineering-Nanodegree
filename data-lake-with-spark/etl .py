import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf 
from pyspark.sql.functions import col
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import date_format
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
def create_spark_session():
    """ Scanning Spark Components """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Process the songs data files and create songs and artist dimension tables.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    """

    # get filepath to song data file
    song_data = str(input_data) + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs", partitionBy=['year', 'artist_id'], mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select([
        'artist_id',
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('lattitude'),
        col('artist_longitude').alias('longitude')
    ]).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists", mode="overwrite")
    
def process_log_data(spark, input_data, output_data):
    """Process the Sparkify event logs data and create users and times dimension tables as well as songplays fact table.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    """

    # get filepath to log data file
    log_data = str(input_data) + "log-data/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select([
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level'
    ]).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users", partitionBy=['level'], mode="overwrite")

    
    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', from_unixtime(col('ts')/1000))
    
    # expand df by adding additional time columns for later use
    df = df.withColumn('hour', hour('start_time'))
    df = df.withColumn('day', dayofmonth('start_time'))
    df = df.withColumn('week', weekofyear('start_time'))
    df = df.withColumn('month', month('start_time'))
    df = df.withColumn('year', year('start_time'))
    df = df.withColumn('weekday', dayofweek('start_time'))

    # extract columns to create time table
    time_table = df.select([
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time", partitionBy=['year','month'], mode="overwrite")

    # create an id in log data data frame
    df = df.withColumn('songplay_id', monotonically_increasing_id())

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')   
 
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title).select([
        'songplay_id',
        'start_time',
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent'),
        df.year,
        'month'
    ]).distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", partitionBy=['year','month'], mode="overwrite")
  


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
