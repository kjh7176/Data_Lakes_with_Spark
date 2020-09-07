import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import StructType as St, StructField as Fld, TimestampType as Ts, \
    StringType as Str, IntegerType as Int, DoubleType as Dbl, LongType as Long

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['ACCESS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['ACCESS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads from song files, 
    transforms them into songs and artists data, 
    and writes them in parquet format.
    
    params:
    - spark: spark session object
    - input_data: input data path
    - output_data: output data path
    """
    
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # use schema when read json files
    song_schema = St([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs", mode="overwrite", \
                              partitionBy=["year", "artist_id"])
    
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", \
                                  "artist_latitude as latitude", "artist_longitude as longitude") \
                                  .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Reads from log files, 
    transforms them into users, time, and songplays data, 
    and writes them in parquet format. 
    
    params:
    - spark: spark session object
    - input_data: input data path
    - output_data: output data path
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # use schema when read json files
    log_schema = St([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Long()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Long()),
        Fld("song", Str()),
        Fld("status", Long()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.where("page='NextSong'")
    
    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", \
                                "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users", mode="overwrite")

    # change column name from ts to start_time
    time_table = df.select(col("ts").alias("start_time")).dropDuplicates()
    
    # convert datatype of start_time into datetime
    get_timestamp = udf(lambda ts: (datetime.fromtimestamp(ts//1000)), Ts())
    time_table = time_table.withColumn("start_time", get_timestamp("start_time"))
    
    # add columns to create time table
    time_table = time_table \
        .withColumn("hour", hour("start_time"))\
        .withColumn("day", date_format("start_time", "dd"))\
        .withColumn("weekofyear", weekofyear("start_time"))\
        .withColumn("month", month("start_time"))\
        .withColumn("year", year("start_time"))\
        .withColumn("weekday", dayofweek("start_time"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time", mode="overwrite")
    
    # read in song data to use for songplays and artists table
    song_df = spark.read.parquet(output_data + "songs")
    artist_df = spark.read.parquet(output_data + "artists")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
        .join(song_df, (df.song == song_df.title) & (df.length == song_df.duration))\
        .join(artist_df, song_df.artist_id == artist_df.artist_id)\
        .select(get_timestamp("ts").alias("start_time"),
                col("userId").alias("user_id"),
                df.level,
                song_df.song_id,
                artist_df.artist_id,
                col("sessionId").alias("session_id"),
                df.location,
                col("userAgent").alias("user_agent"))\
        .dropDuplicates()

    # add year and month columns for partitioning
    songplays_table = songplays_table\
        .withColumn("year", year("start_time"))\
        .withColumn("month", month("start_time"))   
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/songplays", mode="overwrite", 
                                  partitionBy=["year", "month"])


def main():
    """
    Creates a spark session.
    Reads from song files, 
    transforms them into songs and artists data, 
    and writes them in parquet format.
    Reads from log files, 
    transforms them into users, time, and songplays data, 
    and writes them in parquet format. 
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "hdfs:///user/hadoop/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
