import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import TimestampType, StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, DateType as Date

from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.functions import monotonically_increasing_id

#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes the song data of sparkify and creates
    facts/dimensions via spark and saves them to our data lake afterwards
	Arguments:
	    spark {SparkSession}: Spark session to launch the program
	    input_data {str}: location (local/s3) where the (root) input song data resides
	    output_data {str}: location (local/s3) where the (root) output files should be written
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Str()),
        Fld("artist_longitude", Str()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),
    ])

    # since schema can not infered automatically, we need to specify it beforehand
    df_song = spark.read.json(song_data, schema=songSchema)
    df_song.cache()

    # extract columns to create songs table
    songs_table = df_song.filter(df_song.song_id != '') \
        .select(['song_id',
                 'title',
                 'artist_id',
                 'year',
                 'duration']) \
        .dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    output_song_data = f"{output_data}song_data/"
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_song_data)

    # extract columns to create artists table
    artists_table = df_song.filter(df_song.artist_id != '') \
        .selectExpr(['artist_id',
                     'artist_name as name',
                     'artist_location as location',
                     'artist_latitude as latitude',
                     'artist_longitude as longitude']) \
        .dropDuplicates(['artist_id'])

    # write artists table to parquet files
    output_artist_data = f"{output_data}artist_data/"
    artists_table.write.mode('overwrite').parquet(output_artist_data)


def process_log_data(spark, input_data, output_data):
    """
    This function processes the log data of sparkify and creates
    facts/dimensions via spark and saves them to our data lake afterwards
	Arguments:
	    spark {SparkSession}: Spark session to launch the program
	    input_data {str}: location (local/s3) where the (root) input log data resides
	    output_data {str}: location (local/s3) where the (root) output files should be written
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    df_log.cache()

    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table
    users_table = df_log.filter(df_log.userId != '') \
        .selectExpr(['cast(userId as int) user_id',
                     'firstName as first_name',
                     'lastName as last_name',
                     'gender',
                     'level']) \
        .dropDuplicates(['user_id'])

    # write users table to parquet files
    output_users_data = f"{output_data}users_data/"
    users_table.write.mode('overwrite').parquet(output_users_data)

    # create timestamp column from original timestamp column
    df_log = df_log.withColumn("parsed_ts", F.from_unixtime(df_log.ts / 1000))

    # extract columns to create time table
    time_table = df_log.selectExpr("ts as start_time",
                                   "hour(parsed_ts) as hour",
                                   "day(parsed_ts) as day",
                                   "weekofyear(parsed_ts) as week",
                                   "month(parsed_ts) as month",
                                   "year(parsed_ts) as year",
                                   "dayofweek(parsed_ts) as weekday")

    # write time table to parquet files partitioned by year and month
    output_time_data = f"{output_data}time_data/"
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_time_data)

    # extract columns from joined song and log datasets to create songplays table
    # load parquet song data that was written beforehand
    artist_table_location = "{}artist_data/".format(output_data)
    song_table_location = "{}song_data/".format(output_data)

    df_artist_table = spark.read.parquet(artist_table_location)
    df_song_table = spark.read.parquet(song_table_location)

    df_artist_table.createOrReplaceTempView("artist_table")
    df_song_table.createOrReplaceTempView("song_table")

    df_log.createOrReplaceTempView("log_data")
    time_table.createOrReplaceTempView("time_table")

    songplays_table = spark.sql("""
    SELECT
        t.start_time as start_time,
        CAST(e.userId AS int) as user_id,
        e.level,
        s.song_id, 
        a.artist_id,
        CAST(e.sessionId AS int) session_id,
        e.location,
        e.userAgent as user_agent,
        t.year,
        t.month
    FROM log_data e, song_table s, artist_table a, time_table t 
    WHERE a.name = e.artist
        AND s.title = e.song
        AND s.duration = e.length
        AND e.page = 'NextSong'
        AND t.start_time = e.ts
    """)

    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    output_songplays_data = f"{output_data}songplays_data/"
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_songplays_data)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-output-1337/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
