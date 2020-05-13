import os
import configparser
from datetime import datetime
from time import strftime, localtime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id

songplays_cols = [
    "song_id",
    "artist_id",
    "userId",
    "level",
    "sessionId",
    "location",
    "userAgent",
    "parsed_ts"
]

songs_cols = [
    "song_id",
    "title",
    "artist_id",
    "year",
    "duration"
]

artists_cols = [
    "artist_id",
    "artist_name",
    "artist_location",
    "artist_latitude",
    "artist_longitude"
]

users_cols = [
    "userId",
    "firstName",
    "lastName",
    "gender",
    "level"
]

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Load songs data, process it and save it in parquet files.
        Two tables are created: songs and artists.
    """
    
    # Load data
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    df = spark.read.json(song_data)

    # Songs table
    songs_table = df.select(*songs_cols).distinct()
    songs_table.write.partitionBy('year','artist_id').parquet(f"{output_data}/songs.parquet",mode="overwrite")

    # Artists table
    artists_table = df.select(*artists_cols).distinct()
    artists_table.write.parquet(f"{output_data}/artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
        Load logs data, process it and save it in parquet files. This function also
        uses data from songs table created in process_song_data().
        
        Three tables are created: users, time and songplays.
    """
    
    # Load data
    log_data = f'{input_data}/log_data/*/*/*.json'
    df = spark.read.json(log_data)
    df = df.filter(col('page') == 'NextSong')

    # Users table
    users_table = df.select(*users_cols).distinct()
    users_table.write.parquet(f"{output_data}/users.parquet", mode="overwrite")

    # Time table
    # - Extract data from start time timestamp
    @udf
    def parseTimestamp(ts):
        try: dt = strftime('%Y-%m-%d %H:%M:%S', localtime(ts/1000.0))
        except: dt = None
        return dt
    df = df.withColumn("parsed_ts", parseTimestamp("ts"))
    
    time_table = df.select("parsed_ts").where(col("parsed_ts").isNotNull()).distinct()
    
    getHour = udf(lambda dt: dt.hour)
    getDay = udf(lambda dt: dt.day)
    getWeek = udf(lambda dt: int(dt.strftime("%V")))
    getMonth = udf(lambda dt: dt.month)
    getYear = udf(lambda dt: dt.year)
    getWeekday = udf(lambda dt: dt.weekday())

    time_table = time_table.withColumn("hour", getHour("parsed_ts"))
    time_table = time_table.withColumn("day", getDay("parsed_ts"))
    time_table = time_table.withColumn("week", getWeek("parsed_ts"))
    time_table = time_table.withColumn("month", getMonth("parsed_ts"))
    time_table = time_table.withColumn("year", getYear("parsed_ts"))
    time_table = time_table.withColumn("weekday", getWeekday("parsed_ts"))

    time_table.write.partitionBy('year','month').parquet(f"{output_data}/time.parquet", mode="overwrite")

    # Songplays table
    # - Data from songs table is needed in this step
    song_df = spark.read.parquet(f'{output_data}/songs.parquet')

    song_df.alias('song_df')
    df.alias('df')
    songplays_table = df.join(song_df, df.song == song_df.title) \
                        .select(*songplays_cols)
    songplays_table = songplays_table.withColumn("year", getYear("parsed_ts"))
    songplays_table = songplays_table.withColumn("month", getMonth("parsed_ts"))
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    songplays_table.write.partitionBy('year','month').parquet(f"{output_data}/songplays.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = ""
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
