import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, IntegerType
# Row Number
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window


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
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json( song_data ) 

    # extract columns to create songs table
    songs_table = df.select('song_id' , 'title' , 'artist_id' , 'year' , 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = song_df.selectExpr('artist_id' , 'artist_name as name' , 'artist_location as location' ,\
                                       'artist_latitude as lattitude' , 'artist_longitude as longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =  os.path.join(input_data, "log-data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.dropDuplicates((['userId'])).selectExpr('userId as user_id' , 'firstName as first_name' \
                                                                 , 'lastName as last_name' , 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # Get Starting Time From ts
    get_timestamp = F.udf(lambda x : datetime.fromtimestamp(x/1000).isoformat())
    df = df.withColumn('StartingTime' , get_timestamp('ts').cast(TimestampType()))
    
    
    # Create view to run SQL Query
    df.createOrReplaceTempView("log_view")

    # Insert each Column needed for the Time Table 
    # start_time, hour, day, week, month, year, weekday
    
    time_table =spark.sql("""
    SELECT  DISTINCT start_time,
                EXTRACT(hour FROM start_time)    AS hour,
                EXTRACT(day FROM start_time)     AS day,
                EXTRACT(week FROM start_time)    AS week,
                EXTRACT(month FROM start_time)   AS month,
                EXTRACT(year FROM start_time)    AS year,
                EXTRACT(week FROM start_time)    AS weekday
    FROM    log_view
    """)
   
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json("data/song_data/*/*/*/*.json" ) 
    # Create View to be able to run our query
    song_df.createOrReplaceTempView("song_view")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""       
            SELECT DISTINCT   se.start_time,
                              se.userId  as user_id , 
                              se.level  as level,
                              ss.song_id  as song_id,
                              ss.artist_id  as artist_id,
                              se.sessionId  as session_id,
                              se.location  as location,
                              se.userAgent  as user_agent
        FROM  log_view se JOIN song_view ss 
        ON  (se.song = ss.title) AND (se.artist = ss.artist_name) AND (ss.duration = se.length)
        
        """)
    # Now we just need to add songplays_ID for this dataframe before writing
    w = Window().orderBy(lit('A'))
    songplays_table = songplays_table.withColumn("songplay_id", row_number().over(w))
    
    # Reorder columns to put songplay_id at the first column
    songplays_table = songplays_table.select('songplay_id','start_time','user_id','level','song_id','artist_id','session_id','location','user_agent')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
