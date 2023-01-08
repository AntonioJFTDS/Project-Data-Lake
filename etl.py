import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, date_format
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# import in order to covert columns data type for the table songs_table
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
import pyspark.sql.types as TS

##################################################################################################

# set acces to AWS cluster
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

##################################################################################################

def create_spark_session():
    """
    - Establishes a Spark session on an AWS EMR cluster
    
    """  
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark

##################################################################################################

def songs_table(df,output_data):
    """
    - create a dataframe "songs_table" that is a copy of df but with 
    only the columns "song_id", "title" , "artist_id" ,  "year" , "duration" from "df".
    and without duplicate rows.
    
    - write "songs_table" on AWS S3 at 'output_data'
    
    """
    # create "songs_table"
    songs_table = ( 
        df.select("song_id", "title" , "artist_id" ,  "year" , "duration")
        .dropDuplicates()
    )
    
    print('songs_table:')
    songs_table.printSchema()
    
    # write songs_table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table/songs_table.parquet",partitionBy=['year','artist_id'])
    
def artists_table(df,output_data):   
    """ 
    - create a dataframe "artists_table" that is a copy of df but with only
    the columns "artist_id","artist_name","artist_location", "artist_latitude" ,
    "artist_longitude" from "df", and without duplicate rows.
    (some columns will be rename)
    
    - write "artists_table" on AWS S3 at 'output_data'
    
    """
    # create "artists_table"
    artists_table = ( 
        df.select("artist_id","artist_name","artist_location", "artist_latitude","artist_longitude")
        .dropDuplicates()
        .toDF("artist_id","name","location", "latitude" ,"longitude")
    )

    print('artists_table:')
    artists_table.printSchema()
    
    # write artists_table to parquet files
    artists_table.write.parquet(output_data + "artists_table/artists_table.parquet")

def users_table(df,output_data):
    """
    - create a dataframe "users_table" that is copy of df but contains only
    the columns "userId","firstName","lastName", "gender" ,"level" from "df",
    and without duplictate rows.
    (some columns will be rename)
    
    - write "users_table" on AWS S3 at 'output_data'
    
    """
    # create "users_table"
    users_table = (
        df.select("userId", "firstName" , "lastName" ,  "gender" , "level")
        .dropDuplicates()
        .toDF("user_id","first_name","last_name", "gender" ,"level")
    )

    print('users_table:')
    users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/users_table.parquet")   
    
def time_table(df,output_data):    
    """
    - create a dataframe "time_table" that contains each unique value from the "df.ts_timestamp",
    and add the to "time_table" the columns "hour","day","week","month","year","weekday"
    
    - write "time_table" on AWS S3 at 'output_data'
    
    """
    # create "time_table"
    time_table = (
        df.select("ts_timestamp")
        .toDF("start_time")
        .dropDuplicates()
        .withColumn("hour",hour("start_time"))
        .withColumn("day",dayofmonth("start_time"))
        .withColumn("week",weekofyear("start_time"))
        .withColumn("month",month("start_time"))
        .withColumn("year",year("start_time"))
        .withColumn("weekday",date_format(col("start_time"), "E"))
    )
    
    print('time_table:')
    time_table.printSchema()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table/time_table.parquet",partitionBy=['year','month'])
    
def songplays_table(spark,df,input_data,output_data):     
    """
    - create a dataframe "song_df" that contains the details of all the songs and their respective artists 
    ps: this information is contained in the json files located at "input_data/song_data/A/A/"
    
    - create the dataframe "songplays_table" by joinning
    the dataframes "df" and "song_df" on atrist names and song titles.
    and without duplictate rows.
    (some columns will be rename)
    
    - write "song_df" on AWS S3 at 'output_data' 
    
    """
    
    # create 'song_df' from the json files located at "input_data/song_data/*/*/"
    song_data_path = os.path.join(input_data,'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data_path)
    
    # create "songplays_table" 
    songplays_table = (
        df.join(song_df,
                [df.artist == song_df.artist_name,
                df.song == song_df.title],
                "inner")
        .select("userId","ts_timestamp","level",
                "song_id","artist_id","sessionId",
                "location","userAgent",year('ts_timestamp'),
                month('ts_timestamp'))
        .dropDuplicates()
        .toDF("user_id","ts_timestamp","level",
              "song_id","artist_id","session_id",
              "location","user_agent","year",
              "month")
    )
    
    print('songplays_table:')
    songplays_table.printSchema()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table/songplays_table.parquet",partitionBy=['year','month'])

##################################################################################################

def process_song_data(spark, input_data, output_data):   
    """
    - create a dataframe "df" that contains the details of all the songs and their respective artists 
    ps: this information is contained in the json files located at "input_data/song_data/A/A/"
    
    - run the 2 functions songs_table(), artists_table() that respectivelly 
    create and write to in AWS S3 at 'output_data', the spark dataframe `songs_table`
    and `artists_table`
    
    """    
    # before create "df" set the approriate data types for each columns
    song_dataSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int()),
        ])
    
    # create 'df' from the json files located at "input_data/song_data/*/*/"
    song_data_path = os.path.join(input_data, 'song_data/A/A/A/*.json')
    df = spark.read.json(song_data_path, schema=song_dataSchema)
    
    print('df:')
    df.printSchema()
    
    # create a dataframe "songs_table" from 'df' and write a copy at 'output_data'
    songs_table(df,output_data)
    
    # create a dataframe "artists_table" from 'df' and write a copy at 'output_data'
    artists_table(df,output_data)   

def process_log_data(spark, input_data, output_data):
    """
    - create a dataframe "df" that contains the details of all the user logs ( "userId","ts","song",etc..)
    ps: this information is contained in the json files located at "input_data/log_data/*/*/"
    
    - run the 3 function time_table(), users_table(), and songplays_table() 
    that respectivelly create and write to in AWS S3 at 'output_data', 
    the spark dataframe `time_table`, `users_table` and `songplays_table`
    
    """    
    # create 'df' from the json files located at "input_data/log_data/*/*/"
    log_data_path = os.path.join(input_data, 'log_data/*/*/*.json') 
    df = spark.read.json(log_data_path) 

    # filter 'df' by actions for song plays
    df = df.where(df.page == "NextSong")
    
    # add to 'df' a column "ts_timestamp" of type "timestamp". 
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), TS.TimestampType())
    df = df.withColumn("ts_timestamp", get_timestamp(df.ts))
    
    # create a dataframe "users_table" from df and write a copy at 'output_data'
    users_table(df,output_data)
    
    # create a dataframe "time_table" from df and write a copy at 'output_data'
    time_table(df,output_data)
    
    # create a dataframe "songplays_table" from df and some of the information located at input_data 
    # then write a copy of "songplays_table" at 'output_data'
    songplays_table(spark,df,input_data,output_data)  

##################################################################################################

def main():    
    """
    - establishes a Spark session on an AWS EMR cluster
    
    - set the locations of our input data and our output data respectivelly
    in the variables 'input_data' and 'output_data'
    
    - run the function 'process_song_data' to create the Spark dataFrames 
    `song_table` and `artists_table`and run the function 'process_log_data' 
    to create the Spark dataFrames `time_table`, `users_table` and `songplays_table`
    
    """    
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://XXXXXXXXXX/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()