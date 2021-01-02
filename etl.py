import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType # needed for udf return type
import datetime # needed for python function to return datetimestamp 


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
    Summary : Procedure to process Song data from S3 song files and write song and Artist parquet files back to S3
    
    Parameters
    spark - The spark session creted in the main function
    input_data - The location of the root folder on S3 under which all the json files are stored. 
    output_data - The location of the root folder on S3 under which all the processed parquet files will be stored.
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*'
    
    # read song data file
    #df = spark.read.csv(song_data)  -- assuming data is in json format
    df = spark.read.json(song_data)
    
    
    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table =  spark.sql('''
                            SELECT DISTINCT song_id , artist_id ,  title , year , duration 
                            FROM song_data  
                            where title is not null 
                        ''')
    
    # write songs table to parquet files partitioned by year and artist                                                                              
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(os.path.join(output_data,'songs.parquet') )

    # extract columns to create artists table
    artists_table = spark.sql('''
                                SELECT DISTINCT artist_id , artist_name ,  artist_latitude , artist_location , artist_longitude 
                                FROM song_data  
                                where artist_name is not null 
                              ''')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data,'artists.parquet') )
    
    print ('Song extract completed')  


def process_log_data(spark, input_data, output_data):
    """
    Summary : Procedure to process log data from S3 song files and extract and write User, time and songdata parquet files back to S3
    
    Parameters
    spark - The spark session creted in the main function
    input_data - The location of the root folder on S3 under which all the json files are stored. 
    output_data - The location of the root folder on S3 under which all the processed parquet files will be stored.
    
    Python functions needed to convert epoch time in logs to datetimestamp to extract all time relevant information.
    
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*'

    # read log data file
    df_Log_data = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_filter = df_Log_data.filter("page = 'NextSong'")
    
    df_filter.createOrReplaceTempView("Log_data")
    
    # extract columns for users table    
    users_table = spark.sql('''
                            SELECT DISTINCT userId , firstName ,  lastName , gender , level 
                            FROM Log_data  
                            where firstName is not null 
                            ''')
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data,'Users.parquet'))

    # create timestamp column from original timestamp column
    def _to_timestamp(s):
        """
        This function converts an epoch time to datetimestamp
        Returns datetimestamp
        
        """
        return datetime.datetime.fromtimestamp(s/1000) 
    
    
    #get_timestamp = udf(_to_timestamp, TimestampType())
    get_timestamp = udf(lambda s: datetime.datetime.fromtimestamp(s/1000)  , TimestampType())
  
    df_filter.withColumn("TimestampType", get_timestamp("ts")).createOrReplaceTempView("Log_data_Timestamp")
    
    # extract columns to create time table
    time_table = spark.sql('''
                            SELECT DISTINCT ts as epoch , TimestampType as StartTime , year(TimestampType) as Year ,
                                        month(TimestampType) as month , day(TimestampType) as day,
                                         hour(TimestampType) as hour , weekofyear(TimestampType) as weekofyear , weekday(TimestampType) as weekday
                            FROM Log_data_Timestamp  
                            where ts is not null 
                            ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("Year","month").mode("overwrite").parquet(os.path.join(output_data,'time.parquet'))

    # read in song data to use for songplays table
    song_output = output_data + 'songs.parquet'
    song_df = spark.read.parquet(song_output)
    song_df.createOrReplaceTempView("song_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT  row_number() over (order by TimestampType) as songplay_id, TimestampType as start_time , 
                                           userId , level , s.song_id , s.artist_id ,
                                           sessionId, location, userAgent , year(TimestampType) as year , month(TimestampType) as month 
                                FROM Log_data_Timestamp l  INNER JOIN song_data s ON l.song = s.title 

                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(os.path.join(output_data,'songplays.parquet'))

    print ('Log extract completed')  
    
def main():
    """
    Summary 
    Main function which will be called and will invoke sub procedures to load and process log data from S3 bucket.
    
    This procedure will first invoke and create a Spark session 
    Variable input_data will store location of raw log files in S3 bucket 
    Variable output_data will store the location of the files where the final processed parquet files will be stored.
    
    """
    spark = create_spark_session()
    input_data =   "s3a://udacity-dend/"  ## "udacity-dend/"  ## 
    output_data =  "s3a://udacity-dend/Output121220/"   ## "udacity-dend/Output/"   ##  
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print ('Spark activity completed')    

if __name__ == "__main__":
    main()
