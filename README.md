### Project: Leveraging spark to extract and process log files in S3
Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The data will be processed using spark and then written back to S3 in the form of fact and dimension table parquet files . 

---
Description of each output file after it is processed by spark.

## Dimensions

 **1. Songs.parquet** : This file will contain the list of all songs  . The file will be populated using spark and stored as parquet file.
   The parquet file is partitioned by year and then artist id for easy and quick retrieval of data. 
   
 **2. Users.parquet** This dimention file will store all metadata of the users . This file will grow in size as the user base of the app increases . The file will be populated using spark and stored as parquet file. 

 **3. artists.parquet:** This file will contain the list of all artists  . The file will be populated using spark and stored as parquet file.


**4. time.parquet:** This dimention file will store all metadata of the dates received via the log files .The file will be populated using spark and stored as parquet file.   The parquet file is partitioned by year and then month for easy and quick retrieval of data. The file will contain the date in epoch and timestamp format.

### Fact Tables

**1. songplays.parquet:** This Fact file will store all song related event data .The file will be populated using spark and stored as parquet file.
   The parquet file is partitioned by year and then month based off the starttime of the event for easy and quick retrieval of data.
This file will have foreign key references to songs , artists , users  and date dimensions as only the related id is stored in this file.  

---

### ERD Diagram

![ERD Diagram](DBDiagram.PNG)

---

The above parquet files helps us to retrieve data quickly for song related queries. Some of which are mentioned below

* Determine the Most fequently heard songs over a time period eg like Top 50 etc .
* Find the users and locations who heard a particular song .
* Determine the type of songs users have been listening to over a certain period of time 

## How TO use ##
**Prerequisites:**
* Open the config file ( dl.cfg) , which is present in the same path and add the AWS access credentials. The AWS credentials are necessary to access the S3 bucket.
* Within the ety.py script, the variables input_data and output_data need to be modified to specify the name of the S3 bucket
and the location where the output parquet files will be stored. For eg
    * input_data =   "s3a://udacity-dend/"  
    * output_data =  "s3a://udacity-dend/Output121220/"   

### Files in the Repo: ###
* **dl.cfg**: Config file that stores the AWS credentials.
* **etl.py**: This script loads data (log and song metadata) from S3 bucket and processes data using spark , and then writes the Dimensions and the Fact parquet files back to S3.

Executing the python file `etl.py` will load and procees the files. 







             

