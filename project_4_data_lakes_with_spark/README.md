# Data Lakes with Apache Spark & AWS EMR

## Introduction

Sparkify, which is a streaming music start-up, wants to analyse their music data in the cloud environment. 
The data is in S3 Bucket, as log and song data. Datasets are in JSON metadata format.
In this project, we will create an ETL pipeline in order to copy the data from S3 bucket to EMR, by using Spark, create tables and write the data into S3 by using partitions and parquet format. 

## Dataset

Sparkify has two different dataset on AWS S3 Bucket: Song & Log Data.

- **Song Dataset:** 
    - This dataset is a small sample of real data from [Million Song Dataset](http://millionsongdataset.com/). The files are in JSON format and contains song, artist and duration info.
    - The song data folder has a structure as the following: __song_data/\*/\*/\*/song_id.json__ in JSON format.
        - Example: song_data/A/A/B/TRAABJL12903CDCF1A.json
    - The dataset is located on __"s3://udacity-dend/"__
    
- **Log Dataset:** 
    - This dataset contains the user activities. Each event is logged as JSON format and contains user information. 
    - The log data folder has a structure as the following: __log_data/\<year>/\<month>/\<date>-events.json__ in JSON format.
        - Example: log_data/2018/11/2018-11-13-events.json
    - The dataset is located on __"s3://udacity-dend/"__

## Tables

| Table Name | Table Columns                                                                                 | Tabel Design | Explanation                                                 |
|------------|-----------------------------------------------------------------------------------------------|--------------|-------------------------------------------------------------|
| songplays  | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent | Fact         | Records in the event data associated with the played songs. |
| songs      | song_id, title, artist_id, year, duration                                                     | Dimension    | Contains song info.                                         |
| artists    | artist_id, name, location, latitude, longitude                                                | Dimension    | Contains artist info.                                       |
| users      | user_id, first_name, last_name, gender, level                                                 | Dimension    | Contains user info.                                         |
| time       | start_time, hour, day, week, month, year, weekday                                             | Dimension    | Contains time info.                                         |
  
## How to Run

1. Fill the AWS Credentials Field in the __dl.cfg__ file with credentials that have S3 read&write access.
2. Fill the S3 Field in the __dl.cfg__ file with input and output S3 bucket path.
3. Run __etl.py__ in order to read, create and write the tables.
