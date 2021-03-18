# Cloud Data Warehouses on AWS

## Introduction

Sparkify, which is a streaming music start-up,  wants to move their music data to the cloud environment. 
The data is in S3 Bucket, as log and song data. Datasets are in JSON metadata format.
In this project, we will create an ETL pipeline in order to copy the data from S3 bucket to the Redshift Cluster to be hosted on. 

## Dataset

Sparkify has two different dataset on AWS S3 Bucket: Song & Log Data.

### Song Dataset 

This dataset is a small sample of real data from Million Song Dataset[1]. The files are in JSON format and contains song, artist and duration info.

### Log Dataset

This dataset contains the user activities. Each event is logged as JSON format and contains user information. 


## Table Design for Redshift Cluster

We used STAR SCHEMA for the table design. It consists of one fact table and four dimension tables. 

All the queries are stored in _query_files_ folder. The folder consists of three subfolders: _Create_, _Drop_ and _Insert_. Related table queries are stored in the related subfolder.

### Fact Table

**Songplays** : Records in the event data associated with the played songs. 
- **Table Columns :** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables 

**Users** : Contains user info.
- **Table Columns :** user_id, first_name, last_name, gender, level

**Songs** : Contains song info.
- **Table Columns :** song_id, title, artist_id, year, duration

**Artists** : Contains artist info.
- **Table Columns :** artist_id, name, location, lattitude, longitude
  
**Time** : Contains time info.
- **Table Columns :** start_time, hour, day, week, month, year, weekday
  
## How to run

1. Fill the AWS Field in the __dwh.cfg__ file.
2. Run __create_cluster.py__ by using the information in __dwh.cfg__. 
3. Wait until it is finished. When the cluster is available, it prints the Host and IAM Role ARN Information.
4. Copy these information and paste it into the related fields in __dwh.cfg__.
5. Run __create_tables.py__ in order to create tables on Redshift Cluster.
6. Run __etl.py__ to copy data from S3 Bucket to Redshift Cluster.

[1] http://millionsongdataset.com/
