# Sparkify Data Lake Project "Udacity Nano Degree"
## 1. Project Overview
Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.Our goal is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

### Required Process :
+ Extracts their data from S3
+ Process Data Using ***Spark***
+ Loads the data back into S3 as a set of dimensional tables

## 2. Dimension Tables Schema Design  
 
#### 1. ***songplays*** - Records in event data associated with song plays i.e. records with page NextSong
Attributes :
+ songplay_id
+ start_time
+ user_id
+ level
+ song_id
+ artist_id
+ session_id
+ location
+ user_agent

#### 2. users - users in the app
+ user_id
+ first_name
+ last_name
+ gender
+ level
#### 3. songs - songs in music database
+ song_id
+ title
+ artist_id
+ year
+ duration
#### 4. artists - artists in music database
+ artist_id
+ name
+ location
+ lattitude
+ longitude

#### 5. time - timestamps of records in songplays broken down into specific units
+ start_time
+ hour
+ day
+ week
+ month
+ year
+ weekday

## 6. ETL Pipeline
1. Create a new IAM user
2. Create clients for IAM, EC2, S3 and Redshift
3. Create Bucket and Add Out Dataset
4. Open an incoming TCP port to access the cluster endpoint
5. Run "Create_tables.py" to create the tables
6. Run "etl.py" to insert data to the tables
7. Clean up your resources


## 3. ETL Pipeline
1. Import needed libraries
2. Create Spark Session 
3. Process Song Data 
> - Extract Song_table data
> - Extract Artist_table data
5. Process Log Data
> - Extract User_table data 
> - Extract Time_Table data
> - Extract songplays_table data
7. Load the Extracted Tables to the Output Directory as a parquet file





