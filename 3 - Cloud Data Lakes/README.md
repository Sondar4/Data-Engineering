# Project 4: A Data Lake for Sparkify

## Problem discussion
A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in **S3**, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we will build an ETL pipeline to move and transform that data from S3 with **Spark** into a data Lake in another S3 bucket with dimensional tables for the analytics team to continue finding insights in what songs their users are listening to. Data files will be stored in parquet format, which is a free and open-source column-oriented data storage format. This format also allows us to save data partitioned by columns.

## Solution
The solution will consist of a python etl in *etl.py* that executes the whole process. First, song data is loaded, processed and loaded back into S3 to create artists and songs tables. Second, log data is loaded, and processed together with some data from the previously created tables to create users, time and songplays tables.

## Database schema
The analytic database will follow a **star** schema, with a facts table called songplays, that will store song reproductions, and four dimensions.

### Fact Table:
- **songplays**: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- **users**: user_id, first_name, last_name, gender, level
- **songs**: song_id, title, artist_id, year, duration
- **artists**: artist_id, name, location, latitude, longitude
- **time**: start_time, hour, day, week, month, year, weekday

## Other files
There's also a dl.cfg with the keys to acces Amazon EMR.