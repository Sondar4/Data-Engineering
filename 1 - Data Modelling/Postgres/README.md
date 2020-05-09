# Project 1: Create a database for Sparkify

## Problem discussion
Sparkify needs a database to **analyze** the data they have been collecting in a series of .json files. The amount of data is not very big, and we want to have flexibility for the queries because the purpose of the database is data analysis, so **postgres** will be our option. They want tables designed to optimize queries for song plays analysis.

## Solution
The solution will consist of three python files:

- create_tables.py: a python script to create the tables. This will be executed first and once.
- etl.py: a python file that when called as main will load all the .json files and insert data into the tables.
- sql_queries.py: a python file with strings defining the SQL queries that will be imported and used by the other two files.

## Database schema
Database will follow a **star** schema.

### Fact Table:
- **songplays**: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- **users**: user_id, first_name, last_name, gender, level
- **songs**: song_id, title, artist_id, year, duration
- **artists**: artist_id, name, location, latitude, longitude
- **time**: start_time, hour, day, week, month, year, weekday

## Other files
There are also two jupyter notebooks in the project. The one called test.ipynb contains some queries to check that the tables are correctly created and data correctly inserted, and the one called etl.ipynb was used to create the etl process. 
