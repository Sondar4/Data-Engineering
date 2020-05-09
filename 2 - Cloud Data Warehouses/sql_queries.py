import configparser


# --- CONFIG ---

config = configparser.ConfigParser()
config.read('dwh.cfg')

# --- DROP TABLES ---

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# --- CREATE TABLES ---

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    artist text,
    auth text,
    firstname text,
    gender text,
    iteminsession int,
    lastname text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration numeric,
    session_id int,
    song text,
    status int,
    ts bigint,
    useragent text,
    user_id int
    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration DOUBLE PRECISION,
    year int
    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP sortkey NOT NULL,
    user_id BIGINT NOT NULL,
    level VARCHAR(4), 
    song_id VARCHAR(50) NOT NULL,
    artist_id VARCHAR(50) NOT NULL,
    session_id BIGINT,
    location VARCHAR(100) distkey,
    user_agent VARCHAR(250)
    );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id BIGINT PRIMARY KEY distkey sortkey,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(1),
    level VARCHAR(4)
    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(250),
    artist_id VARCHAR(50) distkey,
    year INT,
    duration REAL
    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(50) PRIMARY KEY distkey,
    name VARCHAR(200),
    location VARCHAR(200),
    latitude REAL,
    longitude REAL
    );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL sortkey,
    hour INT,
    day INT,
    week INT,
    month INT distkey, 
    year INT,
    weekday INT
    );
""")

# --- STAGING TABLES ---

staging_events_copy = ("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off
    JSON '{}';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# --- FINAL TABLES ---

# e.song may be NULL but s.title cannot
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT (timestamp 'epoch' + e.ts/1000 * interval '1 second') as start_time,
               e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.useragent
        FROM staging_events e
            LEFT JOIN staging_songs s
            ON e.song = s.title
        WHERE e.page = 'NextSong'
            AND start_time IS NOT NULL
            AND user_id IS NOT NULL
            AND song_id IS NOT NULL
            AND artist_id IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT user_id, firstname, lastname, gender, level
        FROM staging_events
        WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location,
                        artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
         SELECT start_time,
                EXTRACT(h from start_time) as hour,
                EXTRACT(d from start_time) as day,
                EXTRACT(w from start_time) as week,
                EXTRACT(mon from start_time) as month,
                EXTRACT(y from start_time) as year,
                EXTRACT(doy from start_time) as weekday
         FROM songplays;
""")

# --- QUERY LISTS ---

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
