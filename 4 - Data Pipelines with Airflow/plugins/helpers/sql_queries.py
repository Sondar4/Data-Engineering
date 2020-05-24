class SqlQueries:

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
        )
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
        )
    """)

    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
        songplay_id text PRIMARY KEY,
        start_time TIMESTAMP sortkey NOT NULL,
        user_id BIGINT NOT NULL,
        level VARCHAR(4), 
        song_id VARCHAR(50) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        session_id BIGINT,
        location VARCHAR(100) distkey,
        user_agent VARCHAR(250)
        )
    """)

    user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
        user_id BIGINT PRIMARY KEY distkey sortkey,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(1),
        level VARCHAR(4)
        )
    """)

    song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(250),
        artist_id VARCHAR(50) distkey,
        year INT,
        duration REAL
        )
    """)

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(50) PRIMARY KEY distkey,
        name VARCHAR(200),
        location VARCHAR(200),
        latitude REAL,
        longitude REAL
        )
    """)

    time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP NOT NULL sortkey,
        hour INT,
        day INT,
        week INT,
        month INT distkey, 
        year INT,
        weekday INT
        )
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (songplay_id, start_time, user_id, level,
            song_id, artist_id, session_id, location, user_agent)
        SELECT
                md5(events.session_id || events.start_time) songplay_id,
                events.start_time, 
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE events.start_time IS NOT NULL
                AND events.user_id IS NOT NULL
                AND songs.song_id IS NOT NULL
                AND songs.artist_id IS NOT NULL

    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT distinct user_id, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
