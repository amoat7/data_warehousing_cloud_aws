import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXITS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXITS songplays;"
user_table_drop = "DROP TABLE IF EXITS users;"
song_table_drop = "DROP TABLE IF EXITS songs;"
artist_table_drop = "DROP TABLE IF EXITS artists;"
time_table_drop = "DROP TABLE IF EXITS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT, 
    lastName VARCHAR,
    length NUMERIC, 
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT8,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts FLOAT8 ,
    userAgent VARCHAR,
    userId INT       
    );""")

staging_songs_table_create = (""" CREATE TABLE staging_songs(
    song_id VARCHAR PRIMARY KEY,
    num_songs INT, 
    artist_id VARCHAR, 
    artist_lattitude FLOAT8,
    artist_longitude FLOAT8,
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    title VARCHAR, 
    duration NUMERIC, 
    year INT        
);""")

songplay_table_create = (""" CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP sortkey NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR distkey NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = (""" CREATE TABLE users(
    user_id INT NOT NULL PRIMARY KEY ,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR, 
    level VARCHAR,
)
DISTSTYLE all;
""")

song_table_create = (""" CREATE TABLE songs(
    song_id VARCHAR NOT NULL PRIMARY KEY ,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration NUMERIC
    );
""")

artist_table_create = (""" CREATE TABLE artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY ,
    name VARCHAR,
    location VARCHAR,
    lattitude FLOAT8,
    longitude FLOAT8
    
)
DISTSTYLE all;
""")

time_table_create = (""" CREATE TABLE time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY ,
    hour INT,
    day INT,
    WEEK INT,
    MONTH INT,
    year INT, 
    weekday INT

)
DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(CONFIG["S3"]["LOG_DATA"], CONFIG["IAM_ROLE"]["ARN"]))

staging_songs_copy = ("""

COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(CONFIG["S3"]["SONG_DATA"], CONFIG["IAM_ROLE"]["ARN"]))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    
    SELECT DISTINCT TO_TIMESTAMP(e.ts::TIMESTAMP/1000),
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent  
            
    from staging_songs as s
    left join staging_events as e
    on e.artist = s.artist_name and  e.song = s.title
""")

user_table_insert = ("""
    
    
    
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
