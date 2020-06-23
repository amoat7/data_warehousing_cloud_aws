import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"

# CREATE TABLES

staging_events_table_create= """CREATE TABLE staging_events(
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
    ts BIGINT,
    userAgent VARCHAR,
    userId INT       
    );"""

staging_songs_table_create = """ CREATE TABLE staging_songs(
    song_id VARCHAR PRIMARY KEY,
    num_songs INT, 
    artist_id VARCHAR, 
    artist_latitude FLOAT8,
    artist_longitude FLOAT8,
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    title VARCHAR, 
    duration NUMERIC, 
    year INT        
);"""

songplay_table_create = """ CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP sortkey NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR ,
    song_id VARCHAR distkey NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR ,
    user_agent VARCHAR 
);
"""

user_table_create = """ CREATE TABLE users(
    user_id INT  PRIMARY KEY ,
    first_name VARCHAR ,
    last_name VARCHAR ,
    gender VARCHAR , 
    level VARCHAR 
)
DISTSTYLE all;
"""

song_table_create = """ CREATE TABLE songs(
    song_id VARCHAR  PRIMARY KEY ,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration NUMERIC
    );
"""

artist_table_create = """ CREATE TABLE artists(
    artist_id VARCHAR  PRIMARY KEY ,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT8,
    longitude FLOAT8   
)
DISTSTYLE all;
"""

time_table_create = """ CREATE TABLE time_dim(
    start_time TIMESTAMP  PRIMARY KEY ,
    hour INT,
    day INT,
    WEEK INT,
    MONTH INT,
    year INT, 
    weekday INT

)
DISTSTYLE all;
"""

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config['S3']['LOG_JSONPATH']) 

staging_songs_copy = ("""

COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])


# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second',
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent  
            
    from staging_songs AS s
    left join staging_events AS e
    ON e.artist = s.artist_name AND  e.song = s.title
    WHERE e.page ='NextSong';
"""

user_table_insert = """
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    
    SELECT 
        DISTINCT userId,
        firstName,
        lastName,
        gender,
        level
        
    FROM staging_events
    WHERE userId IS NOT NULL;
"""

song_table_insert = """
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    
    
   FROM staging_songs
   WHERE artist_id IS NOT NULL;
"""

time_table_insert = """ 
    INSERT INTO time_dim(start_time, hour, day, WEEK, MONTH, year, weekday)
        SELECT tm,
               EXTRACT(HOUR FROM tm),
               EXTRACT(DAY FROM tm),
               EXTRACT(WEEK FROM tm),
               EXTRACT(YEAR FROM tm),
               EXTRACT(MONTH FROM tm),
               EXTRACT(ISODOW FROM tm)
        
        FROM  (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as tm FROM staging_events 
                     WHERE ts IS NOT NULL;)

"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
