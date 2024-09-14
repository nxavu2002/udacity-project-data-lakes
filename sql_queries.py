import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplays";
user_table_drop = "DROP TABLE IF EXISTS users";
song_table_drop = "DROP TABLE IF EXISTS songs";
artist_table_drop = "DROP TABLE IF EXISTS artists";
time_table_drop = "DROP TABLE IF EXISTS time";

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              VARCHAR
);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
  song_id VARCHAR ,
  artist_id VARCHAR ,
  artist_name VARCHAR ,
  artist_location VARCHAR ,
  artist_latitude FLOAT ,
  artist_longitude FLOAT ,
  duration FLOAT ,
  num_songs INT ,
  title VARCHAR ,
  year INT 
);
""")

songplay_table_create = (""" CREATE TABLE songplays (
  songplay_id INT  DEFAULT nextval('songplays_seq') NOT NULL PRIMARY KEY,
  start_time TIMESTAMP,
  user_id VARCHAR,
  level VARCHAR,
  song_id VARCHAR,
  artist_id VARCHAR,
  session_id INT,
  location VARCHAR,
  user_agent VARCHAR,
  FOREIGN KEY (start_time) REFERENCES time (start_time),
  FOREIGN KEY (user_id) REFERENCES users (user_id),
  FOREIGN KEY (song_id) REFERENCES songs (song_id),
  FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
  user_id VARCHAR PRIMARY KEY,
  first_name VARCHAR,
  last_name VARCHAR,
  gender VARCHAR,
  level VARCHAR
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
  song_id VARCHAR PRIMARY KEY,
  title VARCHAR,
  artist_id VARCHAR,
  year INT,
  duration FLOAT,
  FOREIGN KEY (artist_id) REFERENCES artists (artist_id))
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  location VARCHAR,
  latitude FLOAT,
  longitude FLOAT
);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP PRIMARY KEY,
  hour INT, 
  day INT,
  week INT,
  month INT,
  year INT,
  weekday INT
);
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {}
iam_role {}
region 'us-west-2'
FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs from {}
iam_role {}
region 'us-west-2'
FORMAT AS JSON 'auto'
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
    userId as user_id,
    level,
    song_id,
    artist_id,
    sessionId as session_id,
    location,
    userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
WHERE se.page ='NextSong'
""")

user_table_insert = (""" INSERT INTO user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as  user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
""")

song_table_insert = (""" INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = (""" INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT date(ts) as start_time
    EXTRACT(hour FROM ts) as hour
    EXTRACT(day FROM ts) as day
    EXTRACT(week FROM ts) as week
    EXTRACT(month FROM ts) as month
    EXTRACT(year FROM ts) as year
    CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END weekday
""") 

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
