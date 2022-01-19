import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE if not exists staging_events (
artist VARCHAR,
auth VARCHAR,
first_name VARCHAR,
gender VARCHAR,
itemInSession int,
last_name VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration NUMERIC,
session_id INT,
song VARCHAR,
status VARCHAR,
ts varchar,
user_agent VARCHAR,
user_id INT
)
""")

staging_songs_table_create = ("""CREATE TABLE if not exists staging_songs(
num_songs int,
artist_id VARCHAR PRIMARY KEY NOT NULL,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
song_title VARCHAR,
duration FLOAT,
year VARCHAR
)
""")

songplay_table_create = ("""CREATE TABLE if not exists songplay(
songplay_id INT PRIMARY KEY NOT NULL,
start_time varchar,
user_id INT,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INT,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""CREATE TABLE if not exists users(
user_id INT,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
)
""")

song_table_create =( """CREATE TABLE if not exists song(
song_id VARCHAR PRIMARY KEY NOT NULL,
song_title VARCHAR,
artist_id VARCHAR,
year VARCHAR,
duration FLOAT
)
""")

artist_table_create = ("""CREATE TABLE if not exists artist(
artist_id VARCHAR PRIMARY KEY NOT NULL,
artist_name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
)
""")

time_table_create =( """CREATE TABLE if not exists time(
start_time varchar PRIMARY KEY NOT NULL,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {} 
CREDENTIALS 'aws_iam_role={}'
JSON {} region 'us-west-2'
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' region 'us-west-2'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
se.ts,
se.user_id,
se.level,
ss.song_id,
ss.artist_id,
se.session_id,
ss.artist_location,
se.user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.song_title and se.artist = ss.artist_name and se.length = ss.duration)
WHERE se.page = 'NextSong'
""")

user_table_insert =( """INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
first_name,
last_name,
gender,
level FROM staging_events

""")

song_table_insert = ("""INSERT INTO song(song_id, song_title, artist_id, year, duration)
SELECT DISTINCT song_id,
song_title,
artist_id,
year,
duration FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude FROM staging_songs
"""
)
time_table_insert = ("""INSERT INTO time(start_time,hour, day, week, month, year, weekday)
SELECT DISTINCT 
ts AS start_time,
extract('hour' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
extract('day' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
extract('week' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
extract('month' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')), 
extract('year' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')),
extract('dow' from (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) 
FROM staging_events
""")
            
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,songplay_table_create,user_table_create,song_table_create,artist_table_create,time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
