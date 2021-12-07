# DROP TABLES

songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplay(songplay_id serial PRIMARY KEY, start_time BIGINT, user_id int, level varchar, song_id int, artist_id int,session_id varchar, location varchar , user_agent varchar)
""")

user_table_create = ("""create table if not exists users(user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar )
""")

song_table_create = ("""create table if not exists song(song_id varchar, title varchar, artist_id varchar, year int, duration int)
""")

artist_table_create = ("""create table if not exists artist(artist_id varchar,name varchar,location varchar,latitude varchar, longitude varchar)
""")

time_table_create = ("""create table if not exists time(start_time varchar,hour varchar,day varchar,week varchar,month varchar, year varchar,weekday varchar)
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplay(start_time, user_id, level, song_id , artist_id,session_id, location,  user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""insert into users(user_id, first_name, last_name, gender, level) values(%s,%s,%s,%s,%s)
""")

song_table_insert = ("""insert into song(song_id, title, artist_id,year, duration) values(%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""insert into artist(artist_id,name,location,latitude, longitude) values(%s,%s,%s,%s,%s)
""")


time_table_insert = ("""insert into time(start_time,hour,day,week,month, year,weekday) values(%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""
SELECT song.song_id, artist.artist_id
FROM song JOIN artist ON song.artist_id = artist.artist_id
WHERE song.title=(%s) AND artist.name=(%s) and song.duration=(%s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]