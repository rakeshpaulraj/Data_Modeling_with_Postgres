# ------------------------------------------------------
# DROP TABLES (If exists already)
# ------------------------------------------------------
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# ------------------------------------------------------
# CREATE TABLES
# ------------------------------------------------------

# users dimension table
# primary Key: user_id

user_table_create = """
create table users (
     user_id integer, 
     first_name varchar, 
     last_name varchar, 
     gender varchar, 
     level varchar,
     primary key(user_id)
)"""

# songs dimension table
# primary Key: songs_id

song_table_create = """
create table songs (
    song_id varchar, 
    title varchar, 
    artist_id varchar, 
    year integer, 
    duration float,
    primary key(song_id)
)"""

# artists dimension table
# primary Key: artist_id

artist_table_create = """
create table artists (
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float,
    primary key(artist_id)
)"""

# time dimension table
# primary Key: start_time

time_table_create = """
create table time(
    start_time timestamp, 
    hour smallint, 
    day smallint, 
    week smallint, 
    month smallint, 
    year smallint, 
    weekday smallint,
    primary key(start_time)
)"""


# songplays fact table
# primary Key: songplay_id

songplay_table_create = """
create table songplays(
    songplay_id serial, 
    start_time timestamp not null, 
    user_id integer not null, 
    level varchar not null, 
    song_id varchar, 
    artist_id varchar, 
    session_id integer, 
    location varchar, 
    user_agent varchar,
    primary key(songplay_id)
)"""

# ------------------------------------------------------
# INSERT RECORDS
# ------------------------------------------------------

# users dimension table
user_table_insert = """
insert into users (user_id, first_name, last_name, gender, level) values(%s, %s, %s, %s,%s)
on conflict(user_id)
do
    update
    set level = excluded.level
"""

# songs dimension table
song_table_insert = """
insert into songs (song_id, title, artist_id, year, duration) values(%s, %s, %s, %s,%s)
on conflict(song_id)
do nothing
"""

# artists dimension table
artist_table_insert = """
insert into artists (artist_id, name, location, latitude, longitude) (select %s, nullif(%s,'NaN'), nullif(%s,'NaN'), nullif(%s,'NaN'), nullif(%s,'NaN'))
on conflict(artist_id)
do nothing
"""

# time dimension table
time_table_insert = """
insert into time (start_time, hour, day, week, month, year, weekday ) values(%s, %s, %s, %s, %s, %s, %s)
on conflict(start_time)
do nothing
"""

# songplays fact table
songplay_table_insert = (""" 
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
""")

# ------------------------------------------------------
# FIND SONGS
# ------------------------------------------------------
song_select = (""" 
select 
      s.song_id
    , a.artist_id
from songs s
inner join artists a
    on s.artist_id = a.artist_id
where s.title = %s
and a.name = %s
and s.duration = %s
""")

# ------------------------------------------------------
# QUERY LISTS
# ------------------------------------------------------
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
