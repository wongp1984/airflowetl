class SqlQueries:

# CREATE TABLES #######################################################################
    staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS "staging_events_table" (                          
        "event_id" BIGINT IDENTITY(0,1) NOT NULL, 
        "artist" VARCHAR(256),
        "auth" VARCHAR(256),
        "firstName" VARCHAR(128),
        "gender" VARCHAR(8),
        "itemInSession" INTEGER,
        "lastName" VARCHAR(128),
        "length" FLOAT,
        "level" VARCHAR(16),
        "location" VARCHAR(256),
        "method" VARCHAR(16),
        "page" VARCHAR(32),
        "registration" BIGINT, 
        "sessionId" INTEGER,
        "song" VARCHAR(256), 
        "status" INTEGER,
        "ts" BIGINT,
        "userAgent" VARCHAR(256), 
        "userId" INTEGER,
        primary key(event_id)
    );                            
    """)

    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS  "staging_songs_table" (                          
        "song_id" VARCHAR(256) NOT NULL,
        "num_songs" INTEGER,
        "artist_id" VARCHAR(256),
        "artist_latitude" FLOAT,
        "artist_longitude" FLOAT,
        "artist_location" VARCHAR(1024),
        "artist_name" VARCHAR(1024),
        "title" VARCHAR(256),
        "duration" FLOAT,
        "year" INTEGER,
        primary key(song_id)
    );
    """)

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS  "songplays" (                          
        "songplay_id" BIGINT NOT NULL,
        "start_time" TIMESTAMP,
        "user_id" INTEGER,
        "level" VARCHAR(16),
        "song_id" VARCHAR(256),
        "artist_id" VARCHAR(256),
        "session_id" INTEGER,
        "location" VARCHAR(256), 
        "user_agent" VARCHAR(256), 
        primary key(songplay_id)
    );   
    """)

    

    
    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS  "users" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,                         
        "user_id" INTEGER, 
        "first_name" VARCHAR(128),
        "last_name" VARCHAR(128),
        "gender" VARCHAR(8),
        "level" VARCHAR(16),
        primary key(id)
    );                    
    """)


    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS  "songs" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,    
        "song_id" VARCHAR(256) NOT NULL,
        "title" VARCHAR(256),
        "artist_id" VARCHAR(256),
        "year" INTEGER,
        "duration" FLOAT,
        primary key(id)                         
    );  
    """)

    
    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS  "artists" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,    
        "artist_id" VARCHAR(256) NOT NULL,
        "name" VARCHAR(1024),
        "location" VARCHAR(1024),
        "latitude" FLOAT,
        "longitude" FLOAT,
        primary key(id)                         
    );  
    """)

    
    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS "time" ( 
        "start_time" TIMESTAMP NOT NULL,
        "hour" INTEGER,
        "day" INTEGER,
        "week" INTEGER,
        "month" INTEGER, 
        "year" INTEGER, 
        "weekday" INTEGER,
        primary key(start_time)                         
    );  
    """)

    songplay_table_insert = ("""
        insert into songplays("songplay_id", "start_time", "user_id", "level", "song_id", 
        "artist_id", "session_id", "location", "user_agent") 
        select e.event_id, timestamp 'epoch' + (e.ts/1000) * interval '1 second' AS start_time, 
                e.userId, e.level, s.song_id, s.artist_id, e.sessionId,
                e.location, e.userAgent 
        from staging_events_table e JOIN staging_songs_table s 
        on(e.artist=s.artist_name AND e.song=s.title)
        """)



    user_table_insert = ("""
        insert into users("user_id", "first_name", "last_name", "gender", "level") 
        select userId, firstName, lastName, gender, level 
        from staging_events_table 
        """)


    song_table_insert = ("""
    insert into songs("song_id", "title", "artist_id", "year", "duration") 
    select distinct song_id, title, 
    artist_id, year, duration from staging_songs_table 
    """)

  

    artist_table_insert = ("""
    insert into artists("artist_id", "name", "location", "latitude", "longitude") 
    select distinct artist_id, artist_name, artist_location, 
    artist_latitude, artist_longitude from staging_songs_table 
    """)

   

    time_table_insert = ("""
    insert into time("start_time", "hour", "day", "week", "month", "year", "weekday") 
    select distinct 
    s.start_time as start_time,  
    extract('hour' from  s.start_time)::INTEGER as hour,
    extract('day' from  s.start_time)::INTEGER as day,
    extract('week' from  s.start_time)::INTEGER as week,
    extract('month' from  s.start_time)::INTEGER as month,
    extract('year' from  s.start_time)::INTEGER as year,
    extract('weekday' from  s.start_time)::INTEGER as weekday 
    from songplays as s
    """)

