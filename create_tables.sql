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

 CREATE TABLE IF NOT EXISTS  "users" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,                         
        "user_id" INTEGER, 
        "first_name" VARCHAR(128),
        "last_name" VARCHAR(128),
        "gender" VARCHAR(8),
        "level" VARCHAR(16),
        primary key(id)
    );  


 CREATE TABLE IF NOT EXISTS  "songs" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,    
        "song_id" VARCHAR(256) NOT NULL,
        "title" VARCHAR(256),
        "artist_id" VARCHAR(256),
        "year" INTEGER,
        "duration" FLOAT,
        primary key(id)                         
    );  

 CREATE TABLE IF NOT EXISTS  "artists" ( 
        "id" INTEGER IDENTITY(0,1) NOT NULL,    
        "artist_id" VARCHAR(256) NOT NULL,
        "name" VARCHAR(1024),
        "location" VARCHAR(1024),
        "latitude" FLOAT,
        "longitude" FLOAT,
        primary key(id)                         
    );  


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

