CREATE TABLE songplays
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)