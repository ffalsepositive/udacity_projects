INSERT INTO songplays
(
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
SELECT  
    DISTINCT(staging_events.ts) AS start_time, 
    staging_events.userId AS user_id, 
    staging_events.level AS level, 
    staging_songs.song_id AS song_id, 
    staging_songs.artist_id AS artist_id,
    staging_events.sessionId AS session_id, 
    staging_events.location AS location, 
    staging_events.userAgent AS user_agent
FROM staging_events
JOIN staging_songs
ON  staging_events.song = staging_songs.title 
AND staging_events.artist = staging_songs.artist_name
WHERE staging_events.page = 'NextSong'