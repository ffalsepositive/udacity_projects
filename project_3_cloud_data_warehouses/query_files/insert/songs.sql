INSERT INTO songs 
(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
SELECT  
    DISTINCT(song_id) AS song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL