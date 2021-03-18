INSERT INTO artists
(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT
    DISTINCT(artist_id) AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL