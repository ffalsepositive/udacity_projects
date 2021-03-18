INSERT INTO users 
(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
)
SELECT  
    DISTINCT(userId) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong'