INSERT INTO time
(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
SELECT
    DISTINCT(start_time) AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dayofweek FROM start_time) AS weekday
FROM songplays