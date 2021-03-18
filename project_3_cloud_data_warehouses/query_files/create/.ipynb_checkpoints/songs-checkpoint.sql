CREATE TABLE songs
(
    song_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    duration FLOAT
)