CREATE TABLE artists
(
    artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)