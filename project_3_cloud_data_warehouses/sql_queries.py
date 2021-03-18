# CONFIG
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# get file paths in S3
LOG_PATH = config.get('S3', 'LOG_PATH')
SONG_PATH = config.get('S3', 'SONG_PATH')
LOG_JSON_PATH = config.get('S3', 'LOG_JSON_PATH')
ROLE_ARN = config.get('IAM', 'ROLE_ARN')

# QUERY FUNCTIONS
def drop_table(table_name):
    """
    Drops the given table from the database
    """
    return open(r"./query_files/drop/" + table_name + ".sql", "r").read()

def create_table(table_name):
    """
    Creates the given table on the database
    """
    return open(r"./query_files/create/" + table_name + ".sql", "r").read()

def insert_table(table_name):
    """
    Inserts rows to the given table
    """
    return open(r"./query_files/insert/" + table_name + ".sql", "r").read()
    
# DROP TABLES
staging_events_table_drop = drop_table("staging_events")
staging_songs_table_drop = drop_table("staging_songs")

songplay_table_drop = drop_table("songplays")
user_table_drop = drop_table("users")
song_table_drop = drop_table("songs")
artist_table_drop = drop_table("artists")
time_table_drop = drop_table("time")

# CREATE TABLES
staging_events_table_create = create_table("staging_events")
staging_songs_table_create = create_table("staging_songs")

songplay_table_create = create_table("songplays")
user_table_create = create_table("users")
song_table_create = create_table("songs")
artist_table_create = create_table("artists")
time_table_create = create_table("time")

# STAGING TABLES
# copy query for staging tables
query = r"COPY {} FROM '{}' CREDENTIALS 'aws_iam_role={}' FORMAT AS JSON '{}' REGION 'us-west-2'"
events_list = ["staging_events", LOG_PATH, ROLE_ARN, LOG_JSON_PATH]
songs_list = ["staging_songs", SONG_PATH, ROLE_ARN, 'auto']

staging_events_copy = query.format(*events_list) + r" TIMEFORMAT AS 'epochmillisecs'" 
staging_songs_copy = query.format(*songs_list)

# FINAL TABLES
songplay_table_insert = insert_table("songplays")
user_table_insert = insert_table("users")
song_table_insert = insert_table("songs")
artist_table_insert = insert_table("artists")
time_table_insert = insert_table("time")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
