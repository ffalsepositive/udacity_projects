from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from airflow.operators import CreateTablesOperator

from helpers import SqlQueries

from datetime import datetime, timedelta

default_args = {
    'owner': 'mcelik',
    'start_date': datetime(2018, 12, 31),
    'end_date': datetime(2020, 12, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('airflow_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
)

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

create_tables_task = CreateTablesOperator(
    task_id="Create_tables",
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_task = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    data_format="JSON",
    json_path="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

stage_songs_task = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    provide_context=False
)

load_songplays_task = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    query=SqlQueries.songplay_table_insert
)

load_user_task = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    query=SqlQueries.user_table_insert
)

load_song_task = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    query=SqlQueries.song_table_insert
)

load_artist_task = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    query=SqlQueries.artist_table_insert
)

load_time_task = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["users", "time", "artists", "songs", "songplays"]
    
)

                                        

start_operator >> create_tables_task

create_tables_task >> [stage_events_task, stage_songs_task]

[stage_events_task, stage_songs_task] >> load_songplays_task

load_songplays_task >> [load_user_task, load_song_task, load_artist_task, load_time_task]

[load_user_task, load_song_task, load_artist_task, load_time_task] >> run_quality_checks

run_quality_checks >> end_operator