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
    'end_date': datetime(2019, 3, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('movie_recommendation_dag',
          default_args=default_args,
          description='Load and transform Movie Recommendation Data from S3 to Redshift with Airflow',
          schedule_interval="@monthly"
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

stage_ratings_task = StageToRedshiftOperator(
    task_id="Stage_ratings",
    dag=dag,
    table="staging_ratings",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="ratings",
    data_format="PARQUET"
)

stage_movies_task = StageToRedshiftOperator(
    task_id="Stage_movies",
    dag=dag,
    table="staging_movies",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="movies",
    data_format="PARQUET"
)

stage_genres_task = StageToRedshiftOperator(
    task_id="Stage_genres",
    dag=dag,
    table="staging_genres",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="genres",
    data_format="PARQUET"
)

stage_production_companies_task = StageToRedshiftOperator(
    task_id="Stage_production_companies",
    dag=dag,
    table="staging_production_companies",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="production_companies",
    data_format="PARQUET"
)

stage_production_countries_task = StageToRedshiftOperator(
    task_id="Stage_production_countries",
    dag=dag,
    table="staging_production_countries",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="production_countries",
    data_format="PARQUET"
)

stage_spoken_languages_task = StageToRedshiftOperator(
    task_id="Stage_spoken_languages",
    dag=dag,
    table="staging_spoken_languages",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="spark-out-data",
    s3_key="spoken_languages",
    data_format="PARQUET"
)

load_user_movie_ratings_task = LoadFactOperator(
    task_id='Load_user_movie_ratings_fact_table',
    dag=dag,
    table='user_movie_ratings',
    redshift_conn_id='redshift',
    query=SqlQueries.user_movie_ratings_insert
)

load_movies_task = LoadDimensionOperator(
    task_id='Load_movies_dim_table',
    dag=dag,
    table='movies',
    redshift_conn_id='redshift',
    query=SqlQueries.movies_insert
)

load_genres_task = LoadDimensionOperator(
    task_id='Load_genres_dim_table',
    dag=dag,
    table='genres',
    redshift_conn_id='redshift',
    query=SqlQueries.genres_insert
)

load_production_companies_task = LoadDimensionOperator(
    task_id='Load_production_companies_dim_table',
    dag=dag,
    table='production_companies',
    redshift_conn_id='redshift',
    query=SqlQueries.production_companies_insert
)

load_production_countries_task = LoadDimensionOperator(
    task_id='Load_production_countries_dim_table',
    dag=dag,
    table='production_countries',
    redshift_conn_id='redshift',
    query=SqlQueries.production_countries_insert
)

load_spoken_languages_task = LoadDimensionOperator(
    task_id='Load_spoken_languages_dim_table',
    dag=dag,
    table='spoken_languages',
    redshift_conn_id='redshift',
    query=SqlQueries.spoken_languages_insert
)

load_release_dates_task = LoadDimensionOperator(
    task_id='Load_release_dates_dim_table',
    dag=dag,
    table='release_dates',
    redshift_conn_id='redshift',
    query=SqlQueries.release_dates_insert
)

load_ratings_task = LoadDimensionOperator(
    task_id='Load_ratings_dim_table',
    dag=dag,
    table='ratings',
    redshift_conn_id='redshift',
    query=SqlQueries.ratings_insert
)

load_rating_times_task = LoadDimensionOperator(
    task_id='Load_rating_times_dim_table',
    dag=dag,
    table='rating_times',
    redshift_conn_id='redshift',
    query=SqlQueries.rating_times_insert
)

run_staging_quality_checks = DataQualityOperator(
    task_id='Run_staging_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["staging_movies", "staging_genres", "staging_production_companies", 
            "staging_production_countries", "staging_spoken_languages", "staging_ratings"]
    
)

run_dimension_quality_checks = DataQualityOperator(
    task_id='Run_dimension_table_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["movies", "ratings", "production_companies", "release_dates", 
            "genres", "production_countries", "spoken_languages", "rating_times"]
    
) 


run_fact_quality_checks = DataQualityOperator(
    task_id='Run_fact_table_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=["user_movie_ratings"]
) 

start_operator >> create_tables_task

create_tables_task >> [stage_genres_task,
                       stage_production_companies_task, 
                       stage_production_countries_task, 
                       stage_spoken_languages_task,
                       stage_movies_task, 
                       stage_ratings_task]

stage_genres_task >> load_genres_task
stage_production_companies_task >> load_production_companies_task
stage_production_countries_task >> load_production_countries_task
stage_spoken_languages_task >> load_spoken_languages_task
stage_movies_task >> load_release_dates_task
stage_movies_task >> load_movies_task

stage_ratings_task >> load_ratings_task
stage_ratings_task >> load_rating_times_task

[stage_movies_task, stage_ratings_task] >> load_user_movie_ratings_task

[load_genres_task,
 load_production_companies_task,
 load_production_countries_task,
 load_spoken_languages_task,
 load_release_dates_task,
 load_movies_task,
 load_ratings_task,
 load_rating_times_task,
 load_user_movie_ratings_task] >> run_staging_quality_checks 
                                   
run_staging_quality_checks >> [run_dimension_quality_checks, run_fact_quality_checks]

[run_dimension_quality_checks, 
 run_fact_quality_checks] >> end_operator