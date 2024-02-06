from datetime import datetime, timedelta
import pendulum
import os
import logging


from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.final_project_sql_statements import SqlQueries



default_args = {
    'owner': 'myetldag',
    'start_date': pendulum.now(),
    'retries': 3,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=300),
    'catchup' : False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    
    S3BUCKET = "mys3bucket"
    REDSHIFT_ID = "redshift"
    AWS_ID = "aws_credentials"


    config = configparser.ConfigParser()
    config.read('mydagconfig.cfg')

    # It is required to create the tables in Redshift first
    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')
    
    # We assume the DAG runs hourly to inject the last updated S3 files in last one hour to the staging table  
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_ID,
        aws_credentials_id=AWS_ID,
        table="staging_events_table",
        s3_bucket=S3BUCKET,
        s3_key="log-data",
        json_schema_path = "log_json_path.json",
        is_clear_data = True, 
    )

    # We assume the DAG runs hourly to inject the last updated S3 files in last one hour to the staging table  
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_ID,
        aws_credentials_id=AWS_ID,
        table="staging_songs_table",
        s3_bucket=S3BUCKET,
        s3_key="song-data",
        json_schema_path = None,
        is_clear_data = True, 
    )

    # For Fact and Dimension tables, we assume the data is accumulated and thus not required to be truncated
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table = 'songplays',
        redshift_conn_id=REDSHIFT_ID,
        sql_insert = SqlQueries.songplay_table_insert, 
        is_clear_data = False, 
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table = 'users',
        redshift_conn_id=REDSHIFT_ID,
        sql_insert = SqlQueries.user_table_insert, 
        is_clear_data = False, 
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table = 'songs',
        redshift_conn_id=REDSHIFT_ID,
        sql_insert = SqlQueries.song_table_insert, 
        is_clear_data = False, 
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table = 'artists',
        redshift_conn_id=REDSHIFT_ID,
        sql_insert = SqlQueries.artist_table_insert, 
        is_clear_data = False, 
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table = 'time',
        redshift_conn_id=REDSHIFT_ID,
        sql_insert = SqlQueries.time_table_insert, 
        is_clear_data = False, 
    )

    def compare_rows(preloadcnt, postloadcnt):
        return postloadcnt>=preloadcnt

    table_checks = {"artists": compare_rows, "songplays": compare_rows, "songs": compare_rows, "time": compare_rows, "users": compare_rows}
    # This task takes the row counts of each table before loading
    take_table_counts_before_loading = DataQualityOperator(
        task_id='take_table_counts_before_loading',
        redshift_conn_id="redshift",
        table_checks = table_checks,
        up_stream_id = None,
    )


    # This task examines whether the row counts of each table has been increased or remain the same after loading new data from staging tables
    # The quality check passes if the row counts is equal or greater than those before loading, meaning that new data should have arrived.
    run_quality_checks_post_loading = DataQualityOperator(
        task_id='run_quality_checks_post_loading',
        redshift_conn_id="redshift",
        table_checks = table_checks,
        up_stream_id = "take_table_counts_before_loading",
    )





# Production pipeline for use after creating tables
    start_operator >> take_table_counts_before_loading

    take_table_counts_before_loading >> stage_events_to_redshift
    take_table_counts_before_loading >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
 
    load_user_dimension_table >> run_quality_checks_post_loading
    load_song_dimension_table >> run_quality_checks_post_loading
    load_artist_dimension_table >> run_quality_checks_post_loading
    load_time_dimension_table >> run_quality_checks_post_loading

    run_quality_checks_post_loading >> end_operator


final_project_dag = final_project()