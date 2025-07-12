from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,         # No dependency on past runs
    'retries': 3,                     # Retry 3 times on failure
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
    'email_on_retry': False,          # Do not email on retry
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False                    # Catchup is turned off
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        region='us-east-1',
        s3_bucket="indira-srini2",
        s3_key="log-data/", 
        json_option = "s3://indira-srini2/log_json_path.json",

    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        region='us-east-1',
        s3_bucket="indira-srini2",
        s3_key="song-data/",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplay',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        truncate_table=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='song',
        sql_query=SqlQueries.song_table_insert,
        truncate_table=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artist',
        sql_query=SqlQueries.artist_table_insert,
        truncate_table=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        truncate_table=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        test_cases=[
            {"sql": "SELECT COUNT(*) FROM songplay WHERE songplay_id IS NULL", "expected_result": 0},
            {"sql": "SELECT COUNT(*) FROM users WHERE user_id IS NULL", "expected_result": 0}
        ]
    )
    stop_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_artist_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()