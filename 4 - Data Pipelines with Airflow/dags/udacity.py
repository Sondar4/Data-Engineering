from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")

default_args = {
    "owner": "Sondar4",
    "start_date": datetime(2019, 1, 12),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False
}

dag = DAG("sparkify_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="@hourly"
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="<REPLACE>",
    s3_key="<REPLACE>",
    region="<REPLACE>",
    compupdate="<REPLACE>",
    json="<REPLACE>",
    create_sql=SqlQueries.staging_events_table_create,
    iam_role="<REPLACE>"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="<REPLACE>",
    s3_key="<REPLACE>",
    region="<REPLACE>",
    compupdate="<REPLACE>",
    truncatecolumns="<REPLACE>",
    create_sql=SqlQueries.staging_songs_table_create,
    iam_role="<REPLACE>"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    create_table=True,
    create_sql=SqlQueries.songplay_table_create,
    delete_previous=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    create_table=True,
    create_sql=SqlQueries.user_table_create,
    delete_previous=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    create_table=True,
    create_sql=SqlQueries.song_table_create,
    delete_previous=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    create_table=True,
    create_sql=SqlQueries.artist_table_create,
    delete_previous=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    create_table=True,
    create_sql=SqlQueries.time_table_create,
    delete_previous=True
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    non_empty_tables=["songplays", "users", "songs", "artists", "time"],
    not_null_fields={
        "users": ["first_name", "level"],
        "songs": ["artist_id"],
        "artists": ["name"]
    }
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)


# Graph connections
load_dimensions = [
    load_song_dimension_table,
    load_user_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
]

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dimensions >> run_quality_checks >> end_operator
