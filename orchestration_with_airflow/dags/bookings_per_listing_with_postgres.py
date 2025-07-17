from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv
import random
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    "bookings_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    description="",
)

def bookings_with_postgres_spark_pipeline():

    @task
    def read_bookings_from_postgres():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")


        file_path = f"/tmp/data/bookings/{file_date}/bookings.csv"

        start_of_minute = execution_date.replace(second=0, microsecond=0)
        end_of_minute = start_of_minute + timedelta(minutes=1)


    ## creating output directory
    create_output_dir = BashOperator(
    task_id="create_output_directory",
    bash_command="mkdir -p /tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}"
    )


    spark_job = SparkSubmitOperator(
        task_id="process_listings_and_bookings",
        application="dags/bookings_per_listing_spark.py",
        name="listings_bookings_join",
        application_args=[
            "--listings_file", "/tmp/data/listings/{{ execution_date.strftime('%Y-%m') }}/listings.csv.gz",
            "--bookings_file", "/tmp/data/bookings/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}/bookings.csv",
            "--output_path", "/tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}"
        ],
        conn_id='spark_booking',
    )

    bookings_file = generate_bookings()
    bookings_file >> create_output_dir >> spark_job


dag_instance = bookings_spark_pipeline()