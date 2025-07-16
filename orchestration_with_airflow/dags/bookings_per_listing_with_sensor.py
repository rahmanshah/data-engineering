from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv
import random
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

@dag(
    "bookings_with_sensors_spark_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    description="",
)

def bookings_with_sensors_spark_pipeline():

    @task
    def generate_bookings():
        context = get_current_context()
        execution_date = context["execution_date"]

        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        file_path = f"/tmp/data/bookings/{file_date}/bookings.csv"

        num_bookings = random.randint(30, 50)
        bookings = []

        for i in range(num_bookings):
            booking = {
                "booking_id": random.randint(1000, 5000),
                "listing_id": random.choice([264776,264782,266037,268398,426354,427584,13913,15400 ]),
                "user_id": random.randint(1000, 5000),
                "booking_time": execution_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(["confirmed", "cancelled", "pending"])
            }
            bookings.append(booking)
        
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        fieldnames = ["booking_id", "listing_id", "user_id", "booking_time", "status"]

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for booking in bookings:
                writer.writerow({
                    "booking_id": booking["booking_id"],
                    "listing_id": booking["listing_id"],
                    "user_id": booking["user_id"],
                    "booking_time": booking["booking_time"],
                    "status": booking["status"]
                })

        print(f"Generated bookings data written to {file_path}")

    ## creating output directory
    create_output_dir = BashOperator(
    task_id="create_output_directory",
    bash_command="mkdir -p /tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}"
    )

    wait_for_listings_file = FileSensor(
        task_id='wait_for_listings_file',
        filepath='/tmp/data/listings/{{ execution_date.strftime("%Y-%m") }}/listings.csv.gz',
        fs_conn_id='local_fs',
        poke_interval=30,
        timeout=600,
        mode='poke'
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
    wait_for_listings_file >> spark_job


dag_instance = bookings_with_sensors_spark_pipeline()