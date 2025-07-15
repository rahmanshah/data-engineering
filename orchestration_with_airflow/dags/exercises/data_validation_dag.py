from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random

@dag(
   "data_quality_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
    description="Data Quality Check DAG",
)

def data_quality_pipeline():

    def get_bookings_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")

        return f"/tmp/data/bookings/{file_date}/bookings.json"

    @task
    def generate_bookings():
        context = get_current_context()
        booking_path = get_bookings_path(context)

    
    @task
    def quality_check():
        context = get_current_context()
        booking_path = get_bookings_path(context) 

    
    generate_bookings() >> quality_check()

    dag_instance = data_quality_pipeline()