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

    @task
    def generate_bookings():
        pass 

    
    @task
    def quality_check():
        pass 

    
    generate_bookings() >> quality_check()

    dag_instance = data_quality_pipeline()