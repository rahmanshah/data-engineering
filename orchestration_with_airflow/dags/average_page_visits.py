from datetime import datetime
import os
import json
import random

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    "average_page_visits",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    description=""
)

def average_page_visits():
    @task
    def produce_page_visits_data():
        page_visits = [
            {"id": 1, "name": "Cozy Apartment", "price": 120, "page_visits": random.randint(0, 50)},
            {"id": 2, "name": "Luxury Condo", "price": 300, "page_visits": random.randint(0, 50)},
            {"id": 3, "name": "Modern Studio", "price": 180, "page_visits": random.randint(0, 50)},
            {"id": 4, "name": "Charming Loft", "price": 150, "page_visits": random.randint(0, 50)},
            {"id": 5, "name": "Spacious Villa", "price": 400, "page_visits": random.randint(0, 50)},
            ]
        file_path = get_data_path()

    @task
    def process_page_visits_data():
        pass

demo_dag = average_page_visits()