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
    pass