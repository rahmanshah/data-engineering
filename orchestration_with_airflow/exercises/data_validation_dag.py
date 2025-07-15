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