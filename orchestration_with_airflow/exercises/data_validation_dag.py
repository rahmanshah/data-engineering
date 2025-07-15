from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random