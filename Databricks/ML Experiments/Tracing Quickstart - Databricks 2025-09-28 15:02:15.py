# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://mlflow.org/docs/latest/images/logo-light.svg" width="240px" alt="MLflow logo" />
# MAGIC
# MAGIC # MLflow Tracing with Databricks
# MAGIC
# MAGIC This notebook demonstrates how to use MLflow's Databricks tracing integration to capture and analyze your GenAI application's execution.
# MAGIC
# MAGIC > **Learn more** about this integration in the <a href="https://docs.databricks.com/aws/en/mlflow/mlflow-tracing" target="_blank">MLflow Databricks tracing documentation</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Installation
# MAGIC
# MAGIC First, let's install the required packages and restart the Python kernel:

# COMMAND ----------

# MAGIC %pip install -U mlflow databricks-sdk[openai]
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Enable Autologging (if supported)
# MAGIC
# MAGIC Now, we'll enable MLflow's automatic tracing functionality, if supported. This captures inputs, outputs, and performance metrics without changing your application code:

# COMMAND ----------

# Enable MLflow autologging for Databricks
import mlflow

mlflow.openai.autolog()
mlflow.set_experiment(experiment_id=854947440763040)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Your Application
# MAGIC
# MAGIC With autologging enabled, we can now run our application code. MLflow will automatically track all Databricks operations:
# MAGIC
# MAGIC > **Tip**: After running the code below, you'll see traces in the MLflow UI and directly below the cell output!

# COMMAND ----------

# Example application code
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

response = client.chat.completions.create(
  # You can replace 'model' with any Databricks hosted model from here: http://<workspace-url>/ml/endpoints
  model="databricks-llama-4-maverick",
  messages=[
    {
      "role": "system", 
      "content": "You are a helpful assistant.",
    },
    {
      "role": "user",
      "content": "What is MLflow?",
    },
  ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: View and Analyze Traces
# MAGIC
# MAGIC After running your code, MLflow captures detailed traces of each step. You can:
# MAGIC
# MAGIC * View traces directly below the cell output
# MAGIC * Explore the full trace details in the MLflow UI
# MAGIC * Compare multiple runs to analyze performance and outputs
# MAGIC
# MAGIC Try modifying the input parameters above and run the cell again to see how the traces compare!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC * Experiment with different inputs to see how they affect the outputs
# MAGIC * Add more complex Databricks workflows to your application
# MAGIC * Check out other MLflow tracing integrations for your GenAI stack
