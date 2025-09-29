# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://mlflow.org/docs/latest/images/logo-light.svg" width="240px" alt="MLflow logo" />
# MAGIC
# MAGIC # MLflow Tracing with LangGraph
# MAGIC
# MAGIC This notebook demonstrates how to use MLflow's LangGraph tracing integration to capture and analyze your GenAI application's execution.
# MAGIC
# MAGIC > **Learn more** about this integration in the <a href="https://mlflow.org/docs/latest/tracing/integrations/langgraph" target="_blank">MLflow LangGraph tracing documentation</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Installation
# MAGIC
# MAGIC First, let's install the required packages and restart the Python kernel:

# COMMAND ----------

# MAGIC %pip install -U mlflow langgraph langchain databricks_langchain
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Enable Autologging (if supported)
# MAGIC
# MAGIC Now, we'll enable MLflow's automatic tracing functionality, if supported. This captures inputs, outputs, and performance metrics without changing your application code:

# COMMAND ----------

# Enable MLflow autologging for LangGraph
import mlflow

mlflow.langchain.autolog()
mlflow.set_experiment(experiment_id=1189180429777543)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Your Application
# MAGIC
# MAGIC With autologging enabled, we can now run our application code. MLflow will automatically track all LangGraph operations:
# MAGIC
# MAGIC > **Tip**: After running the code below, you'll see traces in the MLflow UI and directly below the cell output!

# COMMAND ----------

# Example application code
# Visit the LangGraph documentation to see more advanced examples: https://langchain-ai.github.io/langgraph/

from langgraph.prebuilt import create_react_agent
from databricks_langchain import ChatDatabricks

# You can replace 'endpoint' with any Databricks hosted model from here: http://<workspace-url>/ml/endpoints
# Ensure that the LLM supports tool calling prior to using it with this example
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct")

def search(query: str):
    """Call to surf the web."""
    if "mlflow" in query.lower():
        return "MLflow is a tool for building high-quality GenAI apps!"
    return "I couldn't find any results."

agent = create_react_agent(llm, tools=[search])
agent.invoke(
    {"messages": [{"role": "user", "content": "What is MLflow?"}]}
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
# MAGIC * Add more complex LangGraph workflows to your application
# MAGIC * Check out other MLflow tracing integrations for your GenAI stack
