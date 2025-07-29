# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e7714d98-5bc4-4c39-b40e-0030a7b87f04",
# META       "default_lakehouse_name": "MyFirstLakehouse",
# META       "default_lakehouse_workspace_id": "1db9f52d-94fd-4e17-a9c9-caa852b7cf0c",
# META       "known_lakehouses": [
# META         {
# META           "id": "e7714d98-5bc4-4c39-b40e-0030a7b87f04"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("MyFirstLakehouse.publicholidays")
display(df.limit(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM publicholidays
# MAGIC LIMIT 2;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
