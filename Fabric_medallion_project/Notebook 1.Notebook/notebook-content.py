# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1ecad65e-4e7f-41af-9e18-7e597b89575b",
# META       "default_lakehouse_name": "medallion",
# META       "default_lakehouse_workspace_id": "7a93d4dd-b465-4f10-845d-eb46197ecf7d",
# META       "known_lakehouses": [
# META         {
# META           "id": "1ecad65e-4e7f-41af-9e18-7e597b89575b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Bronze layer 

# MARKDOWN ********************

# #### Creating bronze layer dataframe from parquet files

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
inventory_df = spark.read.parquet("abfss://medallion@onelake.dfs.fabric.microsoft.com/medallion.Lakehouse/Files/Bronze/inventory_data.parquet")
orders_df = spark.read.parquet("abfss://medallion@onelake.dfs.fabric.microsoft.com/medallion.Lakehouse/Files/Bronze/orders_data.parquet")
returns_df = spark.read.parquet("abfss://medallion@onelake.dfs.fabric.microsoft.com/medallion.Lakehouse/Files/Bronze/returns_data.xlsx.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(inventory_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(orders_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(returns_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Handled first row of returns_df

# CELL ********************

# Extract first row as header
first_row = returns_df.first()
columns = [str(item).strip() for item in first_row]

# Remove the first row (header row now part of data)
returns_df = returns_df.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]).toDF(columns)

# Show cleaned data
display(returns_df.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Bronze layer table created from dataframe

# CELL ********************

inventory_df.write.format("delta").mode("overwrite").saveAsTable("bronze_inventory_data")
orders_df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders_data")
returns_df.write.format("delta").mode("overwrite").saveAsTable("bronze_returns_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_inventory_data
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_orders_data
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_returns_data
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver Layer

# MARKDOWN ********************

# ##### Cleaning Order table data

# MARKDOWN ********************

# ##### 
