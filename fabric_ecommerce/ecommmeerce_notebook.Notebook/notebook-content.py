# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a22286f2-e2dc-4546-accc-58f2cb8d27aa",
# META       "default_lakehouse_name": "ecommerce_lakehouse",
# META       "default_lakehouse_workspace_id": "4c0f5dd9-f2e6-439d-8f9d-6d0a77c69a0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "a22286f2-e2dc-4546-accc-58f2cb8d27aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Read data from Bronze layer and create dataframe

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
customers_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/Bronze/customers.parquet")
orders_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/Bronze/orders.parquet")
payments_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/Bronze/payments.parquet")
support_tickets_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/Bronze/support_tickets.parquet")
web_activities_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/Bronze/web_activities.parquet")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
