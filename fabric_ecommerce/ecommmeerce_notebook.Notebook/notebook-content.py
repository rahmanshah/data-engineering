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

# CELL ********************

display(customers_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(orders_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(payments_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(support_tickets_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(web_activities_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create bronze table from dataframe

# CELL ********************

customers_raw.write.format("delta").mode("overwrite").saveAsTable("customers")
orders_raw.write.format("delta").mode("overwrite").saveAsTable("orders")
payments_raw.write.format("delta").mode("overwrite").saveAsTable("payments")
support_tickets_raw.write.format("delta").mode("overwrite").saveAsTable("support_tickets")
web_activities_raw.write.format("delta").mode("overwrite").saveAsTable("web_activities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver layer - Cleaning the data

# MARKDOWN ********************

# #### Customer Data

# CELL ********************

display(customers_raw.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_clean = (
    customers_raw
    .withColumn('email',lower(trim(col('EMAIL'))))
    .withColumn('name',initcap(trim(col('name'))))
    .withColumn('gender',when(lower(col('gender')).isin('f','female'),'Female')
                        .when(lower(col('gender')).isin('m','male'),'Male')
                        .otherwise('Other'))
    .withColumn('dob',to_date(regexp_replace(col('dob'),'/','-')))
    .withColumn('location',initcap(col('location')))
    .drop_duplicates(['customer_id'])
    .dropna(subset=['customer_id','email'])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customers_clean.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_clean.write.format("delta").mode("overwrite").saveAsTable("silver_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
