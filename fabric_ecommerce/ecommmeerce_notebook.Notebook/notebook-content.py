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

# MARKDOWN ********************

# #### Order data clean up

# MARKDOWN ********************

# ##### Checking order_date column data formats

# CELL ********************

import re
import pandas as pd
# Function to identify formats
def detect_date_format(date_str):
    if pd.isna(date_str):
        return 'Missing/NaN'
    
    try:
        date_str = str(date_str).strip()
        
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return 'YYYY-MM-DD'
        elif re.match(r'^\d{4}\d{2}\d{2}$', date_str):
            return 'YYYYMMDD'
        elif re.match(r'^\d{2}/\d{2}/\d{4}$', date_str):
            return 'MM/DD/YYYY'
        elif re.match(r'^\d{4}/\d{2}/\d{2}$', date_str):
            return 'YYYY/MM/DD'
        elif re.match(r'^\d{2}\.\d{2}\.\d{4}$', date_str):
            return 'DD.MM.YYYY'
        elif re.match(r'^\d{2}-\d{2}-\d{4}$', date_str):
            return 'MM-DD-YYYY'
        elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', date_str):
            return 'YYYY-MM-DD HH:MM:SS'
        else:
            return 'Unknown format'
    except:
        return 'Invalid date'


# Test that the function works
print("Function test:", detect_date_format("2023-01-15"))

# Create a UDF and apply it to create the new DataFrame 'df'
# Register the function as a UDF, specifying its return type

detect_date_format_udf = udf(detect_date_format, StringType())

# Create the new DataFrame 'df' with the format information

df = orders_raw.withColumn("date_value", col("order_date")) \
               .withColumn("format", detect_date_format_udf(col("order_date"))) \
               .select("date_value", "format")


# Display the results
print("\nSample DataFrame with identified formats:")
df.show(truncate=False)


# Get unique formats and their counts ---
# Get unique formats (equivalent to df['format'].unique())
print("\nUnique date formats found:")
df.select("format").distinct().show()


# Count of each format (equivalent to df['format'].value_counts())
print("\nCount of each date format:")
df.groupBy("format").count().orderBy("count", ascending=False).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_clean = (
    orders_raw
    .withColumn("order_date", 
                when(col("order_date").rlike("^\d{4}/\d{2}/\d{2}$"), to_date(col("order_date"), "yyyy/MM/dd"))
                .when(col("order_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("order_date"), "dd-MM-yyyy"))
                .when(col("order_date").rlike("^\d{8}$"), to_date(col("order_date"), "yyyyMMdd"))
                .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"])
)
display(orders_clean.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_orders
# MAGIC LIMIT 5

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
