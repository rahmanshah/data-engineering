# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "25452ddb-7570-4b5d-9c05-07a17d1b8bbe",
# META       "default_lakehouse_name": "rawDataLakehouse",
# META       "default_lakehouse_workspace_id": "ae3e989e-5bbd-4160-9420-a1e9e48a9927",
# META       "known_lakehouses": [
# META         {
# META           "id": "25452ddb-7570-4b5d-9c05-07a17d1b8bbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##

# MARKDOWN ********************

# ## Bronze Layer dataframe 

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
customers_new = spark.read.parquet("abfss://sparksql_ecommerce@onelake.dfs.fabric.microsoft.com/rawDataLakehouse.Lakehouse/Files/Bronze/customers.parquet")
orders_new = spark.read.parquet("abfss://sparksql_ecommerce@onelake.dfs.fabric.microsoft.com/rawDataLakehouse.Lakehouse/Files/Bronze/orders.parquet")
order_items_new = spark.read.parquet("abfss://sparksql_ecommerce@onelake.dfs.fabric.microsoft.com/rawDataLakehouse.Lakehouse/Files/Bronze/order_items.parquet")
products_new = spark.read.parquet("abfss://sparksql_ecommerce@onelake.dfs.fabric.microsoft.com/rawDataLakehouse.Lakehouse/Files/Bronze/products.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customers_new.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(orders_new.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(order_items_new.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(products_new.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Bronze layer table from dataframe

# CELL ********************

customers_new.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")
orders_new.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
order_items_new.write.format("delta").mode("overwrite").saveAsTable("bronze_order_items")
products_new.write.format("delta").mode("overwrite").saveAsTable("bronze_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * 
# MAGIC FROM bronze_customers
# MAGIC LIMIT 5;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_orders
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_order_items
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_products
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver Layer tables

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_orders
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE silver_orders AS
# MAGIC SELECT
# MAGIC   CAST(OrderID AS STRING) AS OrderID,
# MAGIC   CAST(CustomerID AS STRING) AS CustomerID,
# MAGIC   TO_DATE(REPLACE(OrderDate, '/', '-')) AS OrderDate,
# MAGIC   TO_DATE(REPLACE(ShippedDate, '/', '-')) AS ShippedDate,
# MAGIC   TO_DATE(REPLACE(DeliveredDate, '/', '-')) AS DeliveredDate,
# MAGIC   ROUND(CAST(Price AS DOUBLE), 2) AS Price,
# MAGIC   INITCAP(TRIM(OrderChannel)) AS OrderChannel,
# MAGIC   INITCAP(TRIM(Region)) AS Region,
# MAGIC   TRIM(ReferredBy) AS ReferredBy
# MAGIC FROM bronze_orders
# MAGIC WHERE OrderID IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_orders
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM bronze_customers
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE silver_customers AS
# MAGIC SELECT
# MAGIC   CAST(CustomerID AS STRING) AS CustomerID,
# MAGIC   INITCAP(TRIM(CustomerName)) AS CustomerName,
# MAGIC   CASE 
# MAGIC     WHEN LOWER(IsPremiumCustomer) IN ('yes', 'y') THEN 'Yes'
# MAGIC     ELSE 'No'
# MAGIC   END AS IsPremiumCustomer,
# MAGIC   TO_DATE(SignupDate) AS SignupDate
# MAGIC FROM bronze_customers
# MAGIC WHERE CustomerID IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_customers
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
