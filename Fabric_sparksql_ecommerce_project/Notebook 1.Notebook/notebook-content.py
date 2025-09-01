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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE silver_products AS
# MAGIC SELECT
# MAGIC   CAST(ProductID AS STRING) AS ProductID,
# MAGIC   INITCAP(TRIM(Category)) AS Category,
# MAGIC   INITCAP(TRIM(Carrier)) AS Carrier,
# MAGIC   ROUND(CAST(ShipmentCost AS DOUBLE), 2) AS ShipmentCost,
# MAGIC   CASE 
# MAGIC     WHEN LOWER(IsDiscounted) IN ('yes', 'y') THEN 'Yes'
# MAGIC     ELSE 'No'
# MAGIC   END AS IsDiscounted
# MAGIC FROM bronze_products
# MAGIC WHERE ProductID IS NOT NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_products
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
# MAGIC CREATE OR REPLACE TABLE silver_order_items AS
# MAGIC SELECT
# MAGIC   CAST(OrderID AS STRING) AS OrderID,
# MAGIC   CAST(ProductID AS STRING) AS ProductID,
# MAGIC   CAST(Quantity AS INT) AS Quantity
# MAGIC FROM bronze_order_items
# MAGIC WHERE Quantity IS NOT NULL AND Quantity > 0;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_order_items
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Gold Layers table

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_enriched AS
# MAGIC SELECT
# MAGIC   o.OrderID,
# MAGIC   o.CustomerID,
# MAGIC   o.OrderDate,
# MAGIC   o.ShippedDate,
# MAGIC   o.DeliveredDate,
# MAGIC   o.Price,
# MAGIC   o.Region,
# MAGIC   o.OrderChannel,
# MAGIC   o.ReferredBy,
# MAGIC   c.CustomerName,
# MAGIC   c.IsPremiumCustomer,
# MAGIC   c.SignupDate,
# MAGIC   i.ProductID,
# MAGIC   i.Quantity,
# MAGIC   p.Category,
# MAGIC   p.Carrier,
# MAGIC   p.ShipmentCost,
# MAGIC   p.IsDiscounted,
# MAGIC   DATEDIFF(o.DeliveredDate, o.ShippedDate) AS DeliveryDelay
# MAGIC FROM silver_orders o
# MAGIC LEFT JOIN silver_customers c ON o.CustomerID = c.CustomerID
# MAGIC LEFT JOIN silver_order_items i ON o.OrderID = i.OrderID
# MAGIC LEFT JOIN silver_products p ON i.ProductID = p.ProductID;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM orders_enriched
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold_order_kpis AS
# MAGIC SELECT
# MAGIC   CustomerID,
# MAGIC   CustomerName,
# MAGIC   IsPremiumCustomer,
# MAGIC   Region,
# MAGIC   OrderChannel,
# MAGIC   COUNT(DISTINCT OrderID) AS TotalOrders,
# MAGIC   SUM(Quantity) AS TotalQuantity,
# MAGIC   SUM(Price) AS TotalRevenue,
# MAGIC   ROUND(AVG(DeliveryDelay), 1) AS AvgDeliveryDelayDays,
# MAGIC   COUNT(DISTINCT CASE WHEN DeliveryDelay > 3 THEN OrderID END) AS DelayedOrders,
# MAGIC   ROUND(SUM(ShipmentCost), 2) AS TotalShipmentCost,
# MAGIC   ROUND(SUM(CASE WHEN IsDiscounted = 'Yes' THEN Price ELSE 0 END), 2) AS RevenueFromDiscounted,
# MAGIC   MAX(DeliveryDelay) AS MaxDeliveryDelay
# MAGIC FROM orders_enriched
# MAGIC GROUP BY 
# MAGIC   CustomerID, CustomerName, IsPremiumCustomer, Region, OrderChannel;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold_order_kpis
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
