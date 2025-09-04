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

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_orders = (
    orders_df

    # 2. Clean column names
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("cust_id", "CustomerID")
    .withColumnRenamed("Product_Name", "ProductName")
    .withColumnRenamed("Qty", "Quantity")
    .withColumnRenamed("Order_Date", "OrderDate")
    .withColumnRenamed("Order_Amount$", "OrderAmount")
    .withColumnRenamed("Delivery_Status", "DeliveryStatus")
    .withColumnRenamed("Payment_Mode", "PaymentMode")
    .withColumnRenamed("Ship_Address", "ShipAddress")
    .withColumnRenamed("Promo_Code", "PromoCode")
    .withColumnRenamed("Feedback_Score", "FeedbackScore")

    # 3. Normalize Quantity: convert words like 'one', 'Two' to integer
    .withColumn("Quantity", 
        when(lower(col("Quantity")) == "one", 1)
        .when(lower(col("Quantity")) == "two", 2)
        .when(lower(col("Quantity")) == "three", 3)
        .otherwise(col("Quantity").cast(IntegerType()))
    )

    # 4. Standardize date format using multiple patterns
    .withColumn("OrderDate", to_date(
        coalesce(
            to_date(col("OrderDate"), "yyyy/MM/dd"),
            to_date(col("OrderDate"), "dd-MM-yyyy"),
            to_date(col("OrderDate"), "MM-dd-yyyy"),
            to_date(col("OrderDate"), "yyyy.MM.dd"),
            to_date(col("OrderDate"), "dd/MM/yyyy"),
            to_date(col("OrderDate"), "dd.MM.yyyy"),
            to_date(col("OrderDate"), "MMMM dd yyyy")
        )
    ))

    # 5. Clean and convert OrderAmount
    .withColumn("OrderAmount", regexp_replace(col("OrderAmount"), "[$₹Rs. USD, INR]", ""))
    .withColumn("OrderAmount", col("OrderAmount").cast(DoubleType()))

    # 6. Standardize PaymentMode ! 
    .withColumn("PaymentMode", lower(regexp_replace(col("PaymentMode"), "[^a-zA-Z]", "")))

    # 7. Standardize DeliveryStatus
    .withColumn("DeliveryStatus", lower(regexp_replace(col("DeliveryStatus"), "[^a-zA-Z ]", "")))

    # 8. Validate email using simple regex pattern
    .withColumn("Email", when(col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), col("Email")).otherwise(None))

    # 9. Clean address: remove special characters like #, !, $, @ etc.
    .withColumn("ShipAddress", regexp_replace(col("ShipAddress"), r"[#@!$]", ""))

    # 10. FeedbackScore: convert to float, handle NaN/bad values
    .withColumn("FeedbackScore", col("FeedbackScore").cast(DoubleType()))

    # 11. Fill nulls where possible
    .fillna({"Quantity": 0, "OrderAmount": 0.0, "DeliveryStatus": "unknown", "PaymentMode": "unknown"})

    # 12. Drop rows with no CustomerID or ProductName
    .na.drop(subset=["CustomerID", "ProductName"])

    # 13. Remove duplicates by OrderID
    .dropDuplicates(["OrderID"])
)

display(df_orders.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_orders_data
# MAGIC LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 
