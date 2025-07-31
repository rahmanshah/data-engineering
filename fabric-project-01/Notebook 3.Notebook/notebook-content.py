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
df = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema", "true") \
    .load("Files/products.csv")

display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.summary().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_products = spark.read.table("MyFirstLakehouse.products")
display(df_products.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PySpark dataframe

# CELL ********************

data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]
column_names = ["language", "framework", "users"]

df = spark.createDataFrame(data, column_names)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Writing PySpark dataframe into a delta table

# CELL ********************

df.write.mode('overwrite').format('delta').saveAsTable('framework')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
