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

# CELL ********************

df = spark.read.table("MyFirstLakehouse.sales")
display(df.limit(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Listings table lookup

# CELL ********************

df_listings = spark.read.table("MyFirstLakehouse.listings")
display(df_listings.limit(2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(df_listings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_listings.printSchema())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

listings = spark.read.csv("Files/listings.csv",header=True, inferSchema=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

listings.printSchema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

review = listings.select(listings.review_scores_location)
display(review.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

listings.write.mode("overwrite").format("delta").saveAsTable("listings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
