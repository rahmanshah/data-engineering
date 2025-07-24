from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Query dbt Results") \
    .enableHiveSupport() \
    .getOrCreate()

# Query the customer_orders table
print("Customer Orders:")
df = spark.sql("SELECT * FROM ecommerce_analytics.customer_orders")
df.show()

# Query high-value customers
print("\nHigh-Value Customers:")
high_value = spark.sql("""
    SELECT * FROM ecommerce_analytics.customer_orders 
    WHERE customer_value_segment = 'high_value'
""")
high_value.show()