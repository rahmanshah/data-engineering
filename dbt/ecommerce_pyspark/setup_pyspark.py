from pyspark.sql import SparkSession

def get_spark_session():
    """Create and return a SparkSession for dbt to use"""
    spark = SparkSession.builder \
        .appName("dbt_ecommerce") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

# Create the session when this script is imported
spark = get_spark_session()
print("PySpark session initialized successfully!")