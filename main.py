import os


# Set Python paths
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Shah\AppData\Local\Programs\Python\Python312\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\Shah\AppData\Local\Programs\Python\Python312\python.exe'

# Then create SparkSession
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName("Test").getOrCreate()


# In your VS Code Jupyter notebook cell
from pyspark.sql import SparkSession
#import os

# Optional: Set Java home if needed
# os.environ['JAVA_HOME'] = '/path/to/java'

# Create SparkSession
spark = SparkSession.builder \
    .appName("VSCode PySpark") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Test it
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()