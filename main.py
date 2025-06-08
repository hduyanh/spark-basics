import os
from pyspark.sql import SparkSession
import code 


# Force Spark to bind to localhost
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")
print("Spark Web UI:", spark.sparkContext.uiWebUrl)
spark.sparkContext.setLogLevel("ERROR")

code.interact(local=locals()) 



