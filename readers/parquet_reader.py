import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Parquet Reader").getOrCreate()

start_time = time.time()

df = spark.read.parquet("datasets/data.parquet")

end_time = time.time()

reading_time = end_time - start_time
print(f"========================================================================\nReading Time of Parquet formatted file: {reading_time} seconds\n========================================================================")

