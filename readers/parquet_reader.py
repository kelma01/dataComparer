import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Parquet Reader").getOrCreate()

jvm = spark._jvm
runtime = jvm.java.lang.Runtime.getRuntime()
memory_before = runtime.totalMemory() - runtime.freeMemory()

start_time = time.time()

df = spark.read.parquet("datasets/data.parquet")

end_time = time.time()

df.show()

memory_after = runtime.totalMemory() - runtime.freeMemory()
reading_time = end_time - start_time

print(f"========================================================================")
print(f"Reading Time of Parquet formatted file: {reading_time} seconds")
print(f"Memory Usage Before Reading: {memory_before / (1024 * 1024):.2f} MB")
print(f"Memory Usage After Reading: {memory_after / (1024 * 1024):.2f} MB")
print(f"========================================================================")
#execute this line for running: `spark-submit .\readers\parquet_reader.py`
