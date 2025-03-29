import time
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("XML Reader").getOrCreate()

jvm = spark._jvm
runtime = jvm.java.lang.Runtime.getRuntime()
memory_before = runtime.totalMemory() - runtime.freeMemory()

start_time = time.time()

df = spark.read.format('com.databricks.spark.xml').option("root", "item").load("datasets/data.xml")

end_time = time.time()

df.show()

memory_after = runtime.totalMemory() - runtime.freeMemory()
reading_time = end_time - start_time

print(f"========================================================================")
print(f"Reading Time of Parquet formatted file: {reading_time} seconds")
print(f"Memory Usage Before Reading: {memory_before / (1024 * 1024):.2f} MB")
print(f"Memory Usage After Reading: {memory_after / (1024 * 1024):.2f} MB")
print(f"========================================================================")
#execute this line for running: `spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 .\readers\xml_reader.py`
