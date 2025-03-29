import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Avro Reader").getOrCreate()

jvm = spark._jvm
runtime = jvm.java.lang.Runtime.getRuntime()
memory_before = runtime.totalMemory() - runtime.freeMemory()

start_time = time.time()

df = spark.read.format("avro").load("datasets/data.avro")

end_time = time.time()

df.show()

memory_after = runtime.totalMemory() - runtime.freeMemory()
reading_time = end_time - start_time

print(f"========================================================================")
print(f"Reading Time of Avro formatted file: {reading_time} seconds")
print(f"Memory Usage Before Reading: {memory_before / (1024 * 1024):.2f} MB")
print(f"Memory Usage After Reading: {memory_after / (1024 * 1024):.2f} MB")
print(f"========================================================================")

#execute line: `spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.4 .\readers\avro_reader.py`