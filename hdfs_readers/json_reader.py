import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS JSON Read") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.27.90.91:9000") \
    .config("spark.hadoop.dfs.client.read.shortcircuit", "false") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

jvm = spark._jvm
runtime = jvm.java.lang.Runtime.getRuntime()
memory_before = runtime.totalMemory() - runtime.freeMemory()

start_time = time.time()
 
df = spark.read.json("hdfs://172.27.90.91:9000/user/kerem/datasets/data.json")

end_time = time.time()

df.show()


memory_after = runtime.totalMemory() - runtime.freeMemory()
reading_time = end_time - start_time

print(f"========================================================================")
print(f"Reading Time of JSON formatted file: {reading_time} seconds")
print(f"Memory Usage Before Reading: {memory_before / (1024 * 1024):.2f} MB")
print(f"Memory Usage After Reading: {memory_after / (1024 * 1024):.2f} MB")
print(f"========================================================================")

#execute this line for running: `spark-submit .\readers\json_reader.py`
